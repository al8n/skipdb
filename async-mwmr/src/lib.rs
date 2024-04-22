//! A generic optimistic transaction manger, which is ACID, concurrent with SSI (Serializable Snapshot Isolation).
//!
//! For sync version, please see [`mwmr`](https://crates.io/crates/mwmr)
//!
#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::sync::Arc;

use core::mem;

use error::TransactionError;
pub use smallvec_wrapper::OneOrMore;

pub use wmark::{AsyncSpawner, Detach};

#[cfg(feature = "smol")]
pub use wmark::SmolSpawner;

#[cfg(feature = "async-std")]
pub use wmark::AsyncStdSpawner;

#[cfg(feature = "tokio")]
pub use wmark::TokioSpawner;

/// Error types for the [`async-mwmr`] crate.
pub use mwmr_core::error;

mod oracle;
use oracle::*;
mod read;
pub use read::*;
mod write;
pub use write::*;

pub use mwmr_core::{
  future::*,
  sync::{
    BTreeCm, Cm, CmComparable, CmEquivalent, HashCm, HashCmOptions, Marker, Pwm, PwmComparable,
    PwmComparableRange, PwmEquivalent, PwmEquivalentRange, PwmRange,
  },
  types::*,
};

/// Options for the [`AsyncTm`].
#[derive(Debug, Clone)]
pub struct Options {
  detect_conflicts: bool,
}

impl core::default::Default for Options {
  fn default() -> Self {
    Self::new()
  }
}

impl Options {
  /// Create a new options with default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      detect_conflicts: true,
    }
  }

  /// Returns whether the transactions would be checked for conflicts.
  #[inline]
  pub const fn detect_conflicts(&self) -> bool {
    self.detect_conflicts
  }

  /// Set whether the transactions would be checked for conflicts.
  #[inline]
  pub fn set_detect_conflicts(&mut self, detect_conflicts: bool) -> &mut Self {
    self.detect_conflicts = detect_conflicts;
    self
  }

  /// Set whether the transactions would be checked for conflicts.
  #[inline]
  pub const fn with_detect_conflicts(mut self, detect_conflicts: bool) -> Self {
    self.detect_conflicts = detect_conflicts;
    self
  }
}

/// A multi-writer multi-reader MVCC, ACID, Serializable Snapshot Isolation transaction manager.
pub struct AsyncTm<K, V, C, P, S> {
  inner: Arc<Oracle<C, S>>,
  _phantom: std::marker::PhantomData<(K, V, P)>,
}

impl<K, V, C, P, S> Clone for AsyncTm<K, V, C, P, S> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      _phantom: std::marker::PhantomData,
    }
  }
}

impl<K, V, C, P, S> AsyncTm<K, V, C, P, S>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Create a new writable transaction with
  /// the default pending writes manager to store the pending writes.
  pub async fn write_with_blocking_cm_and_pwm(
    &self,
    pending_manager_opts: P::Options,
    conflict_manager_opts: Option<C::Options>,
  ) -> Result<AsyncWtm<K, V, C, P, S>, TransactionError<C::Error, P::Error>> {
    let read_ts = self.inner.read_ts().await;
    Ok(AsyncWtm {
      orc: self.inner.clone(),
      read_ts,
      size: 0,
      count: 0,
      conflict_manager: if let Some(opts) = conflict_manager_opts {
        Some(C::new(opts).map_err(TransactionError::conflict)?)
      } else {
        None
      },
      pending_writes: Some(P::new(pending_manager_opts).map_err(TransactionError::pending)?),
      duplicate_writes: OneOrMore::new(),
      discarded: false,
      done_read: false,
    })
  }
}

impl<K, V, C, P, S> AsyncTm<K, V, C, P, S>
where
  C: AsyncCm<Key = K>,
  P: AsyncPwm<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Create a new writable transaction with
  /// the default pending writes manager to store the pending writes.
  pub async fn write(
    &self,
    pending_manager_opts: P::Options,
    conflict_manager_opts: Option<C::Options>,
  ) -> Result<AsyncWtm<K, V, C, P, S>, TransactionError<C::Error, P::Error>> {
    let read_ts = self.inner.read_ts().await;
    Ok(AsyncWtm {
      orc: self.inner.clone(),
      read_ts,
      size: 0,
      count: 0,
      conflict_manager: if let Some(opts) = conflict_manager_opts {
        Some(C::new(opts).await.map_err(TransactionError::conflict)?)
      } else {
        None
      },
      pending_writes: Some(
        P::new(pending_manager_opts)
          .await
          .map_err(TransactionError::pending)?,
      ),
      duplicate_writes: OneOrMore::new(),
      discarded: false,
      done_read: false,
    })
  }
}

impl<K, V, C, P, S> AsyncTm<K, V, C, P, S>
where
  S: AsyncSpawner,
{
  /// Create a new transaction manager with the given name (just for logging or debugging, use your crate name is enough)
  /// and the current version (provided by the database).
  #[inline]
  pub async fn new(name: &str, current_version: u64) -> Self {
    Self {
      inner: Arc::new({
        let next_ts = current_version;
        let orc = Oracle::new(
          format!("{}.pending_reads", name).into(),
          format!("{}.txn_timestamps", name).into(),
          next_ts,
        );
        orc.read_mark.done_unchecked(next_ts).await;
        orc.txn_mark.done_unchecked(next_ts).await;
        orc.increment_next_ts().await;
        orc
      }),
      _phantom: std::marker::PhantomData,
    }
  }

  /// Returns the current read version of the database.
  #[inline]
  pub async fn version(&self) -> u64 {
    self.inner.read_ts().await
  }

  /// Close the transaction manager.
  #[inline]
  pub async fn close(&self) {
    self.inner.stop().await;
  }
}

impl<K, V, C, P, S> AsyncTm<K, V, C, P, S>
where
  S: AsyncSpawner,
{
  /// Returns a timestamp which hints that any versions under this timestamp can be discard.
  /// This is useful when users want to implement compaction/merge functionality.
  pub fn discard_hint(&self) -> u64 {
    self.inner.discard_at_or_below()
  }

  /// Create a new writable transaction.
  pub async fn read(&self) -> AsyncRtm<K, V, C, P, S> {
    AsyncRtm {
      db: self.clone(),
      read_ts: self.inner.read_ts().await,
    }
  }
}

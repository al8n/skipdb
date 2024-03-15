//! A generic optimistic transaction manger, which is ACID, concurrent with SSI (Serializable Snapshot Isolation).
//!
//! For tokio runtime, please see [`tokio-mwmr`](https://crates.io/crates/tokio-mwmr)
//!
//! For other async runtime, [`async-mwmr`](https://crates.io/crates/async-mwmr)
#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{cell::RefCell, sync::Arc};

use core::mem;

use either::Either;
use error::TransactionError;
pub use smallvec_wrapper::OneOrMore;

/// Error types for the [`mwmr`] crate.
pub mod error;

/// Generic unit tests for users to test their database implementation based on `mwmr`.
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests;

mod oracle;
use oracle::*;
mod read;
pub use read::*;
mod write;
pub use write::*;

pub use mwmr_core::{sync::*, types::*};

/// Options for the [`TransactionDB`].
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
pub struct TransactionManager<K, V, C, P> {
  inner: Arc<Oracle<C>>,
  _phantom: std::marker::PhantomData<(K, V, P)>,
}

impl<K, V, C, P> Clone for TransactionManager<K, V, C, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      _phantom: std::marker::PhantomData,
    }
  }
}

impl<K, V, C, P> TransactionManager<K, V, C, P>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  /// Create a new writable transaction with
  /// the default pending writes manager to store the pending writes.
  pub fn write(
    &self,
    pending_manager_opts: P::Options,
    conflict_manager_opts: Option<C::Options>,
  ) -> Result<WriteTransaction<K, V, C, P>, TransactionError<C, P>> {
    Ok(WriteTransaction {
      orc: self.inner.clone(),
      read_ts: self.inner.read_ts(),
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

impl<K, V, C, P> TransactionManager<K, V, C, P> {
  /// Create a new transaction manager with the given name (just for logging or debugging, use your crate name is enough)
  /// and the current version (provided by the database).
  #[inline]
  pub fn new(name: &str, current_version: u64) -> Self {
    Self {
      inner: Arc::new({
        let next_ts = current_version;
        let orc = Oracle::new(
          format!("{}.pending_reads", name).into(),
          format!("{}.txn_timestamps", name).into(),
          next_ts,
        );
        orc.read_mark.done_unchecked(next_ts);
        orc.txn_mark.done_unchecked(next_ts);
        orc.increment_next_ts();
        orc
      }),
      _phantom: std::marker::PhantomData,
    }
  }
}

impl<K, V, C, P> TransactionManager<K, V, C, P> {
  /// Returns a timestamp which hints that any versions under this timestamp can be discard.
  /// This is useful when users want to implement compaction/merge functionality.
  pub fn discard_hint(&self) -> u64 {
    self.inner.discard_at_or_below()
  }

  /// Create a new writable transaction.
  pub fn read(&self) -> ReadTransaction<K, V, C, P> {
    ReadTransaction {
      db: self.clone(),
      read_ts: self.inner.read_ts(),
    }
  }

  /// Returns the oracle.
  #[inline]
  fn orc(&self) -> &Oracle<C> {
    &self.inner
  }
}

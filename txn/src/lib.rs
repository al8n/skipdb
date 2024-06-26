//! A generic optimistic transaction manger, which is ACID, concurrent with SSI (Serializable Snapshot Isolation).
//!
//! For other async runtime, [`async-txn`](https://crates.io/crates/async-txn)
#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::sync::Arc;

use core::mem;

pub use smallvec_wrapper::OneOrMore;
use txn_core::error::TransactionError;

/// Error types for the [`txn`] crate.
pub use txn_core::error;

mod oracle;
use oracle::*;
mod read;
pub use read::*;
mod write;
pub use write::*;

pub use txn_core::{sync::*, types::*};

/// A multi-writer multi-reader MVCC, ACID, Serializable Snapshot Isolation transaction manager.
pub struct Tm<K, V, C, P> {
  inner: Arc<Oracle<C>>,
  _phantom: std::marker::PhantomData<(K, V, P)>,
}

impl<K, V, C, P> Clone for Tm<K, V, C, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      _phantom: std::marker::PhantomData,
    }
  }
}

impl<K, V, C, P> Tm<K, V, C, P>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  /// Create a new writable transaction with
  /// the default pending writes manager to store the pending writes.
  pub fn write(
    &self,
    pending_manager_opts: P::Options,
    conflict_manager_opts: C::Options,
  ) -> Result<Wtm<K, V, C, P>, TransactionError<C::Error, P::Error>> {
    let read_ts = self.inner.read_ts();
    Ok(Wtm {
      orc: self.inner.clone(),
      read_ts,
      size: 0,
      count: 0,
      conflict_manager: Some(C::new(conflict_manager_opts).map_err(TransactionError::conflict)?),
      pending_writes: Some(P::new(pending_manager_opts).map_err(TransactionError::pending)?),
      duplicate_writes: OneOrMore::new(),
      discarded: false,
      done_read: false,
    })
  }
}

impl<K, V, C, P> Tm<K, V, C, P> {
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
        orc.read_mark.done(next_ts).unwrap();
        orc.txn_mark.done(next_ts).unwrap();
        orc.increment_next_ts();
        orc
      }),
      _phantom: std::marker::PhantomData,
    }
  }

  /// Returns the current read version of the transaction manager.
  #[inline]
  pub fn version(&self) -> u64 {
    self.inner.read_ts()
  }
}

impl<K, V, C, P> Tm<K, V, C, P> {
  /// Returns a timestamp which hints that any versions under this timestamp can be discard.
  /// This is useful when users want to implement compaction/merge functionality.
  pub fn discard_hint(&self) -> u64 {
    self.inner.discard_at_or_below()
  }

  /// Create a new writable transaction.
  pub fn read(&self) -> Rtm<K, V, C, P> {
    Rtm {
      db: self.clone(),
      read_ts: self.inner.read_ts(),
    }
  }
}

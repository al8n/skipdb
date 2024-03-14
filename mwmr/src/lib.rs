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

use core::{hash::BuildHasher, mem};

use either::Either;
use indexmap::{IndexMap, IndexSet};
use smallvec_wrapper::MediumVec;
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

struct Inner<D, S = std::hash::RandomState> {
  db: D,
  /// Determines whether the transactions would be checked for conflicts.
  /// The transactions can be processed at a higher rate when conflict detection is disabled.
  opts: Options,
  orc: Oracle<S>,
  hasher: S,
}

/// A multi-writer multi-reader MVCC, ACID, Serializable Snapshot Isolation transaction manager.
pub struct TransactionDB<D, S = std::hash::RandomState> {
  inner: Arc<Inner<D, S>>,
}

impl<D, S> Clone for TransactionDB<D, S> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<D: Database, S: BuildHasher + Default + Clone + 'static> TransactionDB<D, S>
where
  D::Key: Eq + core::hash::Hash,
{
  /// Create a new writable transaction with
  /// the default pending writes manager to store the pending writes.
  pub fn write(&self) -> WriteTransaction<D, IndexMapManager<D::Key, D::Value, S>, S> {
    WriteTransaction {
      db: self.clone(),
      read_ts: self.inner.orc.read_ts(),
      size: 0,
      count: 0,
      reads: MediumVec::new(),
      conflict_keys: if self.inner.opts.detect_conflicts {
        Some(IndexSet::with_hasher(self.inner.hasher.clone()))
      } else {
        None
      },
      pending_writes: Some(IndexMap::with_hasher(S::default())),
      duplicate_writes: OneOrMore::new(),
      discarded: false,
      done_read: false,
    }
  }
}

impl<D: Database, S: Clone + 'static> TransactionDB<D, S> {
  /// Create a new writable transaction with the given pending writes manager to store the pending writes.
  pub fn write_by<W: PendingManager>(&self, backend: W) -> WriteTransaction<D, W, S> {
    WriteTransaction {
      db: self.clone(),
      read_ts: self.inner.orc.read_ts(),
      size: 0,
      count: 0,
      reads: MediumVec::new(),
      conflict_keys: if self.inner.opts.detect_conflicts {
        Some(IndexSet::with_hasher(self.inner.hasher.clone()))
      } else {
        None
      },
      pending_writes: Some(backend),
      duplicate_writes: OneOrMore::new(),
      discarded: false,
      done_read: false,
    }
  }
}

impl<D: Database, S: Default> TransactionDB<D, S> {
  /// Open the database with the given options.
  pub fn new(transaction_opts: Options, database_opts: D::Options) -> Result<Self, D::Error> {
    Self::with_hasher(transaction_opts, database_opts, S::default())
  }
}

impl<D: Database, S> TransactionDB<D, S> {
  /// Open the database with the given options.
  pub fn with_hasher(
    transaction_opts: Options,
    database_opts: D::Options,
    hasher: S,
  ) -> Result<Self, D::Error> {
    D::open(database_opts).map(|db| Self {
      inner: Arc::new(Inner {
        orc: {
          let next_ts = db.maximum_version();
          let orc = Oracle::new(
            format!("{}.pending_reads", core::any::type_name::<D>()).into(),
            format!("{}.txn_timestamps", core::any::type_name::<D>()).into(),
            transaction_opts.detect_conflicts(),
            next_ts,
          );
          orc.read_mark.done_unchecked(next_ts);
          orc.txn_mark.done_unchecked(next_ts);
          orc.increment_next_ts();
          orc
        },
        db,
        opts: transaction_opts,
        hasher,
      }),
    })
  }

  /// Returns a timestamp which hints that any versions under this timestamp can be discard.
  /// This is useful when users want to implement compaction/merge functionality.
  pub fn discard_hint(&self) -> u64 {
    self.inner.orc.discard_at_or_below()
  }

  /// Returns the options of the database.
  pub fn database_options(&self) -> &D::Options {
    self.inner.db.options()
  }

  /// Returns the options of the transaction.
  pub fn transaction_options(&self) -> &Options {
    &self.inner.opts
  }

  /// Returns underlying database.
  ///
  /// **Note**: You should not use this method get the underlying database and read/write directly.
  /// This method is only for you to implement advanced functionalities, such as compaction, merge, etc.
  pub fn database(&self) -> &D {
    &self.inner.db
  }

  /// Create a new writable transaction.
  pub fn read(&self) -> ReadTransaction<D, S> {
    ReadTransaction {
      db: self.clone(),
      read_ts: self.inner.orc.read_ts(),
    }
  }

  #[inline]
  fn orc(&self) -> &Oracle<S> {
    &self.inner.orc
  }
}

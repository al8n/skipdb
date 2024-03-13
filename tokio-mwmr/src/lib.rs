//! A multi-writer-multi-reader MVCC, ACID, Serializable Snapshot Isolation transaction manager for database development.
#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::sync::Arc;

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

pub use mwmr_core::{future::*, types::*};

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

struct Inner<D, H = std::hash::RandomState> {
  db: D,
  /// Determines whether the transactions would be checked for conflicts.
  /// The transactions can be processed at a higher rate when conflict detection is disabled.
  opts: Options,
  orc: Oracle<H>,
  hasher: H,
}
/// A multi-writer multi-reader MVCC, ACID, Serializable Snapshot Isolation transaction manager.
pub struct TransactionDB<D, H = std::hash::RandomState> {
  inner: Arc<Inner<D, H>>,
}

impl<D, H> Clone for TransactionDB<D, H> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<D, H> TransactionDB<D, H>
where
  D: AsyncDatabase,
  D::Key: Eq + core::hash::Hash + Send + Sync + 'static,
  D::Value: Send + Sync + 'static,

  H: BuildHasher + Default + Clone + Send + Sync + 'static,
{
  /// Create a new writable transaction with
  /// the default pending writes manager to store the pending writes.
  pub async fn write(&self) -> WriteTransaction<D, AsyncIndexMapManager<D::Key, D::Value, H>, H> {
    WriteTransaction {
      db: self.clone(),
      read_ts: self.inner.orc.read_ts().await,
      size: 0,
      count: 0,
      reads: MediumVec::new(),
      conflict_keys: if self.inner.opts.detect_conflicts {
        Some(IndexSet::with_hasher(self.inner.hasher.clone()))
      } else {
        None
      },
      pending_writes: Some(IndexMap::with_hasher(H::default())),
      duplicate_writes: OneOrMore::new(),
      discarded: false,
      done_read: false,
    }
  }
}

impl<D: AsyncDatabase, H: Clone + 'static> TransactionDB<D, H> {
  /// Create a new writable transaction with the given pending writes manager to store the pending writes.
  pub async fn write_by<W: AsyncPendingManager>(&self, backend: W) -> WriteTransaction<D, W, H> {
    WriteTransaction {
      db: self.clone(),
      read_ts: self.inner.orc.read_ts().await,
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

impl<D: AsyncDatabase, H: Default> TransactionDB<D, H> {
  /// Open the database with the given options.
  pub async fn new(transaction_opts: Options, database_opts: D::Options) -> Result<Self, D::Error> {
    Self::with_hasher(transaction_opts, database_opts, H::default()).await
  }
}

impl<D: AsyncDatabase, H> TransactionDB<D, H> {
  /// Open the database with the given options.
  pub async fn with_hasher(
    transaction_opts: Options,
    database_opts: D::Options,
    hasher: H,
  ) -> Result<Self, D::Error> {
    let db = D::open(database_opts).await?;

    Ok(Self {
      inner: Arc::new(Inner {
        orc: {
          let next_ts = db.maximum_version();
          let orc = Oracle::new(
            format!("{}.pending_reads", core::any::type_name::<D>()).into(),
            format!("{}.txn_timestamps", core::any::type_name::<D>()).into(),
            transaction_opts.detect_conflicts(),
            next_ts,
          );
          orc
            .read_mark
            .done(next_ts)
            .expect("watermark canceled by accident");
          orc
            .txn_mark
            .done(next_ts)
            .expect("watermark canceled by accident");
          orc.increment_next_ts().await;
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
  pub async fn read(&self) -> ReadTransaction<D, H> {
    ReadTransaction {
      db: self.clone(),
      read_ts: self.inner.orc.read_ts().await,
    }
  }

  #[inline]
  fn orc(&self) -> &Oracle<H> {
    &self.inner.orc
  }
}

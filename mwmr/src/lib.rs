#![allow(clippy::type_complexity)]

use std::{cell::RefCell, sync::Arc};

use core::{hash::BuildHasher, mem};

use cheap_clone::CheapClone;
use either::Either;
use indexmap::{IndexMap, IndexSet};
use smallvec_wrapper::MediumVec;
pub use smallvec_wrapper::OneOrMore;

mod oracle;
use oracle::*;
mod read;
pub use read::*;
mod write;
pub use write::*;

/// Generic unit tests for users to test their database implementation based on `mwmr`.
#[cfg(any(feature = "test", test))]
pub mod tests;

#[derive(thiserror::Error)]
pub enum TransactionError<P: PendingManager> {
  /// Returned if an update function is called on a read-only transaction.
  #[error("transaction is read-only")]
  ReadOnly,

  /// Returned when a transaction conflicts with another transaction. This can
  /// happen if the read rows had been updated concurrently by another transaction.
  #[error("transaction conflict, please retry")]
  Conflict,

  /// Returned if a previously discarded transaction is re-used.
  #[error("transaction has been discarded, please create a new one")]
  Discard,

  /// Returned if too many writes are fit into a single transaction.
  #[error("transaction is too large")]
  LargeTxn,

  /// Returned if the transaction manager error occurs.
  #[error("transaction manager error: {0}")]
  Manager(P::Error),
}

impl<P: PendingManager> core::fmt::Debug for TransactionError<P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::ReadOnly => write!(f, "ReadOnly"),
      Self::Conflict => write!(f, "Conflict"),
      Self::Discard => write!(f, "Discard"),
      Self::LargeTxn => write!(f, "LargeTxn"),
      Self::Manager(e) => write!(f, "Manager({:?})", e),
    }
  }
}

#[derive(thiserror::Error)]
pub enum Error<D: Database, P: PendingManager> {
  /// Returned if transaction related error occurs.
  #[error(transparent)]
  Transaction(#[from] TransactionError<P>),

  /// Returned if DB related error occurs.
  #[error(transparent)]
  DB(D::Error),
}

impl<D: Database, P: PendingManager> core::fmt::Debug for Error<D, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Transaction(e) => e.fmt(f),
      Self::DB(e) => e.fmt(f),
    }
  }
}

impl<D: Database, P: PendingManager> Error<D, P> {
  /// Create a new error from the database error.
  pub fn database(err: D::Error) -> Self {
    Self::DB(err)
  }

  /// Create a new error from the transaction error.
  pub fn transaction(err: TransactionError<P>) -> Self {
    Self::Transaction(err)
  }
}

/// A reference to a key.
pub struct KeyRef<'a, K: 'a> {
  key: &'a K,
  version: u64,
}

impl<'a, K: 'a> KeyRef<'a, K> {
  /// Returns the key.
  pub fn key(&self) -> &K {
    self.key
  }

  /// Returns the version of the key.
  ///
  /// This version is useful when you want to implement MVCC.
  pub fn version(&self) -> u64 {
    self.version
  }
}

/// An item that is either prefetched or fetched from the database.
pub enum Item<'a, D: Database> {
  /// A pending item, which means that this item is still in a
  /// transaction, and not yet commit to the database.
  Pending(EntryRef<'a, D>),
  /// An item comes from the database, some db implementation is
  /// Copy-on-Write, so this enum varint allows such kind of behavior
  Borrowed(D::ItemRef<'a>),
  /// An item comes from the database, some db implementation is not
  /// Copy-on-Write, so this enum varint allows such kind of behavior
  Owned(D::Item),
}

impl<'a, D: Database> Item<'a, D> {
  /// Returns the prefetched item.
  ///
  /// # Panic
  /// If the item is not prefetched
  pub fn unwrap_pending(&self) -> EntryRef<'a, D> {
    match self {
      Item::Pending(item) => *item,
      _ => panic!("expected pending item"),
    }
  }

  /// Returns the borrowed item.
  ///
  /// # Panic
  /// If the item is not borrowed
  pub fn unwrap_borrow(&self) -> &D::ItemRef<'a> {
    match self {
      Item::Borrowed(item) => item,
      _ => panic!("expected borrowed item"),
    }
  }

  /// Returns the owned item.
  ///
  /// # Panic
  /// If the item is not owned
  pub fn unwrap_owned(self) -> D::Item {
    match self {
      Item::Owned(item) => item,
      _ => panic!("expected owned item"),
    }
  }

  /// Returns the owned item ref.
  ///
  /// # Panic
  /// If the item is not owned
  pub fn unwrap_owned_ref(&self) -> &D::Item {
    match self {
      Item::Owned(item) => item,
      _ => panic!("expected owned item"),
    }
  }

  /// Returns the committed item.
  ///
  /// # Panic
  /// If the item is not committed
  pub fn unwrap_committed(self) -> Either<D::ItemRef<'a>, D::Item> {
    match self {
      Item::Borrowed(item) => Either::Left(item),
      Item::Owned(item) => Either::Right(item),
      _ => panic!("expected committed item"),
    }
  }

  /// Returns the committed item ref.
  ///
  /// # Panic
  /// If the item is not committed
  pub fn unwrap_committed_ref(&self) -> Either<&D::ItemRef<'a>, &D::Item> {
    match self {
      Item::Borrowed(item) => Either::Left(item),
      Item::Owned(item) => Either::Right(item),
      _ => panic!("expected committed item"),
    }
  }
}

/// The reference of the [`Entry`].
pub struct EntryRef<'a, D: Database> {
  data: EntryDataRef<'a, D>,
  version: u64,
}

impl<'a, D: Database> core::fmt::Debug for EntryRef<'a, D> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("EntryRef")
      .field("version", &self.version)
      .field("data", &self.data)
      .finish()
  }
}

impl<'a, D: Database> Clone for EntryRef<'a, D> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, D: Database> Copy for EntryRef<'a, D> {}

impl<'a, D: Database> EntryRef<'a, D> {
  /// Get the data of the entry.
  #[inline]
  pub const fn data(&self) -> &EntryDataRef<'a, D> {
    &self.data
  }

  /// Get the value of the entry, if None, it means the entry is removed.
  #[inline]
  pub const fn value(&self) -> Option<&D::Value> {
    match self.data {
      EntryDataRef::Insert { value, .. } => Some(value),
      EntryDataRef::Remove(_) => None,
    }
  }

  /// Returns the version of the entry.
  ///
  /// This version is useful when you want to implement MVCC.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// The reference of the [`EntryData`].
pub enum EntryDataRef<'a, D: Database> {
  /// Insert the key and the value.
  Insert {
    /// key of the entry.
    key: &'a D::Key,
    /// value of the entry.
    value: &'a D::Value,
  },
  /// Remove the key.
  Remove(&'a D::Key),
}

impl<'a, D: Database> core::fmt::Debug for EntryDataRef<'a, D> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Insert { key, value } => f
        .debug_struct("Insert")
        .field("key", key)
        .field("value", value)
        .finish(),
      Self::Remove(key) => f.debug_tuple("Remove").field(key).finish(),
    }
  }
}

impl<'a, D: Database> Clone for EntryDataRef<'a, D> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, D: Database> Copy for EntryDataRef<'a, D> {}

/// The data of the [`Entry`].
pub enum EntryData<D: Database> {
  /// Insert the key and the value.
  Insert {
    /// key of the entry.
    key: D::Key,
    /// value of the entry.
    value: D::Value,
  },
  /// Remove the key.
  Remove(D::Key),
}

impl<D: Database> EntryData<D> {
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &D::Key {
    match self {
      Self::Insert { key, .. } => key,
      Self::Remove(key) => key,
    }
  }
}

impl<D: Database> core::fmt::Debug for EntryData<D> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Insert { key, value } => f
        .debug_struct("Insert")
        .field("key", key)
        .field("value", value)
        .finish(),
      Self::Remove(key) => f.debug_tuple("Remove").field(key).finish(),
    }
  }
}

impl<D: Database> Clone for EntryData<D>
where
  D::Key: Clone,
  D::Value: Clone,
{
  fn clone(&self) -> Self {
    match self {
      Self::Insert { key, value } => Self::Insert {
        key: key.clone(),
        value: value.clone(),
      },
      Self::Remove(key) => Self::Remove(key.clone()),
    }
  }
}

impl<D: Database> CheapClone for EntryData<D>
where
  D::Key: CheapClone,
  D::Value: CheapClone,
{
  fn cheap_clone(&self) -> Self {
    match self {
      Self::Insert { key, value } => Self::Insert {
        key: key.cheap_clone(),
        value: value.cheap_clone(),
      },
      Self::Remove(key) => Self::Remove(key.cheap_clone()),
    }
  }
}

/// An entry can be persisted to the database.
pub struct Entry<D: Database> {
  version: u64,
  data: EntryData<D>,
}

impl<D: Database> core::fmt::Debug for Entry<D> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("version", &self.version)
      .field("data", &self.data)
      .finish()
  }
}

impl<D: Database> Clone for Entry<D>
where
  D::Key: Clone,
  D::Value: Clone,
{
  fn clone(&self) -> Self {
    Self {
      version: self.version,
      data: self.data.clone(),
    }
  }
}

impl<D: Database> CheapClone for Entry<D>
where
  D::Key: CheapClone,
  D::Value: CheapClone,
{
  fn cheap_clone(&self) -> Self {
    Self {
      version: self.version,
      data: self.data.cheap_clone(),
    }
  }
}

impl<D: Database> Entry<D> {
  /// Returns the data contained by the entry.
  #[inline]
  pub const fn data(&self) -> &EntryData<D> {
    &self.data
  }

  /// Returns the version (can also be tought as transaction timestamp) of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Consumes the entry and returns the version and the entry data.
  #[inline]
  pub fn into_components(self) -> (u64, EntryData<D>) {
    (self.version, self.data)
  }

  #[inline]
  fn key(&self) -> &D::Key {
    match &self.data {
      EntryData::Insert { key, .. } => key,
      EntryData::Remove(key) => key,
    }
  }

  fn split(self) -> (D::Key, EntryValue<D::Value>) {
    let Entry { data, version } = self;

    let (key, value) = match data {
      EntryData::Insert { key, value } => (key, Some(value)),
      EntryData::Remove(key) => (key, None),
    };
    (key, EntryValue { value, version })
  }

  fn unsplit(key: D::Key, value: EntryValue<D::Value>) -> Self {
    let EntryValue { value, version } = value;
    Entry {
      data: match value {
        Some(value) => EntryData::Insert { key, value },
        None => EntryData::Remove(key),
      },
      version,
    }
  }
}

#[doc(hidden)]
pub struct EntryValue<V> {
  version: u64,
  value: Option<V>,
}

impl<V> Clone for EntryValue<V>
where
  V: Clone,
{
  fn clone(&self) -> Self {
    Self {
      version: self.version,
      value: self.value.clone(),
    }
  }
}

impl<V> CheapClone for EntryValue<V>
where
  V: CheapClone,
{
  fn cheap_clone(&self) -> Self {
    Self {
      version: self.version,
      value: self.value.cheap_clone(),
    }
  }
}

/// Used to set options when iterating over key-value
/// stores.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct KeysOptions {
  /// The number of KV pairs to prefetch while iterating.
  ///
  /// Some databases optimize iteration by prefetching
  pub prefetch_size: usize,
  /// Direction of iteration. False is forward, true is backward.
  pub reverse: bool,
  /// Fetch all valid versions of the same key.
  pub all_versions: bool,
  /// Only read data that has version > `since_version`.
  pub since_version: u64,
}

impl Default for KeysOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl KeysOptions {
  /// Create a new iterator options with default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      prefetch_size: 0,
      reverse: false,
      all_versions: false,
      since_version: 0,
    }
  }

  /// Set the number of KV pairs to prefetch while iterating.
  #[inline]
  pub fn set_prefetch_size(&mut self, prefetch_size: usize) -> &mut Self {
    self.prefetch_size = prefetch_size;
    self
  }

  /// Set the number of KV pairs to prefetch while iterating.
  #[inline]
  pub const fn with_prefetch_size(mut self, prefetch_size: usize) -> Self {
    self.prefetch_size = prefetch_size;
    self
  }

  /// Set the direction of iteration. False is forward, true is backward.
  #[inline]
  pub fn set_reverse(&mut self, reverse: bool) -> &mut Self {
    self.reverse = reverse;
    self
  }

  /// Set the direction of iteration. False is forward, true is backward.
  #[inline]
  pub const fn with_reverse(mut self, reverse: bool) -> Self {
    self.reverse = reverse;
    self
  }

  /// Set whether to fetch all valid versions of the same key.
  #[inline]
  pub fn set_all_versions(&mut self, all_versions: bool) -> &mut Self {
    self.all_versions = all_versions;
    self
  }

  /// Set whether to fetch all valid versions of the same key.
  #[inline]
  pub const fn with_all_versions(mut self, all_versions: bool) -> Self {
    self.all_versions = all_versions;
    self
  }

  /// Set the version to start reading from.
  #[inline]
  pub fn set_since_version(&mut self, since_version: u64) -> &mut Self {
    self.since_version = since_version;
    self
  }

  /// Set the version to start reading from.
  #[inline]
  pub const fn with_since_version(mut self, since_version: u64) -> Self {
    self.since_version = since_version;
    self
  }
}

/// Used to set options when iterating over key-value
/// stores.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct IteratorOptions {
  /// The number of KV pairs to prefetch while iterating.
  ///
  /// Some databases optimize iteration by prefetching
  pub prefetch_size: usize,
  /// Indicates whether we should prefetch values during
  /// iteration and store them.
  ///
  /// Some databases use key-value separation for optimization
  /// and this option can be used to prefetch values.
  pub prefetch_values: bool,
  /// Direction of iteration. False is forward, true is backward.
  pub reverse: bool,
  /// Fetch all valid versions of the same key.
  pub all_versions: bool,
  /// Only read data that has version > `since_version`.
  pub since_version: u64,
}

impl Default for IteratorOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl IteratorOptions {
  /// Create a new iterator options with default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      prefetch_size: 0,
      prefetch_values: false,
      reverse: false,
      all_versions: false,
      since_version: 0,
    }
  }

  /// Set the number of KV pairs to prefetch while iterating.
  #[inline]
  pub fn set_prefetch_size(&mut self, prefetch_size: usize) -> &mut Self {
    self.prefetch_size = prefetch_size;
    self
  }

  /// Set the number of KV pairs to prefetch while iterating.
  #[inline]
  pub const fn with_prefetch_size(mut self, prefetch_size: usize) -> Self {
    self.prefetch_size = prefetch_size;
    self
  }

  /// Set whether we should prefetch values during iteration and store them.
  #[inline]
  pub fn set_prefetch_values(&mut self, prefetch_values: bool) -> &mut Self {
    self.prefetch_values = prefetch_values;
    self
  }

  /// Set whether we should prefetch values during iteration and store them.
  #[inline]
  pub const fn with_prefetch_values(mut self, prefetch_values: bool) -> Self {
    self.prefetch_values = prefetch_values;
    self
  }

  /// Set the direction of iteration. False is forward, true is backward.
  #[inline]
  pub fn set_reverse(&mut self, reverse: bool) -> &mut Self {
    self.reverse = reverse;
    self
  }

  /// Set the direction of iteration. False is forward, true is backward.
  #[inline]
  pub const fn with_reverse(mut self, reverse: bool) -> Self {
    self.reverse = reverse;
    self
  }

  /// Set whether to fetch all valid versions of the same key.
  #[inline]
  pub fn set_all_versions(&mut self, all_versions: bool) -> &mut Self {
    self.all_versions = all_versions;
    self
  }

  /// Set whether to fetch all valid versions of the same key.
  #[inline]
  pub const fn with_all_versions(mut self, all_versions: bool) -> Self {
    self.all_versions = all_versions;
    self
  }

  /// Set the version to start reading from.
  #[inline]
  pub fn set_since_version(&mut self, since_version: u64) -> &mut Self {
    self.since_version = since_version;
    self
  }

  /// Set the version to start reading from.
  #[inline]
  pub const fn with_since_version(mut self, since_version: u64) -> Self {
    self.since_version = since_version;
    self
  }
}

pub trait Database: Sized + 'static {
  /// The error type returned by the database.
  type Error: std::error::Error + 'static;
  /// The options type of the database, which used to create the database.
  type Options;
  /// The key type of the database.
  type Key: core::fmt::Debug + 'static;
  /// The value type of the database.
  type Value: core::fmt::Debug + 'static;
  /// The owned item type can be returned by `get`.
  type Item: 'static;
  /// The reference item type can be returned by `get`.
  type ItemRef<'a>
  where
    Self: 'a;
  /// The iterator type of the database.
  type Iterator<'a>
  where
    Self: 'a;
  /// The key iterator type of the database.
  type Keys<'a>
  where
    Self: 'a;

  /// Returns the maximum batch size in bytes
  fn max_batch_size(&self) -> u64;

  /// Returns the maximum entries in batch
  fn max_batch_entries(&self) -> u64;

  /// Returns the estimated size of the entry in bytes when persisted in the database.
  fn estimate_size(&self, entry: &Entry<Self>) -> u64;

  /// Validate if the entry is valid for this database.
  ///
  /// e.g.
  /// - If the entry is expired
  /// - If the key or the value is too large
  /// - If the key or the value is empty
  /// - If the key or the value contains invalid characters
  /// - and etc.
  fn validate_entry(&self, entry: &Entry<Self>) -> Result<(), Self::Error>;

  /// Returns the maximum version of the entry in the database, if you are not implementing MVCC, you can just ignore this method.
  fn maximum_version(&self) -> u64;

  /// Returns the options of the database.
  fn options(&self) -> &Self::Options;

  /// Open the database with the given options.
  fn open(opts: Self::Options) -> Result<Self, Self::Error>;

  /// Returns the fingerprint of key.
  ///
  /// Implementors should ensure that the fingerprint is consistent for the same key.
  fn fingerprint(&self, k: &Self::Key) -> u64;

  /// Applies a series of entries to the database. This method will be invoked in [`Mwmr::commit`] method.
  ///
  /// This method is responsible for persisting a batch of entries to the database. It is called
  /// after the entries have been prepared and serialized by a higher-level [`Mwmr`] transaction manager,
  /// ensuring that consistency and isolation requirements are already satisfied. Users of this
  /// method do not need to handle these concerns; they can simply pass the entries to be applied.
  ///
  /// # Implementation Notes
  ///
  /// Implementors of this method must ensure atomicity of the apply operation; either all entries
  /// are applied successfully, or none are, to prevent partial updates to the database state. It is
  /// assumed that the consistency and isolation levels required for the entries have been managed
  /// by a higher-level transaction manager before invocation of this method.
  fn apply(&self, entries: OneOrMore<Entry<Self>>) -> Result<(), Self::Error>;

  /// Get the item from the database by the key and the version (version can be used for MVCC).
  fn get(
    &self,
    k: &Self::Key,
    version: u64,
  ) -> Result<Option<Either<Self::ItemRef<'_>, Self::Item>>, Self::Error>;

  /// Accepts an iterator of pending and returns an combined iterator.
  ///
  /// It depends on the database implementation to decide how to handle the `pending` and construct
  /// the final conbined iterator.
  ///
  /// The order of the items in the iterator depends on the [`PendingManager`] of the [`WriteTransaction`].
  ///
  /// e.g.
  /// - if users create [`WriteTransaction`] with [`IndexMap`] as the [`PendingManager`], the order of the
  /// entries in the iterator will be the same as the insertion order.
  /// - if users create [`WriteTransaction`] with [`BTreeCache`] as the [`PendingManager`], the order of the
  /// entires in the iterator will be sorted by key.
  fn iter<'a, 'b: 'a>(
    &'a self,
    pending: impl Iterator<Item = EntryRef<'b, Self>> + 'b,
    transaction_version: u64,
    opts: IteratorOptions,
  ) -> Self::Iterator<'a>;

  /// Accepts an iterator of pending keys and returns an combined iterator.
  ///
  /// It depends on the database implementation to decide how to handle the `pending` and construct
  /// the final conbined iterator.
  ///
  /// The order of the items in the iterator depends on the [`PendingManager`] of the [`WriteTransaction`].
  ///
  /// e.g.
  /// - if users create [`WriteTransaction`] with [`IndexMap`] as the [`PendingManager`], the order of the
  /// keys in the iterator will be the same as the insertion order.
  /// - if users create [`WriteTransaction`] with [`BTreeCache`] as the [`PendingManager`], the order of the
  /// keys in the iterator will be sorted by key.
  fn keys<'a, 'b: 'a>(
    &'a self,
    pending: impl Iterator<Item = KeyRef<'b, Self::Key>> + 'b,
    transaction_version: u64,
    opts: KeysOptions,
  ) -> Self::Keys<'a>;
}

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
/// A multi-writer multi-reader Serializable Snapshot Isolation database.
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

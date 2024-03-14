#![allow(clippy::type_complexity)]

use std::{collections::BTreeMap, hash::BuildHasher};

use either::Either;
use indexmap::IndexMap;
use smallvec_wrapper::OneOrMore;

/// Types
pub mod types {
  use core::cmp::Reverse;

  use super::*;
  use cheap_clone::CheapClone;

  /// Key is a versioned key
  #[derive(Debug, Clone, PartialEq, Eq, Hash)]
  pub struct Key<K> {
    key: K,
    version: u64,
  }

  impl<K> Key<K> {
    /// Create a new key with default version.
    pub fn new(key: K) -> Key<K> {
      Key { key, version: 0 }
    }

    /// Returns the key.
    #[inline]
    pub const fn key(&self) -> &K {
      &self.key
    }

    /// Returns the version of the key.
    #[inline]
    pub const fn version(&self) -> u64 {
      self.version
    }

    /// Set the version of the key.
    #[inline]
    pub fn set_version(&mut self, version: u64) {
      self.version = version;
    }

    /// Set the version of the key.
    #[inline]
    pub const fn with_version(mut self, version: u64) -> Self {
      self.version = version;
      self
    }

    /// Consumes the key and returns the key and the version.
    #[inline]
    pub fn into_components(self) -> (K, u64) {
      (self.key, self.version)
    }
  }

  impl<K: Ord> PartialOrd for Key<K> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
      Some(self.cmp(other))
    }
  }

  impl<K: Ord> Ord for Key<K> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
      self
        .key
        .cmp(&other.key)
        .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
    }
  }

  /// A reference to a key.
  pub struct KeyRef<'a, K: 'a> {
    /// The key.
    pub key: &'a K,
    /// The version of the key.
    pub version: u64,
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
  pub enum Item<'a, K, V, B, O> {
    /// A pending item, which means that this item is still in a
    /// transaction, and not yet commit to the database.
    Pending(EntryRef<'a, K, V>),
    /// An item comes from the database, some db implementation is
    /// Copy-on-Write, so this enum varint allows such kind of behavior
    Borrowed(B),
    /// An item comes from the database, some db implementation is not
    /// Copy-on-Write, so this enum varint allows such kind of behavior
    Owned(O),
  }

  impl<'a, K, V, B, O> Item<'a, K, V, B, O> {
    /// Returns the prefetched item.
    ///
    /// # Panic
    /// If the item is not prefetched
    pub fn unwrap_pending(&self) -> EntryRef<'a, K, V> {
      match self {
        Item::Pending(item) => *item,
        _ => panic!("expected pending item"),
      }
    }

    /// Returns the borrowed item.
    ///
    /// # Panic
    /// If the item is not borrowed
    pub fn unwrap_borrow(&self) -> &B {
      match self {
        Item::Borrowed(item) => item,
        _ => panic!("expected borrowed item"),
      }
    }

    /// Returns the owned item.
    ///
    /// # Panic
    /// If the item is not owned
    pub fn unwrap_owned(self) -> O {
      match self {
        Item::Owned(item) => item,
        _ => panic!("expected owned item"),
      }
    }

    /// Returns the owned item ref.
    ///
    /// # Panic
    /// If the item is not owned
    pub fn unwrap_owned_ref(&self) -> &O {
      match self {
        Item::Owned(item) => item,
        _ => panic!("expected owned item"),
      }
    }

    /// Returns the committed item.
    ///
    /// # Panic
    /// If the item is not committed
    pub fn unwrap_committed(self) -> Either<B, O> {
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
    pub fn unwrap_committed_ref(&self) -> Either<&B, &O> {
      match self {
        Item::Borrowed(item) => Either::Left(item),
        Item::Owned(item) => Either::Right(item),
        _ => panic!("expected committed item"),
      }
    }
  }

  /// The reference of the [`Entry`].
  pub struct EntryRef<'a, K, V> {
    pub data: EntryDataRef<'a, K, V>,
    pub version: u64,
  }

  impl<'a, K: core::fmt::Debug, V: core::fmt::Debug> core::fmt::Debug for EntryRef<'a, K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("EntryRef")
        .field("version", &self.version)
        .field("data", &self.data)
        .finish()
    }
  }

  impl<'a, K, V> Clone for EntryRef<'a, K, V> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<'a, K, V> Copy for EntryRef<'a, K, V> {}

  impl<'a, K, V> EntryRef<'a, K, V> {
    /// Get the data of the entry.
    #[inline]
    pub const fn data(&self) -> &EntryDataRef<'a, K, V> {
      &self.data
    }

    /// Get the value of the entry, if None, it means the entry is removed.
    #[inline]
    pub const fn value(&self) -> Option<&V> {
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
  pub enum EntryDataRef<'a, K, V> {
    /// Insert the key and the value.
    Insert {
      /// key of the entry.
      key: &'a K,
      /// value of the entry.
      value: &'a V,
    },
    /// Remove the key.
    Remove(&'a K),
  }

  impl<'a, K: core::fmt::Debug, V: core::fmt::Debug> core::fmt::Debug for EntryDataRef<'a, K, V> {
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

  impl<'a, K, V> Clone for EntryDataRef<'a, K, V> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<'a, K, V> Copy for EntryDataRef<'a, K, V> {}

  /// The data of the [`Entry`].
  pub enum EntryData<K, V> {
    /// Insert the key and the value.
    Insert {
      /// key of the entry.
      key: K,
      /// value of the entry.
      value: V,
    },
    /// Remove the key.
    Remove(K),
  }

  impl<K, V> EntryData<K, V> {
    /// Returns the key of the entry.
    #[inline]
    pub const fn key(&self) -> &K {
      match self {
        Self::Insert { key, .. } => key,
        Self::Remove(key) => key,
      }
    }
  }

  impl<K: core::fmt::Debug, V: core::fmt::Debug> core::fmt::Debug for EntryData<K, V> {
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

  impl<K, V> Clone for EntryData<K, V>
  where
    K: Clone,
    V: Clone,
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

  impl<K, V> CheapClone for EntryData<K, V>
  where
    K: CheapClone,
    V: CheapClone,
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
  pub struct Entry<K, V> {
    pub version: u64,
    pub data: EntryData<K, V>,
  }

  impl<K: core::fmt::Debug, V: core::fmt::Debug> core::fmt::Debug for Entry<K, V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("Entry")
        .field("version", &self.version)
        .field("data", &self.data)
        .finish()
    }
  }

  impl<K, V> Clone for Entry<K, V>
  where
    K: Clone,
    V: Clone,
  {
    fn clone(&self) -> Self {
      Self {
        version: self.version,
        data: self.data.clone(),
      }
    }
  }

  impl<K, V> CheapClone for Entry<K, V>
  where
    K: CheapClone,
    V: CheapClone,
  {
    fn cheap_clone(&self) -> Self {
      Self {
        version: self.version,
        data: self.data.cheap_clone(),
      }
    }
  }

  impl<K, V> Entry<K, V> {
    /// Returns the data contained by the entry.
    #[inline]
    pub const fn data(&self) -> &EntryData<K, V> {
      &self.data
    }

    /// Returns the version (can also be tought as transaction timestamp) of the entry.
    #[inline]
    pub const fn version(&self) -> u64 {
      self.version
    }

    /// Consumes the entry and returns the version and the entry data.
    #[inline]
    pub fn into_components(self) -> (u64, EntryData<K, V>) {
      (self.version, self.data)
    }

    /// Returns the key of the entry.
    #[inline]
    pub fn key(&self) -> &K {
      match &self.data {
        EntryData::Insert { key, .. } => key,
        EntryData::Remove(key) => key,
      }
    }

    /// Split the entry into its key and [`EntryValue`].
    pub fn split(self) -> (K, EntryValue<V>) {
      let Entry { data, version } = self;

      let (key, value) = match data {
        EntryData::Insert { key, value } => (key, Some(value)),
        EntryData::Remove(key) => (key, None),
      };
      (key, EntryValue { value, version })
    }

    /// Unsplit the key and [`EntryValue`] into an entry.
    pub fn unsplit(key: K, value: EntryValue<V>) -> Self {
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

  /// A entry value
  pub struct EntryValue<V> {
    /// The version of the entry.
    pub version: u64,
    /// The value of the entry.
    pub value: Option<V>,
  }

  impl<V: core::fmt::Debug> core::fmt::Debug for EntryValue<V> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("EntryValue")
        .field("version", &self.version)
        .field("value", &self.value)
        .finish()
    }
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
}

/// Traits for synchronization.
pub mod sync {
  use super::{types::*, *};

  /// The conflict manager that can be used to manage the conflicts in a transaction.
  pub trait ConflictManager: 'static {
    /// The error type returned by the pending manager.
    type Error: std::error::Error + 'static;
    /// The key type.
    type Key: 'static;

    /// Mark the key is read.
    fn mark_read(&mut self, key: &Self::Key) -> Result<(), Self::Error>;
  }

  /// A pending writes manager that can be used to store pending writes in a transaction.
  ///
  /// By default, there are two implementations of this trait:
  /// - [`IndexMap`]: A hash map with consistent ordering and fast lookups.
  /// - [`BTreeMap`]: A balanced binary tree with ordered keys and fast lookups.
  ///
  /// But, users can create their own implementations by implementing this trait.
  /// e.g. if you want to implement a recovery transaction manager, you can use a persistent
  /// storage to store the pending writes.
  pub trait PendingManager: 'static {
    /// The error type returned by the pending manager.
    type Error: std::error::Error + 'static;
    /// The key type.
    type Key: 'static;
    /// The value type.
    type Value: 'static;

    /// Returns true if the buffer is empty.
    fn is_empty(&self) -> bool;

    /// Returns the number of elements in the buffer.
    fn len(&self) -> usize;

    /// Returns a reference to the value corresponding to the key.
    fn get(&self, key: &Self::Key) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>;

    /// Inserts a key-value pair into the buffer.
    fn insert(&mut self, key: Self::Key, value: EntryValue<Self::Value>)
      -> Result<(), Self::Error>;

    /// Removes a key from the buffer, returning the key-value pair if the key was previously in the buffer.
    fn remove_entry(
      &mut self,
      key: &Self::Key,
    ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>;

    /// Returns an iterator over the keys in the buffer.
    fn keys(&self) -> impl Iterator<Item = &'_ Self::Key>;

    /// Returns an iterator over the key-value pairs in the buffer.
    fn iter(&self) -> impl Iterator<Item = (&'_ Self::Key, &'_ EntryValue<Self::Value>)>;

    /// Returns an iterator that consumes the buffer.
    fn into_iter(self) -> impl Iterator<Item = (Self::Key, EntryValue<Self::Value>)>;
  }

  /// A type alias for [`PendingManager`] that based on the [`IndexMap`].
  pub type IndexMapManager<K, V, S = std::hash::RandomState> = IndexMap<K, EntryValue<V>, S>;
  /// A type alias for [`PendingManager`] that based on the [`BTreeMap`].
  pub type BTreeMapManager<K, V> = BTreeMap<K, EntryValue<V>>;

  impl<K, V, S> PendingManager for IndexMap<K, EntryValue<V>, S>
  where
    K: Eq + core::hash::Hash + 'static,
    V: 'static,
    S: BuildHasher + Default + 'static,
  {
    type Error = std::convert::Infallible;
    type Key = K;
    type Value = V;

    fn is_empty(&self) -> bool {
      self.is_empty()
    }

    fn len(&self) -> usize {
      self.len()
    }

    fn get(&self, key: &K) -> Result<Option<&EntryValue<V>>, Self::Error> {
      Ok(self.get(key))
    }

    fn insert(&mut self, key: K, value: EntryValue<V>) -> Result<(), Self::Error> {
      self.insert(key, value);
      Ok(())
    }

    fn remove_entry(&mut self, key: &K) -> Result<Option<(K, EntryValue<V>)>, Self::Error> {
      Ok(self.shift_remove_entry(key))
    }

    fn keys(&self) -> impl Iterator<Item = &K> {
      self.keys()
    }

    fn iter(&self) -> impl Iterator<Item = (&K, &EntryValue<V>)> {
      self.iter()
    }

    fn into_iter(self) -> impl Iterator<Item = (K, EntryValue<V>)> {
      core::iter::IntoIterator::into_iter(self)
    }
  }

  impl<K, V> PendingManager for BTreeMap<K, EntryValue<V>>
  where
    K: Eq + core::hash::Hash + Ord + 'static,
    V: 'static,
  {
    type Error = std::convert::Infallible;
    type Key = K;
    type Value = V;

    fn is_empty(&self) -> bool {
      self.is_empty()
    }

    fn len(&self) -> usize {
      self.len()
    }

    fn get(&self, key: &K) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
      Ok(self.get(key))
    }

    fn insert(&mut self, key: K, value: EntryValue<Self::Value>) -> Result<(), Self::Error> {
      self.insert(key, value);
      Ok(())
    }

    fn remove_entry(
      &mut self,
      key: &K,
    ) -> Result<Option<(K, EntryValue<Self::Value>)>, Self::Error> {
      Ok(self.remove_entry(key))
    }

    fn keys(&self) -> impl Iterator<Item = &K> {
      self.keys()
    }

    fn iter(&self) -> impl Iterator<Item = (&K, &EntryValue<Self::Value>)> {
      self.iter()
    }

    fn into_iter(self) -> impl Iterator<Item = (K, EntryValue<Self::Value>)> {
      core::iter::IntoIterator::into_iter(self)
    }
  }

  /// An abstraction of database which can be managed by the [`TransactionDB`].
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
    fn estimate_size(&self, entry: &Entry<Self::Key, Self::Value>) -> u64;

    /// Validate if the entry is valid for this database.
    ///
    /// e.g.
    /// - If the entry is expired
    /// - If the key or the value is too large
    /// - If the key or the value is empty
    /// - If the key or the value contains invalid characters
    /// - and etc.
    fn validate_entry(&self, entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error>;

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
    fn apply(&self, entries: OneOrMore<Entry<Self::Key, Self::Value>>) -> Result<(), Self::Error>;

    /// Get the item from the database by the key and the version (version can be used for MVCC).
    fn get<'a, 'b: 'a>(
      &'a self,
      k: &'b Self::Key,
      version: u64,
    ) -> Result<Option<Either<Self::ItemRef<'a>, Self::Item>>, Self::Error>;

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
      pending: impl Iterator<Item = EntryRef<'b, Self::Key, Self::Value>> + 'b,
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
}

/// Traits for asynchronous.
pub mod future {
  use super::{types::*, *};
  use core::future::Future;

  /// A pending writes manager that can be used to store pending writes in a transaction.
  ///
  /// By default, there are two implementations of this trait:
  /// - [`IndexMap`]: A hash map with consistent ordering and fast lookups.
  /// - [`BTreeMap`]: A balanced binary tree with ordered keys and fast lookups.
  ///
  /// But, users can create their own implementations by implementing this trait.
  /// e.g. if you want to implement a recovery transaction manager, you can use a persistent
  /// storage to store the pending writes.
  pub trait AsyncPendingManager: Send + Sync + 'static {
    /// The error type returned by the pending manager.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The key type.
    type Key: Send + Sync + 'static;
    /// The value type.
    type Value: Send + Sync + 'static;

    /// Returns true if the buffer is empty.
    fn is_empty(&self) -> bool;

    /// Returns the number of elements in the buffer.
    fn len(&self) -> usize;

    /// Returns a reference to the value corresponding to the key.
    fn get(
      &self,
      key: &Self::Key,
    ) -> impl Future<Output = Result<Option<&EntryValue<Self::Value>>, Self::Error>> + Send;

    /// Inserts a key-value pair into the buffer.
    fn insert(
      &mut self,
      key: Self::Key,
      value: EntryValue<Self::Value>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes a key from the buffer, returning the key-value pair if the key was previously in the buffer.
    fn remove_entry(
      &mut self,
      key: &Self::Key,
    ) -> impl Future<Output = Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>> + Send;

    /// Returns an iterator over the keys in the buffer.
    fn keys(&self) -> impl Future<Output = impl Iterator<Item = &'_ Self::Key>> + Send;

    /// Returns an iterator over the key-value pairs in the buffer.
    fn iter(
      &self,
    ) -> impl Future<Output = impl Iterator<Item = (&'_ Self::Key, &'_ EntryValue<Self::Value>)>> + Send;

    /// Returns an iterator that consumes the buffer.
    fn into_iter(
      self,
    ) -> impl Future<Output = impl Iterator<Item = (Self::Key, EntryValue<Self::Value>)>> + Send;
  }

  /// A type alias for [`PendingManager`] that based on the [`IndexMap`].
  pub type AsyncIndexMapManager<K, V, S = std::hash::RandomState> = IndexMap<K, EntryValue<V>, S>;
  /// A type alias for [`PendingManager`] that based on the [`BTreeMap`].
  pub type AsyncBTreeMapManager<K, V> = BTreeMap<K, EntryValue<V>>;

  impl<K, V, S> AsyncPendingManager for IndexMap<K, EntryValue<V>, S>
  where
    K: Eq + core::hash::Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Default + Send + Sync + 'static,
  {
    type Error = std::convert::Infallible;
    type Key = K;
    type Value = V;

    fn is_empty(&self) -> bool {
      self.is_empty()
    }

    fn len(&self) -> usize {
      self.len()
    }

    async fn get(&self, key: &K) -> Result<Option<&EntryValue<V>>, Self::Error> {
      Ok(self.get(key))
    }

    async fn insert(&mut self, key: K, value: EntryValue<V>) -> Result<(), Self::Error> {
      self.insert(key, value);
      Ok(())
    }

    async fn remove_entry(&mut self, key: &K) -> Result<Option<(K, EntryValue<V>)>, Self::Error> {
      Ok(self.shift_remove_entry(key))
    }

    async fn keys(&self) -> impl Iterator<Item = &K> {
      self.keys()
    }

    async fn iter(&self) -> impl Iterator<Item = (&K, &EntryValue<V>)> {
      self.iter()
    }

    async fn into_iter(self) -> impl Iterator<Item = (K, EntryValue<V>)> {
      core::iter::IntoIterator::into_iter(self)
    }
  }

  impl<K, V> AsyncPendingManager for BTreeMap<K, EntryValue<V>>
  where
    K: Eq + core::hash::Hash + Ord + Send + Sync + 'static,
    V: Send + Sync + 'static,
  {
    type Error = std::convert::Infallible;
    type Key = K;
    type Value = V;

    fn is_empty(&self) -> bool {
      self.is_empty()
    }

    fn len(&self) -> usize {
      self.len()
    }

    async fn get(&self, key: &K) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
      Ok(self.get(key))
    }

    async fn insert(&mut self, key: K, value: EntryValue<Self::Value>) -> Result<(), Self::Error> {
      self.insert(key, value);
      Ok(())
    }

    async fn remove_entry(
      &mut self,
      key: &K,
    ) -> Result<Option<(K, EntryValue<Self::Value>)>, Self::Error> {
      Ok(self.remove_entry(key))
    }

    async fn keys(&self) -> impl Iterator<Item = &K> {
      self.keys()
    }

    async fn iter(&self) -> impl Iterator<Item = (&K, &EntryValue<Self::Value>)> {
      self.iter()
    }

    async fn into_iter(self) -> impl Iterator<Item = (K, EntryValue<Self::Value>)> {
      core::iter::IntoIterator::into_iter(self)
    }
  }

  /// An abstraction of database which can be managed by the [`TransactionDB`].
  pub trait AsyncDatabase: Sized + Send + Sync + 'static {
    /// The error type returned by the database.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The options type of the database, which used to create the database.
    type Options;
    /// The key type of the database.
    type Key: core::fmt::Debug + Send + Sync + 'static;
    /// The value type of the database.
    type Value: core::fmt::Debug + Send + Sync + 'static;
    /// The owned item type can be returned by `get`.
    type Item: Send + Sync + 'static;
    /// The reference item type can be returned by `get`.
    type ItemRef<'a>: Send + Sync
    where
      Self: 'a;
    /// The iterator type of the database.
    type Iterator<'a>: Send + Sync
    where
      Self: 'a;
    /// The key iterator type of the database.
    type Keys<'a>: Send + Sync
    where
      Self: 'a;

    /// Returns the maximum batch size in bytes
    fn max_batch_size(&self) -> u64;

    /// Returns the maximum entries in batch
    fn max_batch_entries(&self) -> u64;

    /// Returns the estimated size of the entry in bytes when persisted in the database.
    fn estimate_size(&self, entry: &Entry<Self::Key, Self::Value>) -> u64;

    /// Validate if the entry is valid for this database.
    ///
    /// e.g.
    /// - If the entry is expired
    /// - If the key or the value is too large
    /// - If the key or the value is empty
    /// - If the key or the value contains invalid characters
    /// - and etc.
    fn validate_entry(&self, entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error>;

    /// Returns the maximum version of the entry in the database, if you are not implementing MVCC, you can just ignore this method.
    fn maximum_version(&self) -> u64;

    /// Returns the options of the database.
    fn options(&self) -> &Self::Options;

    /// Open the database with the given options.
    fn open(opts: Self::Options) -> impl Future<Output = Result<Self, Self::Error>> + Send;

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
    fn apply(
      &self,
      entries: OneOrMore<Entry<Self::Key, Self::Value>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Get the item from the database by the key and the version (version can be used for MVCC).
    fn get(
      &self,
      k: &Self::Key,
      version: u64,
    ) -> impl Future<Output = Result<Option<Either<Self::ItemRef<'_>, Self::Item>>, Self::Error>> + Send;

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
      pending: impl Iterator<Item = EntryRef<'b, Self::Key, Self::Value>> + 'b,
      transaction_version: u64,
      opts: IteratorOptions,
    ) -> impl Future<Output = Self::Iterator<'a>> + 'a;

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
    ) -> impl Future<Output = Self::Keys<'a>> + 'a;
  }
}

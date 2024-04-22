//! Core traits and types for [`txn`](https://crates.io/crates/txn) and [async-txn](https://crates.io/crates/async-txn) crates.
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs, warnings)]
#![allow(clippy::type_complexity)]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub use cheap_clone::CheapClone;

/// Default hasher.
#[cfg(feature = "std")]
pub type DefaultHasher = std::collections::hash_map::DefaultHasher;

/// Default hasher.
#[cfg(not(feature = "std"))]
pub type DefaultHasher = ahash::AHasher;

/// Types
pub mod types {
  use cheap_clone::CheapClone;
  use core::cmp::{self, Reverse};

  /// The reference of the [`Entry`].
  #[derive(Debug, PartialEq, Eq, Hash)]
  pub struct EntryRef<'a, K, V> {
    /// The data reference of the entry.
    pub data: EntryDataRef<'a, K, V>,
    /// The version of the entry.
    pub version: u64,
  }

  impl<'a, K, V> Clone for EntryRef<'a, K, V> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<'a, K, V> Copy for EntryRef<'a, K, V> {}

  impl<'a, K, V> EntryRef<'a, K, V> {
    /// Get the key of the entry.
    #[inline]
    pub const fn key(&self) -> &K {
      match self.data {
        EntryDataRef::Insert { key, .. } => key,
        EntryDataRef::Remove(key) => key,
      }
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
  #[derive(Debug, PartialEq, Eq, Hash)]
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

  impl<'a, K, V> Clone for EntryDataRef<'a, K, V> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<'a, K, V> Copy for EntryDataRef<'a, K, V> {}

  /// The data of the [`Entry`].
  #[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

  impl<K: Ord, V: Eq> PartialOrd for EntryData<K, V> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
      Some(self.cmp(other))
    }
  }

  impl<K: Ord, V: Eq> Ord for EntryData<K, V> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
      self.key().cmp(other.key())
    }
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

    /// Returns the value of the entry, if None, it means the entry is marked as remove.
    #[inline]
    pub const fn value(&self) -> Option<&V> {
      match self {
        Self::Insert { value, .. } => Some(value),
        Self::Remove(_) => None,
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
  #[derive(Debug, PartialEq, Eq, Hash)]
  pub struct Entry<K, V> {
    /// The version of the entry.
    pub version: u64,
    /// The data of the entry.
    pub data: EntryData<K, V>,
  }

  impl<K: Ord, V: Eq> PartialOrd for Entry<K, V> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
      Some(self.cmp(other))
    }
  }

  impl<K: Ord, V: Eq> Ord for Entry<K, V> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
      self
        .data
        .key()
        .cmp(other.data.key())
        .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
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
  #[derive(Debug, PartialEq, Eq, Hash)]
  pub struct EntryValue<V> {
    /// The version of the entry.
    pub version: u64,
    /// The value of the entry.
    pub value: Option<V>,
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
}

/// Traits for synchronization.
pub mod sync;

/// Traits for asynchronous.
pub mod future;

/// Errors
pub mod error;

impl<T> future::AsyncCm for T
where
  T: sync::Cm,
{
  type Error = <T as sync::Cm>::Error;

  type Key = <T as sync::Cm>::Key;

  type Options = <T as sync::Cm>::Options;

  async fn new(options: Self::Options) -> Result<Self, Self::Error> {
    <T as sync::Cm>::new(options)
  }

  async fn mark_read(&mut self, key: &Self::Key) {
    <T as sync::Cm>::mark_read(self, key)
  }

  async fn mark_conflict(&mut self, key: &Self::Key) {
    <T as sync::Cm>::mark_conflict(self, key)
  }

  async fn has_conflict(&self, other: &Self) -> bool {
    <T as sync::Cm>::has_conflict(self, other)
  }

  async fn rollback(&mut self) -> Result<(), Self::Error> {
    <T as sync::Cm>::rollback(self)
  }
}

impl<T> future::AsyncCmComparable for T
where
  T: sync::CmComparable,
{
  async fn mark_read_comparable<Q>(&mut self, key: &Q)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as sync::CmComparable>::mark_read_comparable(self, key)
  }

  async fn mark_conflict_comparable<Q>(&mut self, key: &Q)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as sync::CmComparable>::mark_conflict_comparable(self, key)
  }
}

impl<T> future::AsyncCmEquivalent for T
where
  T: sync::CmEquivalent,
{
  async fn mark_read_equivalent<Q>(&mut self, key: &Q)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    <T as sync::CmEquivalent>::mark_read_equivalent(self, key)
  }

  async fn mark_conflict_equivalent<Q>(&mut self, key: &Q)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    <T as sync::CmEquivalent>::mark_conflict_equivalent(self, key)
  }
}

impl<T> future::AsyncPwm for T
where
  T: sync::Pwm,
{
  type Error = <T as sync::Pwm>::Error;

  type Key = <T as sync::Pwm>::Key;

  type Value = <T as sync::Pwm>::Value;

  type Options = <T as sync::Pwm>::Options;

  type Iter<'a> = <T as sync::Pwm>::Iter<'a> where Self: 'a;

  type IntoIter = <T as sync::Pwm>::IntoIter;

  async fn new(options: Self::Options) -> Result<Self, Self::Error> {
    <T as sync::Pwm>::new(options)
  }

  async fn is_empty(&self) -> bool {
    <T as sync::Pwm>::is_empty(self)
  }

  async fn len(&self) -> usize {
    <T as sync::Pwm>::len(self)
  }

  async fn validate_entry(
    &self,
    entry: &types::Entry<Self::Key, Self::Value>,
  ) -> Result<(), Self::Error> {
    <T as sync::Pwm>::validate_entry(self, entry)
  }

  fn max_batch_size(&self) -> u64 {
    <T as sync::Pwm>::max_batch_size(self)
  }

  fn max_batch_entries(&self) -> u64 {
    <T as sync::Pwm>::max_batch_entries(self)
  }

  fn estimate_size(&self, entry: &types::Entry<Self::Key, Self::Value>) -> u64 {
    <T as sync::Pwm>::estimate_size(self, entry)
  }

  async fn get(
    &self,
    key: &Self::Key,
  ) -> Result<Option<&types::EntryValue<Self::Value>>, Self::Error> {
    <T as sync::Pwm>::get(self, key)
  }

  async fn get_entry(
    &self,
    key: &Self::Key,
  ) -> Result<Option<(&Self::Key, &types::EntryValue<Self::Value>)>, Self::Error> {
    <T as sync::Pwm>::get_entry(self, key)
  }

  async fn contains_key(&self, key: &Self::Key) -> Result<bool, Self::Error> {
    <T as sync::Pwm>::contains_key(self, key)
  }

  async fn insert(
    &mut self,
    key: Self::Key,
    value: types::EntryValue<Self::Value>,
  ) -> Result<(), Self::Error> {
    <T as sync::Pwm>::insert(self, key, value)
  }

  async fn remove_entry(
    &mut self,
    key: &Self::Key,
  ) -> Result<Option<(Self::Key, types::EntryValue<Self::Value>)>, Self::Error> {
    <T as sync::Pwm>::remove_entry(self, key)
  }

  async fn rollback(&mut self) -> Result<(), Self::Error> {
    <T as sync::Pwm>::rollback(self)
  }

  async fn iter(&self) -> impl Iterator<Item = (&Self::Key, &types::EntryValue<Self::Value>)> {
    <T as sync::Pwm>::iter(self)
  }

  async fn into_iter(self) -> impl Iterator<Item = (Self::Key, types::EntryValue<Self::Value>)> {
    <T as sync::Pwm>::into_iter(self)
  }
}

impl<T> future::AsyncPwmComparable for T
where
  T: sync::PwmComparable,
{
  async fn get_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<&types::EntryValue<Self::Value>>, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as sync::PwmComparable>::get_comparable(self, key)
  }

  async fn get_entry_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &types::EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as sync::PwmComparable>::get_entry_comparable(self, key)
  }

  async fn contains_key_comparable<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as sync::PwmComparable>::contains_key_comparable(self, key)
  }

  async fn remove_entry_comparable<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(Self::Key, types::EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as sync::PwmComparable>::remove_entry_comparable(self, key)
  }
}

impl<T> future::AsyncPwmEquivalent for T
where
  T: sync::PwmEquivalent,
{
  async fn get_equivalent<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<&types::EntryValue<Self::Value>>, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    <T as sync::PwmEquivalent>::get_equivalent(self, key)
  }

  async fn get_entry_equivalent<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &types::EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    <T as sync::PwmEquivalent>::get_entry_equivalent(self, key)
  }

  async fn contains_key_equivalent<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    <T as sync::PwmEquivalent>::contains_key_equivalent(self, key)
  }

  async fn remove_entry_equivalent<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(Self::Key, types::EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    <T as sync::PwmEquivalent>::remove_entry_equivalent(self, key)
  }
}

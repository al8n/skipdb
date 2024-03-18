#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![forbid(unsafe_code)]
#![allow(clippy::type_complexity)]

use std::{collections::BTreeMap, hash::BuildHasher};

use indexmap::IndexMap;

pub use cheap_clone::CheapClone;

/// Types
pub mod types {
  use core::cmp::Reverse;

  use super::*;

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

  impl<K> core::borrow::Borrow<K> for Key<K> {
    #[inline]
    fn borrow(&self) -> &K {
      &self.key
    }
  }

  /// A reference to a key.
  #[derive(Debug, PartialEq, Eq, Hash)]
  pub struct KeyRef<'a, K: 'a> {
    /// The key.
    pub key: &'a K,
    /// The version of the key.
    pub version: u64,
  }

  impl<'a, K> Clone for KeyRef<'a, K> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<'a, K> Copy for KeyRef<'a, K> {}

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

  impl<'a, K: Ord> PartialOrd for KeyRef<'a, K> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
      Some(self.cmp(other))
    }
  }

  impl<'a, K: Ord> Ord for KeyRef<'a, K> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
      self
        .key
        .cmp(other.key)
        .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
    }
  }

  impl<'a, K> core::borrow::Borrow<K> for KeyRef<'a, K> {
    #[inline]
    fn borrow(&self) -> &K {
      self.key
    }
  }

  /// The reference of the [`Entry`].
  #[derive(Debug, PartialEq, Eq, Hash)]
  pub struct EntryRef<'a, K, V> {
    pub data: EntryDataRef<'a, K, V>,
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
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
      Some(self.cmp(other))
    }
  }

  impl<K: Ord, V: Eq> Ord for EntryData<K, V> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
      self
        .key()
        .cmp(other.key())
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
    pub version: u64,
    pub data: EntryData<K, V>,
  }

  impl<K: Ord, V: Eq> PartialOrd for Entry<K, V> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
      Some(self.cmp(other))
    }
  }

  impl<K: Ord, V: Eq> Ord for Entry<K, V> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
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

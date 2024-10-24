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
pub type DefaultHasher = std::collections::hash_map::RandomState;

/// Default hasher.
#[cfg(not(feature = "std"))]
pub type DefaultHasher = core::hash::BuildHasherDefault<ahash::AHasher>;

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

  impl<K, V> Clone for EntryRef<'_, K, V> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<K, V> Copy for EntryRef<'_, K, V> {}

  impl<K, V> EntryRef<'_, K, V> {
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

  impl<K, V> Clone for EntryDataRef<'_, K, V> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<K, V> Copy for EntryDataRef<'_, K, V> {}

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

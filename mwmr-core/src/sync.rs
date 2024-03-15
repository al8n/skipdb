use indexmap::IndexSet;
use smallvec_wrapper::MediumVec;

use super::{types::*, *};

/// Default hasher used by the conflict manager.
#[cfg(feature = "std")]
pub type DefaultHasher = std::hash::DefaultHasher;

/// The conflict manager that can be used to manage the conflicts in a transaction.
///
/// The conflict normally needs to have:
///
/// 1. Contains fingerprints of keys read.
/// 2. Contains fingerprints of keys written. This is used for conflict detection.
pub trait Cm: Sized + 'static {
  /// The error type returned by the pending manager.
  #[cfg(feature = "std")]
  type Error: std::error::Error + 'static;

  #[cfg(not(feature = "std"))]
  type Error: core::fmt::Display + 'static;

  /// The key type.
  type Key: 'static;

  /// The options type used to create the conflict manager.
  type Options: 'static;

  /// Create a new conflict manager with the given options.
  fn new(options: Self::Options) -> Result<Self, Self::Error>;

  /// Mark the key is read.
  fn mark_read(&mut self, key: &Self::Key) -> Result<(), Self::Error>;

  /// Mark the key is .
  fn mark_conflict(&mut self, key: &Self::Key) -> Result<(), Self::Error>;

  /// Returns true if we have a conflict.
  fn has_conflict(&self, other: &Self) -> bool;
}

/// A [`ComflictManager`] that based on the hash.
pub struct HashCM<K, S = DefaultHasher> {
  reads: MediumVec<u64>,
  conflict_keys: IndexSet<u64, S>,
  _k: core::marker::PhantomData<K>,
}

impl<K, S> Cm for HashCM<K, S>
where
  S: BuildHasher + 'static,
  K: core::hash::Hash + Eq + 'static,
{
  type Error = core::convert::Infallible;
  type Key = K;
  type Options = S;

  #[inline]
  fn new(options: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      reads: MediumVec::new(),
      conflict_keys: IndexSet::with_hasher(options),
      _k: core::marker::PhantomData,
    })
  }

  #[inline]
  fn mark_read(&mut self, key: &K) -> Result<(), Self::Error> {
    let fp = self.conflict_keys.hasher().hash_one(key);
    self.reads.push(fp);
    Ok(())
  }

  #[inline]
  fn mark_conflict(&mut self, key: &Self::Key) -> Result<(), Self::Error> {
    let fp = self.conflict_keys.hasher().hash_one(key);
    self.conflict_keys.insert(fp);
    Ok(())
  }

  #[inline]
  fn has_conflict(&self, other: &Self) -> bool {
    if self.reads.is_empty() {
      return false;
    }

    for ro in self.reads.iter() {
      if other.conflict_keys.contains(ro) {
        return true;
      }
    }
    false
  }
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
pub trait Pwm: Sized + 'static {
  /// The error type returned by the pending manager.
  type Error: std::error::Error + 'static;
  /// The key type.
  type Key: 'static;
  /// The value type.
  type Value: 'static;
  /// The options type used to create the pending manager.
  type Options: 'static;

  /// Create a new pending manager with the given options.
  fn new(options: Self::Options) -> Result<Self, Self::Error>;

  /// Returns true if the buffer is empty.
  fn is_empty(&self) -> bool;

  /// Returns the number of elements in the buffer.
  fn len(&self) -> usize;

  /// Validate if the entry is valid for this database.
  ///
  /// e.g.
  /// - If the entry is expired
  /// - If the key or the value is too large
  /// - If the key or the value is empty
  /// - If the key or the value contains invalid characters
  /// - and etc.
  fn validate_entry(&self, entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error>;

  /// Returns the maximum batch size in bytes
  fn max_batch_size(&self) -> u64;

  /// Returns the maximum entries in batch
  fn max_batch_entries(&self) -> u64;

  /// Returns the estimated size of the entry in bytes when persisted in the database.
  fn estimate_size(&self, entry: &Entry<Self::Key, Self::Value>) -> u64;

  /// Returns a reference to the value corresponding to the key.
  fn get(&self, key: &Self::Key) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>;

  /// Inserts a key-value pair into the buffer.
  fn insert(&mut self, key: Self::Key, value: EntryValue<Self::Value>) -> Result<(), Self::Error>;

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

/// A type alias for [`Pwm`] that based on the [`IndexMap`].
pub type IndexMapManager<K, V, S = std::hash::RandomState> = IndexMap<K, EntryValue<V>, S>;
/// A type alias for [`Pwm`] that based on the [`BTreeMap`].
pub type BTreeMapManager<K, V> = BTreeMap<K, EntryValue<V>>;

impl<K, V, S> Pwm for IndexMap<K, EntryValue<V>, S>
where
  K: Eq + core::hash::Hash + 'static,
  V: 'static,
  S: BuildHasher + Default + 'static,
{
  type Error = std::convert::Infallible;
  type Key = K;
  type Value = V;
  type Options = Option<S>;

  fn new(options: Self::Options) -> Result<Self, Self::Error> {
    Ok(match options {
      Some(hasher) => Self::with_hasher(hasher),
      None => Self::default(),
    })
  }

  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  fn len(&self) -> usize {
    self.len()
  }

  fn estimate_size(&self, _entry: &Entry<Self::Key, Self::Value>) -> u64 {
    todo!()
  }

  fn max_batch_entries(&self) -> u64 {
    todo!()
  }

  fn max_batch_size(&self) -> u64 {
    todo!()
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

  fn validate_entry(&self, _entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error> {
    Ok(())
  }
}

impl<K, V> Pwm for BTreeMap<K, EntryValue<V>>
where
  K: Eq + core::hash::Hash + Ord + 'static,
  V: 'static,
{
  type Error = std::convert::Infallible;
  type Key = K;
  type Value = V;

  type Options = ();

  fn new(_: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self::default())
  }

  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  fn len(&self) -> usize {
    self.len()
  }

  fn estimate_size(&self, _entry: &Entry<Self::Key, Self::Value>) -> u64 {
    todo!()
  }

  fn max_batch_entries(&self) -> u64 {
    todo!()
  }

  fn max_batch_size(&self) -> u64 {
    todo!()
  }

  fn validate_entry(&self, _entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error> {
    Ok(())
  }

  fn get(&self, key: &K) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
    Ok(self.get(key))
  }

  fn insert(&mut self, key: K, value: EntryValue<Self::Value>) -> Result<(), Self::Error> {
    self.insert(key, value);
    Ok(())
  }

  fn remove_entry(&mut self, key: &K) -> Result<Option<(K, EntryValue<Self::Value>)>, Self::Error> {
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

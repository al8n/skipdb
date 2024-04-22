use core::{borrow::Borrow, future::Future};

use super::types::*;

/// A marker used to mark the keys that are read.
pub struct AsyncMarker<'a, C> {
  marker: &'a mut C,
}

impl<'a, C> AsyncMarker<'a, C> {
  /// Returns a new marker.
  #[inline]
  pub fn new(marker: &'a mut C) -> Self {
    Self { marker }
  }
}

impl<'a, C: AsyncCm> AsyncMarker<'a, C> {
  /// Marks a key is operated.
  pub async fn mark(&mut self, k: &C::Key) {
    self.marker.mark_read(k).await;
  }
}

/// The conflict manager that can be used to manage the conflicts in a transaction.
///
/// The conflict normally needs to have:
///
/// 1. Contains fingerprints of keys read.
/// 2. Contains fingerprints of keys written. This is used for conflict detection.
pub trait AsyncCm: Sized {
  /// The error type returned by the conflict manager.
  type Error: crate::error::Error;

  /// The key type.
  type Key;

  /// The options type used to create the conflict manager.
  type Options;

  /// Create a new conflict manager with the given options.
  fn new(options: Self::Options) -> impl Future<Output = Result<Self, Self::Error>>;

  /// Mark the key is read.
  fn mark_read(&mut self, key: &Self::Key) -> impl Future<Output = ()>;

  /// Mark the key is .
  fn mark_conflict(&mut self, key: &Self::Key) -> impl Future<Output = ()>;

  /// Returns true if we have a conflict.
  fn has_conflict(&self, other: &Self) -> impl Future<Output = bool>;

  /// Rollback the conflict manager.
  fn rollback(&mut self) -> impl Future<Output = Result<(), Self::Error>>;
}

/// An optimized version of the [`AsyncCm`] trait that if your conflict manager is depend on hash.
pub trait AsyncCmEquivalent: AsyncCm {
  /// Optimized version of [`mark_read`] that accepts borrowed keys. Optional to implement.
  fn mark_read_equivalent<Q>(&mut self, key: &Q) -> impl Future<Output = ()>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized + Sync;

  /// Optimized version of [`mark_conflict`] that accepts borrowed keys. Optional to implement.
  fn mark_conflict_equivalent<Q>(&mut self, key: &Q) -> impl Future<Output = ()>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized + Sync;
}

/// An optimized version of the [`AsyncCm`] trait that if your conflict manager is depend on the order.
pub trait AsyncCmComparable: AsyncCm {
  /// Optimized version of [`mark_read`] that accepts borrowed keys. Optional to implement.
  fn mark_read_comparable<Q>(&mut self, key: &Q) -> impl Future<Output = ()>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized + Sync;

  /// Optimized version of [`mark_conflict`] that accepts borrowed keys. Optional to implement.
  fn mark_conflict_comparable<Q>(&mut self, key: &Q) -> impl Future<Output = ()>
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: Ord + ?Sized + Sync;
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
pub trait AsyncPwm: Sized {
  /// The error type returned by the conflict manager.
  type Error: crate::error::Error;
  /// The key type.
  type Key;
  /// The value type.
  type Value;
  /// The options type used to create the pending manager.
  type Options;
  /// The iterator type that borrows the pending writes.
  type Iter<'a>: Iterator<Item = (&'a Self::Key, &'a EntryValue<Self::Value>)>
  where
    Self: 'a;
  /// The iterator type that consumes the pending writes.
  type IntoIter: Iterator<Item = (Self::Key, EntryValue<Self::Value>)>;

  /// Create a new pending manager with the given options.
  fn new(options: Self::Options) -> impl Future<Output = Result<Self, Self::Error>>;

  /// Returns true if the buffer is empty.
  fn is_empty(&self) -> impl Future<Output = bool>;

  /// Returns the number of elements in the buffer.
  fn len(&self) -> impl Future<Output = usize>;

  /// Validate if the entry is valid for this database.
  ///
  /// e.g.
  /// - If the entry is expired
  /// - If the key or the value is too large
  /// - If the key or the value is empty
  /// - If the key or the value contains invalid characters
  /// - and etc.
  fn validate_entry(
    &self,
    entry: &Entry<Self::Key, Self::Value>,
  ) -> impl Future<Output = Result<(), Self::Error>>;

  /// Returns the maximum batch size in bytes
  fn max_batch_size(&self) -> u64;

  /// Returns the maximum entries in batch
  fn max_batch_entries(&self) -> u64;

  /// Returns the estimated size of the entry in bytes when persisted in the database.
  fn estimate_size(&self, entry: &Entry<Self::Key, Self::Value>) -> u64;

  /// Returns a reference to the value corresponding to the key.
  fn get(
    &self,
    key: &Self::Key,
  ) -> impl Future<Output = Result<Option<&EntryValue<Self::Value>>, Self::Error>>;

  /// Returns a reference to the key-value pair corresponding to the key.
  fn get_entry(
    &self,
    key: &Self::Key,
  ) -> impl Future<Output = Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>>;

  /// Returns true if the pending manager contains the key.
  fn contains_key(&self, key: &Self::Key) -> impl Future<Output = Result<bool, Self::Error>>;

  /// Inserts a key-value pair into the er.
  fn insert(
    &mut self,
    key: Self::Key,
    value: EntryValue<Self::Value>,
  ) -> impl Future<Output = Result<(), Self::Error>>;

  /// Removes a key from the pending writes, returning the key-value pair if the key was previously in the pending writes.
  fn remove_entry(
    &mut self,
    key: &Self::Key,
  ) -> impl Future<Output = Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>>;

  /// Rollback the pending writes.
  fn rollback(&mut self) -> impl Future<Output = Result<(), Self::Error>>;

  /// Returns an iterator over the pending writes.
  fn iter(
    &self,
  ) -> impl Future<Output = impl Iterator<Item = (&Self::Key, &EntryValue<Self::Value>)>>;

  /// Returns an iterator that consumes the pending writes.
  fn into_iter(
    self,
  ) -> impl Future<Output = impl Iterator<Item = (Self::Key, EntryValue<Self::Value>)>>;
}

/// An optimized version of the [`AsyncPwm`] trait that if your pending writes manager is depend on hash.
pub trait AsyncPwmEquivalent: AsyncPwm {
  /// Optimized version of [`AsyncPwm::get`] that accepts borrowed keys.
  fn get_equivalent<Q>(
    &self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<&EntryValue<Self::Value>>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized;

  /// Optimized version of [`AsyncPwm::get_entry`] that accepts borrowed keys.
  fn get_entry_equivalent<Q>(
    &self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized;

  /// Optimized version of [`AsyncPwm::contains_key`] that accepts borrowed keys.
  fn contains_key_equivalent<Q>(&self, key: &Q) -> impl Future<Output = Result<bool, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized;

  /// Optimized version of [`AsyncPwm::remove_entry`] that accepts borrowed keys.
  fn remove_entry_equivalent<Q>(
    &mut self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized;
}

/// An optimized version of the [`AsyncPwm`] trait that if your pending writes manager is depend on the order.
pub trait AsyncPwmComparable: AsyncPwm {
  /// Optimized version of [`AsyncPwm::get`] that accepts borrowed keys.
  fn get_comparable<Q>(
    &self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<&EntryValue<Self::Value>>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized;

  /// Optimized version of [`AsyncPwm::get_entry`] that accepts borrowed keys.
  fn get_entry_comparable<Q>(
    &self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized;

  /// Optimized version of [`AsyncPwm::contains_key`] that accepts borrowed keys.
  fn contains_key_comparable<Q>(&self, key: &Q) -> impl Future<Output = Result<bool, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized;

  /// Optimized version of [`AsyncPwm::remove_entry`] that accepts borrowed keys.
  fn remove_entry_comparable<Q>(
    &mut self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized;
}

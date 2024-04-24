use core::{borrow::Borrow, future::Future, hash::Hash, ops::RangeBounds};

use super::{
  sync::*,
  types::{Entry, EntryValue},
};

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

  /// Marks a key is conflicted.
  pub async fn mark_conflict(&mut self, k: &C::Key) {
    self.marker.mark_conflict(k).await;
  }
}

impl<'a, C: AsyncCmComparable> AsyncMarker<'a, C> {
  /// Marks a key is operated.
  pub async fn mark_comparable<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.marker.mark_read_comparable(k).await;
  }

  /// Marks a key is conflicted.
  pub async fn mark_conflict_comparable<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.marker.mark_conflict_comparable(k).await;
  }
}

impl<'a, C: AsyncCmEquivalent> AsyncMarker<'a, C> {
  /// Marks a key is operated.
  pub async fn mark_equivalent<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    self.marker.mark_read_equivalent(k).await;
  }

  /// Marks a key is conflicted.
  pub async fn mark_conflict_equivalent<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    self.marker.mark_conflict_equivalent(k).await;
  }
}

impl<'a, C: Cm> AsyncMarker<'a, C> {
  /// Marks a key is operated.
  pub fn mark_blocking(&mut self, k: &C::Key) {
    self.marker.mark_read(k);
  }

  /// Marks a key is conflicted.
  pub fn mark_conflict_blocking(&mut self, k: &C::Key) {
    self.marker.mark_conflict(k);
  }
}

impl<'a, C: CmComparable> AsyncMarker<'a, C> {
  /// Marks a key is operated.
  pub fn mark_comparable_blocking<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.marker.mark_read_comparable(k);
  }

  /// Marks a key is conflicted.
  pub fn mark_conflict_comparable_blocking<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.marker.mark_conflict_comparable(k);
  }
}

impl<'a, C: CmEquivalent> AsyncMarker<'a, C> {
  /// Marks a key is operated.
  pub fn mark_equivalent_blocking<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    self.marker.mark_read_equivalent(k);
  }

  /// Marks a key is conflicted.
  pub fn mark_conflict_equivalent_blocking<Q>(&mut self, k: &Q)
  where
    C::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    self.marker.mark_conflict_equivalent(k);
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
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized;

  /// Optimized version of [`mark_conflict`] that accepts borrowed keys. Optional to implement.
  fn mark_conflict_equivalent<Q>(&mut self, key: &Q) -> impl Future<Output = ()>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized;
}

/// An optimized version of the [`AsyncCm`] trait that if your conflict manager is depend on the order.
pub trait AsyncCmComparable: AsyncCm {
  /// Optimized version of [`mark_read`] that accepts borrowed keys. Optional to implement.
  fn mark_read_comparable<Q>(&mut self, key: &Q) -> impl Future<Output = ()>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized;

  /// Optimized version of [`mark_conflict`] that accepts borrowed keys. Optional to implement.
  fn mark_conflict_comparable<Q>(&mut self, key: &Q) -> impl Future<Output = ()>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized;
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
  fn iter(&self) -> impl Future<Output = Self::Iter<'_>>;

  /// Returns an iterator that consumes the pending writes.
  fn into_iter(self) -> impl Future<Output = Self::IntoIter>;
}

/// An trait that can be used to get a range over the pending writes.
pub trait AsyncPwmRange: AsyncPwm {
  /// The iterator type.
  type Range<'a>: IntoIterator<Item = (&'a Self::Key, &'a EntryValue<Self::Value>)>
  where
    Self: 'a;

  /// Returns an iterator over the pending writes.
  fn range<R: RangeBounds<Self::Key>>(&self, range: R) -> impl Future<Output = Self::Range<'_>>;
}

/// An trait that can be used to get a range over the pending writes.
pub trait AsyncPwmComparableRange: AsyncPwmRange + AsyncPwmComparable {
  /// Returns an iterator over the pending writes.
  fn range_comparable<T, R>(&self, range: R) -> impl Future<Output = Self::Range<'_>>
  where
    T: ?Sized + Ord,
    Self::Key: Borrow<T> + Ord,
    R: RangeBounds<T>;
}

/// An trait that can be used to get a range over the pending writes.
pub trait AsyncPwmEquivalentRange: AsyncPwmRange + AsyncPwmEquivalent {
  /// Returns an iterator over the pending writes.
  fn range_equivalent<T, R>(&self, range: R) -> impl Future<Output = Self::Range<'_>>
  where
    T: ?Sized + Eq + Hash,
    Self::Key: Borrow<T> + Eq + Hash,
    R: RangeBounds<T>;
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
    Q: Hash + Eq + ?Sized;

  /// Optimized version of [`AsyncPwm::get_entry`] that accepts borrowed keys.
  fn get_entry_equivalent<Q>(
    &self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized;

  /// Optimized version of [`AsyncPwm::contains_key`] that accepts borrowed keys.
  fn contains_key_equivalent<Q>(&self, key: &Q) -> impl Future<Output = Result<bool, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized;

  /// Optimized version of [`AsyncPwm::remove_entry`] that accepts borrowed keys.
  fn remove_entry_equivalent<Q>(
    &mut self,
    key: &Q,
  ) -> impl Future<Output = Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized;
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

impl<T> AsyncCm for T
where
  T: Cm,
{
  type Error = <T as Cm>::Error;

  type Key = <T as Cm>::Key;

  type Options = <T as Cm>::Options;

  async fn new(options: Self::Options) -> Result<Self, Self::Error> {
    <T as Cm>::new(options)
  }

  async fn mark_read(&mut self, key: &Self::Key) {
    <T as Cm>::mark_read(self, key)
  }

  async fn mark_conflict(&mut self, key: &Self::Key) {
    <T as Cm>::mark_conflict(self, key)
  }

  async fn has_conflict(&self, other: &Self) -> bool {
    <T as Cm>::has_conflict(self, other)
  }

  async fn rollback(&mut self) -> Result<(), Self::Error> {
    <T as Cm>::rollback(self)
  }
}

impl<T> AsyncCmComparable for T
where
  T: CmComparable,
{
  async fn mark_read_comparable<Q>(&mut self, key: &Q)
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as CmComparable>::mark_read_comparable(self, key)
  }

  async fn mark_conflict_comparable<Q>(&mut self, key: &Q)
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as CmComparable>::mark_conflict_comparable(self, key)
  }
}

impl<T> AsyncCmEquivalent for T
where
  T: CmEquivalent,
{
  async fn mark_read_equivalent<Q>(&mut self, key: &Q)
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    <T as CmEquivalent>::mark_read_equivalent(self, key)
  }

  async fn mark_conflict_equivalent<Q>(&mut self, key: &Q)
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    <T as CmEquivalent>::mark_conflict_equivalent(self, key)
  }
}

impl<T> AsyncPwm for T
where
  T: Pwm,
{
  type Error = <T as Pwm>::Error;

  type Key = <T as Pwm>::Key;

  type Value = <T as Pwm>::Value;

  type Options = <T as Pwm>::Options;

  type Iter<'a> = <T as Pwm>::Iter<'a> where Self: 'a;

  type IntoIter = <T as Pwm>::IntoIter;

  async fn new(options: Self::Options) -> Result<Self, Self::Error> {
    <T as Pwm>::new(options)
  }

  async fn is_empty(&self) -> bool {
    <T as Pwm>::is_empty(self)
  }

  async fn len(&self) -> usize {
    <T as Pwm>::len(self)
  }

  async fn validate_entry(&self, entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error> {
    <T as Pwm>::validate_entry(self, entry)
  }

  fn max_batch_size(&self) -> u64 {
    <T as Pwm>::max_batch_size(self)
  }

  fn max_batch_entries(&self) -> u64 {
    <T as Pwm>::max_batch_entries(self)
  }

  fn estimate_size(&self, entry: &Entry<Self::Key, Self::Value>) -> u64 {
    <T as Pwm>::estimate_size(self, entry)
  }

  async fn get(&self, key: &Self::Key) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
    <T as Pwm>::get(self, key)
  }

  async fn get_entry(
    &self,
    key: &Self::Key,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error> {
    <T as Pwm>::get_entry(self, key)
  }

  async fn contains_key(&self, key: &Self::Key) -> Result<bool, Self::Error> {
    <T as Pwm>::contains_key(self, key)
  }

  async fn insert(
    &mut self,
    key: Self::Key,
    value: EntryValue<Self::Value>,
  ) -> Result<(), Self::Error> {
    <T as Pwm>::insert(self, key, value)
  }

  async fn remove_entry(
    &mut self,
    key: &Self::Key,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error> {
    <T as Pwm>::remove_entry(self, key)
  }

  async fn rollback(&mut self) -> Result<(), Self::Error> {
    <T as Pwm>::rollback(self)
  }

  async fn iter(&self) -> Self::Iter<'_> {
    <T as Pwm>::iter(self)
  }

  async fn into_iter(self) -> Self::IntoIter {
    <T as Pwm>::into_iter(self)
  }
}

impl<T> AsyncPwmRange for T
where
  T: PwmRange,
{
  type Range<'a> = <T as PwmRange>::Range<'a> where Self: 'a;

  async fn range<R: RangeBounds<Self::Key>>(&self, range: R) -> Self::Range<'_> {
    <T as PwmRange>::range(self, range)
  }
}

impl<C> AsyncPwmComparableRange for C
where
  C: PwmComparableRange,
{
  async fn range_comparable<T, R>(&self, range: R) -> Self::Range<'_>
  where
    T: ?Sized + Ord,
    Self::Key: Borrow<T> + Ord,
    R: RangeBounds<T>,
  {
    <C as PwmComparableRange>::range_comparable(self, range)
  }
}

impl<C> AsyncPwmEquivalentRange for C
where
  C: PwmEquivalentRange,
{
  async fn range_equivalent<T, R>(&self, range: R) -> Self::Range<'_>
  where
    T: ?Sized + Eq + Hash,
    Self::Key: Borrow<T> + Eq + Hash,
    R: RangeBounds<T>,
  {
    <C as PwmEquivalentRange>::range_equivalent(self, range)
  }
}

impl<T> AsyncPwmComparable for T
where
  T: PwmComparable,
{
  async fn get_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as PwmComparable>::get_comparable(self, key)
  }

  async fn get_entry_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as PwmComparable>::get_entry_comparable(self, key)
  }

  async fn contains_key_comparable<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as PwmComparable>::contains_key_comparable(self, key)
  }

  async fn remove_entry_comparable<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    <T as PwmComparable>::remove_entry_comparable(self, key)
  }
}

impl<T> AsyncPwmEquivalent for T
where
  T: PwmEquivalent,
{
  async fn get_equivalent<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    <T as PwmEquivalent>::get_equivalent(self, key)
  }

  async fn get_entry_equivalent<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    <T as PwmEquivalent>::get_entry_equivalent(self, key)
  }

  async fn contains_key_equivalent<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    <T as PwmEquivalent>::contains_key_equivalent(self, key)
  }

  async fn remove_entry_equivalent<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    <T as PwmEquivalent>::remove_entry_equivalent(self, key)
  }
}

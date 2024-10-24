use super::*;

use core::convert::Infallible;

use alloc::collections::{
  btree_map::{IntoIter as BTreeMapIntoIter, Iter as BTreeMapIter, Range as BTreeMapRange},
  BTreeMap,
};

/// A type alias for [`Pwm`] that based on the [`BTreeMap`].
pub type BTreePwm<K, V> = BTreeMap<K, EntryValue<V>>;

impl<K, V> Pwm for BTreeMap<K, EntryValue<V>>
where
  K: Ord,
{
  type Error = Infallible;
  type Key = K;
  type Value = V;

  type Iter<'a>
    = BTreeMapIter<'a, K, EntryValue<V>>
  where
    Self: 'a;

  type IntoIter = BTreeMapIntoIter<K, EntryValue<V>>;

  type Options = ();

  #[inline]
  fn new(_: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self::default())
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  #[inline]
  fn len(&self) -> usize {
    self.len()
  }

  #[inline]
  fn validate_entry(&self, _entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[inline]
  fn max_batch_size(&self) -> u64 {
    u64::MAX
  }

  #[inline]
  fn max_batch_entries(&self) -> u64 {
    u64::MAX
  }

  #[inline]
  fn estimate_size(&self, _entry: &Entry<Self::Key, Self::Value>) -> u64 {
    core::mem::size_of::<Self::Key>() as u64 + core::mem::size_of::<Self::Value>() as u64
  }

  #[inline]
  fn get(&self, key: &K) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
    Ok(self.get(key))
  }

  #[inline]
  fn get_entry(
    &self,
    key: &Self::Key,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error> {
    Ok(self.get_key_value(key))
  }

  #[inline]
  fn contains_key(&self, key: &K) -> Result<bool, Self::Error> {
    Ok(self.contains_key(key))
  }

  #[inline]
  fn insert(&mut self, key: K, value: EntryValue<Self::Value>) -> Result<(), Self::Error> {
    self.insert(key, value);
    Ok(())
  }

  #[inline]
  fn remove_entry(&mut self, key: &K) -> Result<Option<(K, EntryValue<Self::Value>)>, Self::Error> {
    Ok(self.remove_entry(key))
  }

  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    BTreeMap::iter(self)
  }

  #[inline]
  fn into_iter(self) -> Self::IntoIter {
    core::iter::IntoIterator::into_iter(self)
  }

  #[inline]
  fn rollback(&mut self) -> Result<(), Self::Error> {
    self.clear();
    Ok(())
  }
}

impl<K, V> PwmRange for BTreeMap<K, EntryValue<V>>
where
  K: Ord,
{
  type Range<'a>
    = BTreeMapRange<'a, K, EntryValue<V>>
  where
    Self: 'a;

  #[inline]
  fn range<R: RangeBounds<Self::Key>>(&self, range: R) -> Self::Range<'_> {
    BTreeMap::range(self, range)
  }
}

impl<K, V> PwmComparableRange for BTreeMap<K, EntryValue<V>>
where
  K: Ord,
{
  #[inline]
  fn range_comparable<T, R>(&self, range: R) -> Self::Range<'_>
  where
    T: ?Sized + Ord,
    Self::Key: Borrow<T> + Ord,
    R: RangeBounds<T>,
  {
    BTreeMap::range(self, range)
  }
}

impl<K, V> PwmComparable for BTreeMap<K, EntryValue<V>>
where
  K: Ord,
{
  #[inline]
  fn get_comparable<Q>(&self, key: &Q) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(BTreeMap::get(self, key))
  }

  #[inline]
  fn get_entry_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(BTreeMap::get_key_value(self, key))
  }

  #[inline]
  fn contains_key_comparable<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(BTreeMap::contains_key(self, key))
  }

  #[inline]
  fn remove_entry_comparable<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(K, EntryValue<V>)>, Self::Error>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(BTreeMap::remove_entry(self, key))
  }
}

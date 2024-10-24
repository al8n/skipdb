use crate::DefaultHasher;

use super::*;
use core::{convert::Infallible, hash::BuildHasher};
use indexmap::IndexMap;

/// A type alias for [`Pwm`] that based on the [`IndexMap`].
pub type IndexMapPwm<K, V, S = DefaultHasher> = IndexMap<K, EntryValue<V>, S>;

impl<K, V, S> Pwm for IndexMap<K, EntryValue<V>, S>
where
  K: Eq + Hash,
  S: BuildHasher + Default,
{
  type Error = Infallible;
  type Key = K;
  type Value = V;
  type Iter<'a>
    = indexmap::map::Iter<'a, K, EntryValue<V>>
  where
    Self: 'a;
  type IntoIter = indexmap::map::IntoIter<K, EntryValue<V>>;

  type Options = Option<S>;

  #[inline]
  fn new(options: Self::Options) -> Result<Self, Self::Error> {
    Ok(match options {
      Some(hasher) => Self::with_hasher(hasher),
      None => Self::default(),
    })
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
  fn get(&self, key: &K) -> Result<Option<&EntryValue<V>>, Self::Error> {
    Ok(self.get(key))
  }

  #[inline]
  fn get_entry(
    &self,
    key: &Self::Key,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error> {
    Ok(self.get_full(key).map(|(_, k, v)| (k, v)))
  }

  #[inline]
  fn contains_key(&self, key: &K) -> Result<bool, Self::Error> {
    Ok(self.contains_key(key))
  }

  #[inline]
  fn insert(&mut self, key: K, value: EntryValue<V>) -> Result<(), Self::Error> {
    self.insert(key, value);
    Ok(())
  }

  #[inline]
  fn remove_entry(&mut self, key: &K) -> Result<Option<(K, EntryValue<V>)>, Self::Error> {
    Ok(self.shift_remove_entry(key))
  }

  #[inline]
  fn iter(&self) -> Self::Iter<'_> {
    IndexMap::iter(self)
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

impl<K, V, S> PwmEquivalent for IndexMap<K, EntryValue<V>, S>
where
  K: Eq + Hash,
  S: BuildHasher + Default,
{
  #[inline]
  fn get_equivalent<Q>(&self, key: &Q) -> Result<Option<&EntryValue<V>>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    Ok(self.get(key))
  }

  #[inline]
  fn get_entry_equivalent<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    Ok(self.get_full(key).map(|(_, k, v)| (k, v)))
  }

  #[inline]
  fn contains_key_equivalent<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    Ok(self.contains_key(key))
  }

  #[inline]
  fn remove_entry_equivalent<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(K, EntryValue<V>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    Ok(self.shift_remove_entry(key))
  }
}

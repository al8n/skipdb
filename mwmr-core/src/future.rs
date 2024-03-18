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
pub trait AsyncPwm: Send + Sync + 'static {
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

/// A type alias for [`Pwm`] that based on the [`IndexMap`].
pub type AsyncIndexMapManager<K, V, S = std::hash::RandomState> = IndexMap<K, EntryValue<V>, S>;
/// A type alias for [`Pwm`] that based on the [`BTreeMap`].
pub type AsyncBTreeMapManager<K, V> = BTreeMap<K, EntryValue<V>>;

impl<K, V, S> AsyncPwm for IndexMap<K, EntryValue<V>, S>
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

impl<K, V> AsyncPwm for BTreeMap<K, EntryValue<V>>
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

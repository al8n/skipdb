use super::*;
use std::{collections::hash_map::RandomState, hash::Hash};

mod write;
pub use write::*;

#[cfg(test)]
mod tests;

struct Inner<K, V, S = RandomState> {
  tm: Tm<K, V, HashCm<K, S>, BTreePwm<K, V>>,
  map: SkipCore<K, V>,
  hasher: S,
}

impl<K, V, S> Inner<K, V, S> {
  fn new(name: &str, hasher: S) -> Self {
    let tm = Tm::new(name, 0);
    Self {
      tm,
      map: SkipCore::new(),
      hasher,
    }
  }

  fn version(&self) -> u64 {
    self.tm.version()
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
///
/// `EquivalentDb` requires key to be [`Ord`] and [`Hash`](Hash).
///
/// Comparing to [`ComparableDb`](crate::comparable::ComparableDb),
/// `EquivalentDb` has more flexible write transaction APIs and no clone happen.
/// But, [`ComparableDb`](crate::comparable::ComparableDb) does not require the key to implement [`Hash`](Hash).
pub struct EquivalentDb<K, V, S = RandomState> {
  inner: Arc<Inner<K, V, S>>,
}

#[doc(hidden)]
impl<K, V, S> AsSkipCore<K, V> for EquivalentDb<K, V, S> {
  #[inline]
  #[allow(private_interfaces)]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V, S> Clone for EquivalentDb<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for EquivalentDb<K, V> {
  /// Creates a new `EquivalentDb` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> EquivalentDb<K, V> {
  /// Creates a new `EquivalentDb` with the given options.
  #[inline]
  pub fn new() -> Self {
    Self::with_hasher(Default::default())
  }
}

impl<K, V, S> EquivalentDb<K, V, S> {
  /// Creates a new `EquivalentDb` with the given hasher.
  #[inline]
  pub fn with_hasher(hasher: S) -> Self {
    let inner = Arc::new(Inner::new(core::any::type_name::<Self>(), hasher));
    Self { inner }
  }

  /// Returns the current read version of the database.
  #[inline]
  pub fn version(&self) -> u64 {
    self.inner.version()
  }

  /// Create a read transaction.
  #[inline]
  pub fn read(&self) -> ReadTransaction<K, V, EquivalentDb<K, V, S>, HashCm<K, S>> {
    ReadTransaction::new(self.clone(), self.inner.tm.read())
  }
}

impl<K, V, S> EquivalentDb<K, V, S>
where
  K: Ord + Eq + Hash,
  V: 'static,
  S: BuildHasher + Clone,
{
  /// Create a write transaction.
  #[inline]
  pub fn write(&self) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone(), None)
  }

  /// Create a write transaction with the given capacity hint.
  #[inline]
  pub fn write_with_capacity(&self, capacity: usize) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone(), Some(capacity))
  }
}

impl<K, V, S> EquivalentDb<K, V, S>
where
  K: Ord + Eq + Hash + Send + 'static,
  V: Send + 'static,
  S: BuildHasher + Clone,
{
  /// Compact the database.
  #[inline]
  pub fn compact(&self) {
    self.inner.map.compact(self.inner.tm.discard_hint());
  }
}

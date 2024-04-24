pub use cheap_clone::CheapClone;
use txn::BTreeCm;

use super::*;

mod write;
pub use write::*;

#[cfg(test)]
mod tests;

struct Inner<K, V> {
  tm: Tm<K, V, BTreeCm<K>, BTreePwm<K, V>>,
  map: SkipCore<K, V>,
}

impl<K, V> Inner<K, V> {
  fn new(name: &str) -> Self {
    let tm = Tm::new(name, 0);
    Self {
      tm,
      map: SkipCore::new(),
    }
  }

  fn version(&self) -> u64 {
    self.tm.version()
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
///
/// `ComparableDb` requires key to be [`Ord`] and [`CheapClone`].
/// The [`CheapClone`] bound here hints the user that the key should be cheap to clone,
/// because it will be cloned at least one time during the write transaction.
///
/// Comparing to [`EquivalentDb`](crate::equivalent::EquivalentDb), `ComparableDb` does not require key to implement [`Hash`](core::hash::Hash).
/// But, [`EquivalentDb`](crate::equivalent::EquivalentDb) has more flexible write transaction APIs.
pub struct ComparableDb<K, V> {
  inner: Arc<Inner<K, V>>,
}

#[doc(hidden)]
impl<K, V> AsSkipCore<K, V> for ComparableDb<K, V> {
  #[inline]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V> Clone for ComparableDb<K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for ComparableDb<K, V> {
  /// Creates a new `ComparableDb` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> ComparableDb<K, V> {
  /// Creates a new `ComparableDb`
  #[inline]
  pub fn new() -> Self {
    Self {
      inner: Arc::new(Inner::new(core::any::type_name::<Self>())),
    }
  }
}

impl<K, V> ComparableDb<K, V> {
  /// Returns the current read version of the database.
  #[inline]
  pub fn version(&self) -> u64 {
    self.inner.version()
  }

  /// Create a read transaction.
  #[inline]
  pub fn read(&self) -> ReadTransaction<K, V, ComparableDb<K, V>, BTreeCm<K>> {
    ReadTransaction::new(self.clone(), self.inner.tm.read())
  }
}

impl<K, V> ComparableDb<K, V>
where
  K: CheapClone + Ord + 'static,
  V: 'static,
{
  /// Create a write transaction.
  #[inline]
  pub fn write(&self) -> WriteTransaction<K, V> {
    WriteTransaction::new(self.clone())
  }
}

impl<K, V> ComparableDb<K, V>
where
  K: CheapClone + Ord + Send + 'static,
  V: Send + 'static,
{
  /// Compact the database.
  #[inline]
  pub fn compact(&self) {
    self.inner.map.compact(self.inner.tm.discard_hint());
  }
}

pub use cheap_clone::CheapClone;
use txn::BTreeCm;

use super::*;

mod optimistic;
pub use optimistic::*;

#[allow(clippy::module_inception)]
mod serializable;
pub use serializable::*;

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

/// A concurrent MVCC in-memory key-value database.
///
/// `SerializableDb` requires key to be [`Ord`] and [`CheapClone`].
/// The [`CheapClone`] bound here hints the user that the key should be cheap to clone,
/// because it will be cloned at least one time during the write transaction.
///
/// Comparing to [`OptimisticDb`](crate::optimistic::OptimisticDb):
/// 1. `SerializableDb` support full serializable snapshot isolation, which can detect both direct dependencies and indirect dependencies.
/// 2. `SerializableDb` does not require key to implement [`Hash`](core::hash::Hash).
/// 3. But, [`OptimisticDb`](crate::optimistic::OptimisticDb) has more flexible write transaction APIs.
#[repr(transparent)]
pub struct SerializableDb<K, V> {
  inner: Arc<Inner<K, V>>,
}

#[doc(hidden)]
impl<K, V> AsSkipCore<K, V> for SerializableDb<K, V> {
  #[inline]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V> Clone for SerializableDb<K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for SerializableDb<K, V> {
  /// Creates a new `SerializableDb` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> SerializableDb<K, V> {
  /// Creates a new `SerializableDb`
  #[inline]
  pub fn new() -> Self {
    Self {
      inner: Arc::new(Inner::new(core::any::type_name::<Self>())),
    }
  }
}

impl<K, V> SerializableDb<K, V> {
  /// Returns the current read version of the database.
  #[inline]
  pub fn version(&self) -> u64 {
    self.inner.version()
  }

  /// Create a read transaction.
  #[inline]
  pub fn read(&self) -> ReadTransaction<K, V, SerializableDb<K, V>, BTreeCm<K>> {
    ReadTransaction::new(self.clone(), self.inner.tm.read())
  }
}

impl<K, V> SerializableDb<K, V>
where
  K: CheapClone + Ord + 'static,
  V: 'static,
{
  /// Create a optimistic write transaction.
  ///
  /// Optimistic write transaction is not a totally Serializable Snapshot Isolation transaction.
  /// It can handle most of write skew anomaly, but not all. Basically, all directly dependencies
  /// can be handled, but indirect dependencies (logical dependencies) can not be handled.
  /// If you need a totally Serializable Snapshot Isolation transaction, you should use
  /// [`SerializableDb::serializable_write`](SerializableDb::serializable_write) instead.
  #[inline]
  pub fn optimistic_write(&self) -> OptimisticTransaction<K, V> {
    OptimisticTransaction::new(self.clone())
  }

  /// Create a serializable write transaction.
  ///
  /// Serializable write transaction is a totally Serializable Snapshot Isolation transaction.
  /// It can handle all kinds of write skew anomaly, including indirect dependencies (logical dependencies).
  /// If in your code, you do not care about indirect dependencies (logical dependencies), you can use
  /// [`SerializableDb::optimistic_write`](SerializableDb::optimistic_write) instead.
  #[inline]
  pub fn serializable_write(&self) -> SerializableTransaction<K, V> {
    SerializableTransaction::new(self.clone())
  }
}

impl<K, V> SerializableDb<K, V>
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

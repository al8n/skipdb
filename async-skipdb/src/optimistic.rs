use skipdb_core::types::Values;
use std::{collections::hash_map::RandomState, hash::Hash};

use super::*;

mod write;
pub use write::*;

#[cfg(all(test, any(feature = "tokio", feature = "smol", feature = "async-std")))]
mod tests;

/// Database for [`smol`](https://crates.io/crates/smol) runtime.
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub type SmolOptimisticDb<K, V, S = RandomState> = OptimisticDb<K, V, SmolSpawner, S>;

/// Database for [`tokio`](https://crates.io/crates/tokio) runtime.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub type TokioOptimisticDb<K, V, S = RandomState> = OptimisticDb<K, V, TokioSpawner, S>;

/// Database for [`async-std`](https://crates.io/crates/async-std) runtime.
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub type AsyncStdOptimisticDb<K, V, S = RandomState> = OptimisticDb<K, V, AsyncStdSpawner, S>;

struct Inner<K, V, SP, S = RandomState>
where
  SP: AsyncSpawner,
{
  tm: AsyncTm<K, V, HashCm<K, S>, BTreePwm<K, V>, SP>,
  map: SkipCore<K, V>,
  hasher: S,
}

impl<K, V, SP: AsyncSpawner, S> Inner<K, V, SP, S> {
  async fn new(name: &str, hasher: S) -> Self {
    let tm = AsyncTm::<_, _, _, _, SP>::new(name, 0).await;
    Self {
      tm,
      map: SkipCore::new(),
      hasher,
    }
  }

  async fn version(&self) -> u64 {
    self.tm.version().await
  }
}

/// A concurrent MVCC in-memory key-value database.
///
/// `OptimisticDb` requires key to be [`Ord`] and [`Hash`](Hash).
///
/// Comparing to [`SerializableDb`](crate::serializable::SerializableDb):
/// 1. `SerializableDb` support full serializable snapshot isolation, which can detect both direct dependencies and indirect dependencies.
/// 2. `SerializableDb` does not require key to implement [`Hash`](core::hash::Hash).
/// 3. But, [`OptimisticDb`](crate::optimistic::OptimisticDb) has more flexible write transaction APIs and no clone happen.
pub struct OptimisticDb<K, V, SP: AsyncSpawner, S = RandomState> {
  inner: Arc<Inner<K, V, SP, S>>,
}

#[doc(hidden)]
impl<K, V, SP, S> AsSkipCore<K, V> for OptimisticDb<K, V, SP, S>
where
  SP: AsyncSpawner,
{
  #[inline]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V, SP, S> Clone for OptimisticDb<K, V, SP, S>
where
  SP: AsyncSpawner,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V, SP: AsyncSpawner> OptimisticDb<K, V, SP> {
  /// Creates a new `OptimisticDb` with the given options.
  #[inline]
  pub async fn new() -> Self {
    Self::with_hasher(Default::default()).await
  }
}

impl<K, V, SP: AsyncSpawner, S> OptimisticDb<K, V, SP, S> {
  /// Creates a new `OptimisticDb` with the given hasher.
  #[inline]
  pub async fn with_hasher(hasher: S) -> Self {
    let inner = Arc::new(Inner::<_, _, SP, _>::new(core::any::type_name::<Self>(), hasher).await);
    Self { inner }
  }

  /// Returns the current read version of the database.
  #[inline]
  pub async fn version(&self) -> u64 {
    self.inner.version().await
  }

  /// Create a read transaction.
  #[inline]
  pub async fn read(&self) -> ReadTransaction<K, V, OptimisticDb<K, V, SP, S>, HashCm<K, S>, SP> {
    ReadTransaction::new(self.clone(), self.inner.tm.read().await)
  }
}

impl<K, V, SP, S> OptimisticDb<K, V, SP, S>
where
  K: Ord + Hash + Eq,
  S: BuildHasher + Clone,
  SP: AsyncSpawner,
{
  /// Create a write transaction.
  #[inline]
  pub async fn write(&self) -> OptimisticTransaction<K, V, SP, S> {
    OptimisticTransaction::new(self.clone(), None).await
  }

  /// Create a write transaction with the given capacity hint.
  #[inline]
  pub async fn write_with_capacity(&self, capacity: usize) -> OptimisticTransaction<K, V, SP, S> {
    OptimisticTransaction::new(self.clone(), Some(capacity)).await
  }
}

impl<K, V, SP, S> OptimisticDb<K, V, SP, S>
where
  K: Ord + Eq + Hash + Send + 'static,
  V: Send + 'static,
  Values<V>: Send,
  SP: AsyncSpawner,
{
  /// Compact the database.
  #[inline]
  pub fn compact(&self) {
    self.inner.map.compact(self.inner.tm.discard_hint());
  }
}

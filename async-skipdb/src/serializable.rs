use async_txn::{AsyncSpawner, BTreeCm};
pub use cheap_clone::CheapClone;
use skipdb_core::types::Values;

use super::*;

mod optimistic;
pub use optimistic::*;

#[allow(clippy::module_inception)]
mod serializable;
pub use serializable::*;

/// Database for [`smol`](https://crates.io/crates/smol) runtime.
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub type SmolSerializableDb<K, V> = SerializableDb<K, V, SmolSpawner>;

/// Database for [`tokio`](https://crates.io/crates/tokio) runtime.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub type TokioSerializableDb<K, V> = SerializableDb<K, V, TokioSpawner>;

/// Database for [`async-std`](https://crates.io/crates/async-std) runtime.
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub type AsyncStdSerializableDb<K, V> = SerializableDb<K, V, AsyncStdSpawner>;

struct Inner<K, V, S>
where
  S: AsyncSpawner,
{
  tm: AsyncTm<K, V, BTreeCm<K>, BTreePwm<K, V>, S>,
  map: SkipCore<K, V>,
}

impl<K, V, S: AsyncSpawner> Inner<K, V, S> {
  async fn new(name: &str) -> Self {
    let tm = AsyncTm::new(name, 0).await;
    Self {
      tm,
      map: SkipCore::new(),
    }
  }

  async fn version(&self) -> u64 {
    self.tm.version().await
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
pub struct SerializableDb<K, V, S: AsyncSpawner> {
  inner: Arc<Inner<K, V, S>>,
}

#[doc(hidden)]
impl<K, V, S: AsyncSpawner> AsSkipCore<K, V> for SerializableDb<K, V, S> {
  #[inline]
  #[allow(private_interfaces)]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V, S: AsyncSpawner> Clone for SerializableDb<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V, S: AsyncSpawner> SerializableDb<K, V, S> {
  /// Creates a new `SerializableDb`.
  #[inline]
  pub async fn new() -> Self {
    Self {
      inner: Arc::new(Inner::new(core::any::type_name::<Self>()).await),
    }
  }
}

impl<K, V, S: AsyncSpawner> SerializableDb<K, V, S> {
  /// Returns the current read version of the database.
  #[inline]
  pub async fn version(&self) -> u64 {
    self.inner.version().await
  }

  /// Create a read transaction.
  #[inline]
  pub async fn read(&self) -> ReadTransaction<K, V, SerializableDb<K, V, S>, BTreeCm<K>, S> {
    ReadTransaction::new(self.clone(), self.inner.tm.read().await)
  }
}

impl<K, V, S> SerializableDb<K, V, S>
where
  K: CheapClone + Ord,
  S: AsyncSpawner,
{
  /// Create a optimistic write transaction.
  ///
  /// Optimistic write transaction is not a totally Serializable Snapshot Isolation transaction.
  /// It can handle most of write skew anomaly, but not all. Basically, all directly dependencies
  /// can be handled, but indirect dependencies (logical dependencies) can not be handled.
  /// If you need a totally Serializable Snapshot Isolation transaction, you should use
  /// [`SerializableDb::serializable_write`](SerializableDb::serializable_write) instead.
  #[inline]
  pub async fn optimistic_write(&self) -> OptimisticTransaction<K, V, S> {
    OptimisticTransaction::new(self.clone()).await
  }

  /// Create a serializable write transaction.
  ///
  /// Serializable write transaction is a totally Serializable Snapshot Isolation transaction.
  /// It can handle all kinds of write skew anomaly, including indirect dependencies (logical dependencies).
  /// If in your code, you do not care about indirect dependencies (logical dependencies), you can use
  /// [`SerializableDb::optimistic_write`](SerializableDb::optimistic_write) instead.
  #[inline]
  pub async fn serializable_write(&self) -> SerializableTransaction<K, V, S> {
    SerializableTransaction::new(self.clone()).await
  }
}

impl<K, V, S> SerializableDb<K, V, S>
where
  K: CheapClone + Ord + Send + 'static,
  V: Send + 'static,
  Values<V>: Send,
  S: AsyncSpawner,
{
  /// Compact the database.
  #[inline]
  pub fn compact(&self) {
    self.inner.map.compact(self.inner.tm.discard_hint());
  }
}

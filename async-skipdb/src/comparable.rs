use async_txn::{AsyncSpawner, BTreeCm};
pub use cheap_clone::CheapClone;

use super::*;

mod write;
pub use write::*;

#[cfg(all(test, any(feature = "tokio", feature = "smol", feature = "async-std")))]
mod tests;

/// Database for [`smol`](https://crates.io/crates/smol) runtime.
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub type SmolComparableDb<K, V> = ComparableDb<K, V, SmolSpawner>;

/// Database for [`tokio`](https://crates.io/crates/tokio) runtime.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub type TokioComparableDb<K, V> = ComparableDb<K, V, TokioSpawner>;

/// Database for [`async-std`](https://crates.io/crates/async-std) runtime.
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub type AsyncStdComparableDb<K, V> = ComparableDb<K, V, AsyncStdSpawner>;

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

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
///
/// `ComparableDb` requires key to be [`Ord`] and [`CheapClone`].
/// The [`CheapClone`] bound here hints the user that the key should be cheap to clone,
/// because it will be cloned at least one time during the write transaction.
///
/// Comparing to [`EquivalentDb`](crate::equivalent::EquivalentDb), `ComparableDb` does not require key to implement [`Hash`](core::hash::Hash).
/// But, [`EquivalentDb`](crate::equivalent::EquivalentDb) has more flexible write transaction APIs.
pub struct ComparableDb<K, V, S: AsyncSpawner> {
  inner: Arc<Inner<K, V, S>>,
}

#[doc(hidden)]
impl<K, V, S: AsyncSpawner> AsSkipCore<K, V> for ComparableDb<K, V, S> {
  #[inline]
  #[allow(private_interfaces)]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V, S: AsyncSpawner> Clone for ComparableDb<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V, S: AsyncSpawner> ComparableDb<K, V, S> {
  /// Creates a new `ComparableDb`.
  #[inline]
  pub async fn new() -> Self {
    Self {
      inner: Arc::new(Inner::new(core::any::type_name::<Self>()).await),
    }
  }
}

impl<K, V, S: AsyncSpawner> ComparableDb<K, V, S> {
  /// Returns the current read version of the database.
  #[inline]
  pub async fn version(&self) -> u64 {
    self.inner.version().await
  }

  /// Create a read transaction.
  #[inline]
  pub async fn read(&self) -> ReadTransaction<K, V, ComparableDb<K, V, S>, BTreeCm<K>, S> {
    ReadTransaction::new(self.clone(), self.inner.tm.read().await)
  }
}

impl<K, V, S> ComparableDb<K, V, S>
where
  K: CheapClone + Ord,
  S: AsyncSpawner,
{
  /// Create a write transaction.
  #[inline]
  pub async fn write(&self) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone()).await
  }
}

impl<K, V, S> ComparableDb<K, V, S>
where
  K: CheapClone + Ord + Send + 'static,
  V: Send + 'static,
  S: AsyncSpawner,
{
  /// Compact the database.
  #[inline]
  pub fn compact(&self) {
    self.inner.map.compact(self.inner.tm.discard_hint());
  }
}

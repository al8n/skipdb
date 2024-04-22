use async_txn::{AsyncSpawner, BTreeCm};
pub use cheap_clone::CheapClone;

use super::*;

mod write;
pub use write::*;

#[cfg(all(test, any(feature = "tokio", feature = "smol", feature = "async-std")))]
mod tests;

struct Inner<K, V, S>
where
  S: AsyncSpawner,
{
  tm: AsyncTm<K, V, BTreeCm<K>, PendingMap<K, V>, S>,
  map: SkipCore<K, V>,
  max_batch_size: u64,
  max_batch_entries: u64,
}

impl<K, V, S: AsyncSpawner> Inner<K, V, S> {
  async fn new(name: &str, max_batch_size: u64, max_batch_entries: u64) -> Self {
    let tm = AsyncTm::new(name, 0).await;
    Self {
      tm,
      map: SkipCore::new(),
      max_batch_size,
      max_batch_entries,
    }
  }

  async fn version(&self) -> u64 {
    self.tm.version().await
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
///
/// `ComparableDB` requires key to be [`Ord`] and [`CheapClone`].
/// The [`CheapClone`] bound here hints the user that the key should be cheap to clone,
/// because it will be cloned at least one time during the write transaction.
///
/// Comparing to [`EquivalentDB`](crate::equivalent::EquivalentDB), `ComparableDB` does not require key to implement [`Hash`](core::hash::Hash).
/// But, [`EquivalentDB`](crate::equivalent::EquivalentDB) has more flexible write transaction APIs.
pub struct ComparableDB<K, V, S: AsyncSpawner> {
  inner: Arc<Inner<K, V, S>>,
}

impl<K, V, S: AsyncSpawner> AsSkipCore<K, V> for ComparableDB<K, V, S> {
  #[inline]
  #[allow(private_interfaces)]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V, S: AsyncSpawner> Clone for ComparableDB<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V, S: AsyncSpawner> ComparableDB<K, V, S> {
  /// Creates a new `ComparableDB` with the given options.
  #[inline]
  pub async fn new() -> Self {
    Self::with_options(Default::default()).await
  }
}

impl<K, V, S: AsyncSpawner> ComparableDB<K, V, S> {
  /// Creates a new `ComparableDB` with the given options.
  #[inline]
  pub async fn with_options(opts: Options) -> Self {
    Self {
      inner: Arc::new(
        Inner::new(
          core::any::type_name::<Self>(),
          opts.max_batch_size(),
          opts.max_batch_entries(),
        )
        .await,
      ),
    }
  }

  /// Returns the current read version of the database.
  #[inline]
  pub async fn version(&self) -> u64 {
    self.inner.version().await
  }

  /// Create a read transaction.
  #[inline]
  pub async fn read(&self) -> ReadTransaction<K, V, ComparableDB<K, V, S>, BTreeCm<K>, S> {
    ReadTransaction::new(self.clone(), self.inner.tm.read().await)
  }
}

impl<K, V, S> ComparableDB<K, V, S>
where
  K: CheapClone + Ord,
  S: AsyncSpawner,
{
  /// Create a write transaction.
  #[inline]
  pub async fn write(&self) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone(), None).await
  }

  /// Create a write transaction with the given capacity hint.
  #[inline]
  pub async fn write_with_capacity(&self, cap: usize) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone(), Some(cap)).await
  }
}

impl<K, V, S> ComparableDB<K, V, S>
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

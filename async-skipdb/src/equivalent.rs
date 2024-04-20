use super::*;

mod write;
pub use write::*;

#[cfg(test)]
mod tests;

struct Inner<K, V, SP, S = std::hash::RandomState> {
  tm: AsyncTm<K, V, HashCm<K, S>, PendingMap<K, V>, SP>,
  map: SkipCore<K, V>,
  hasher: S,
  max_batch_size: u64,
  max_batch_entries: u64,
}

impl<K, V, SP: AsyncSpawner, S> Inner<K, V, SP, S> {
  async fn new(name: &str, max_batch_size: u64, max_batch_entries: u64, hasher: S) -> Self {
    let tm = AsyncTm::<_, _, _, _, SP>::new(name, 0).await;
    Self {
      tm,
      map: SkipCore::new(),
      hasher,
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
/// `EquivalentDB` requires key to be [`Ord`] and [`Hash`](core::hash::Hash).
///
/// Comparing to [`ComparableDB`](crate::comparable::ComparableDB),
/// `EquivalentDB` has more flexible write transaction APIs and no clone happen.
/// But, [`ComparableDB`](crate::comparable::ComparableDB) does not require the key to implement [`Hash`](core::hash::Hash).
pub struct EquivalentDB<K, V, SP, S = std::hash::RandomState> {
  inner: Arc<Inner<K, V, SP, S>>,
}

impl<K, V, S> AsSkipCore<K, V> for EquivalentDB<K, V, S> {
  #[inline]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V, SP, S> Clone for EquivalentDB<K, V, SP, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V, SP: AsyncSpawner> EquivalentDB<K, V, SP> {
  /// Creates a new `EquivalentDB` with the given options.
  #[inline]
  pub async fn new() -> Self {
    Self::with_options_and_hasher(Default::default(), Default::default()).await
  }
}

impl<K, V, SP: AsyncSpawner, S> EquivalentDB<K, V, SP, S> {
  /// Creates a new `EquivalentDB` with the given hasher.
  #[inline]
  pub async fn with_hasher(hasher: S) -> Self {
    Self::with_options_and_hasher(Default::default(), hasher).await
  }

  /// Creates a new `EquivalentDB` with the given options and hasher.
  #[inline]
  pub async fn with_options_and_hasher(opts: Options, hasher: S) -> Self {
    let inner = Arc::new(
      Inner::<_, _, SP, _>::new(
        core::any::type_name::<Self>(),
        opts.max_batch_size(),
        opts.max_batch_entries(),
        hasher,
      )
      .await,
    );
    Self { inner }
  }

  /// Returns the current read version of the database.
  #[inline]
  pub async fn version(&self) -> u64 {
    self.inner.version().await
  }

  /// Create a read transaction.
  #[inline]
  pub async fn read(&self) -> ReadTransaction<K, V, EquivalentDB<K, V, SP, S>, HashCm<K, S>, SP> {
    ReadTransaction::new(self.clone(), self.inner.tm.read().await)
  }
}

impl<K, V, SP, S> EquivalentDB<K, V, SP, S>
where
  K: Ord + Eq + core::hash::Hash + Send + Sync + 'static,
  V: Send + Sync + 'static,
  S: BuildHasher + Clone + Send + Sync + 'static,
  SP: AsyncSpawner,
{
  /// Create a write transaction.
  #[inline]
  pub async fn write(&self) -> WriteTransaction<K, V, SP, S> {
    WriteTransaction::new(self.clone()).await
  }
}

use super::*;

mod write;
pub use write::*;

#[cfg(test)]
mod tests;

struct Inner<K, V, S = std::hash::RandomState> {
  tm: Tm<K, V, HashCm<K, S>, PendingMap<K, V>>,
  map: SkipCore<K, V>,
  hasher: S,
  max_batch_size: u64,
  max_batch_entries: u64,
}

impl<K, V, S> Inner<K, V, S> {
  fn new(name: &str, max_batch_size: u64, max_batch_entries: u64, hasher: S) -> Self {
    let tm = Tm::new(name, 0);
    Self {
      tm,
      map: SkipCore::new(),
      hasher,
      max_batch_size,
      max_batch_entries,
    }
  }

  fn version(&self) -> u64 {
    self.tm.version()
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
///
/// `EquivalentDB` requires key to be [`Ord`] and [`Hash`](core::hash::Hash).
///
/// Comparing to [`ComparableDB`](crate::comparable::ComparableDB),
/// `EquivalentDB` has more flexible write transaction APIs and no clone happen.
/// But, [`ComparableDB`](crate::comparable::ComparableDB) does not require the key to implement [`Hash`](core::hash::Hash).
pub struct EquivalentDB<K, V, S = std::hash::RandomState> {
  inner: Arc<Inner<K, V, S>>,
}

impl<K, V, S> AsSkipCore<K, V> for EquivalentDB<K, V, S> {
  #[inline]
  #[allow(private_interfaces)]
  fn as_inner(&self) -> &SkipCore<K, V> {
    &self.inner.map
  }
}

impl<K, V, S> Clone for EquivalentDB<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for EquivalentDB<K, V> {
  /// Creates a new `EquivalentDB` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> EquivalentDB<K, V> {
  /// Creates a new `EquivalentDB` with the given options.
  #[inline]
  pub fn new() -> Self {
    Self::with_options_and_hasher(Default::default(), Default::default())
  }
}

impl<K, V, S> EquivalentDB<K, V, S> {
  /// Creates a new `EquivalentDB` with the given hasher.
  #[inline]
  pub fn with_hasher(hasher: S) -> Self {
    Self::with_options_and_hasher(Default::default(), hasher)
  }

  /// Creates a new `EquivalentDB` with the given options and hasher.
  #[inline]
  pub fn with_options_and_hasher(opts: Options, hasher: S) -> Self {
    let inner = Arc::new(Inner::new(
      core::any::type_name::<Self>(),
      opts.max_batch_size(),
      opts.max_batch_entries(),
      hasher,
    ));
    Self { inner }
  }

  /// Returns the current read version of the database.
  #[inline]
  pub fn version(&self) -> u64 {
    self.inner.version()
  }

  /// Create a read transaction.
  #[inline]
  pub fn read(&self) -> ReadTransaction<K, V, EquivalentDB<K, V, S>, HashCm<K, S>> {
    ReadTransaction::new(self.clone(), self.inner.tm.read())
  }
}

impl<K, V, S> EquivalentDB<K, V, S>
where
  K: Ord + Eq + core::hash::Hash,
  V: 'static,
  S: BuildHasher + Clone,
{
  /// Create a write transaction.
  #[inline]
  pub fn write(&self) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone())
  }
}

impl<K, V, S> EquivalentDB<K, V, S>
where
  K: Ord + Eq + core::hash::Hash + Send + 'static,
  V: Send + 'static,
  S: BuildHasher + Clone,
{
  /// Compact the database.
  #[inline]
  pub fn compact(&self) {
    self.inner.map.compact(self.inner.tm.discard_hint());
  }
}

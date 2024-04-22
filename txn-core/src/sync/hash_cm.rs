use smallvec_wrapper::MediumVec;

use super::*;

/// Options for the [`HashCm`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HashCmOptions<S> {
  /// The hasher used by the conflict manager.
  pub hasher: S,
  /// The initialized capacity of the conflict manager.
  pub capacity: Option<usize>,
}

impl<S> HashCmOptions<S> {
  /// Creates a new `HashCmOptions` with the given hasher.
  #[inline]
  pub const fn new(hasher: S) -> Self {
    Self {
      hasher,
      capacity: None,
    }
  }

  /// Creates a new `HashCmOptions` with the given hasher and capacity.
  #[inline]
  pub const fn with_capacity(hasher: S, capacity: usize) -> Self {
    Self {
      hasher,
      capacity: Some(capacity),
    }
  }
}

/// A [`Cm`] conflict manager implementation that based on the [`Hash`](core::hash::Hash).
pub struct HashCm<K, S = DefaultHasher> {
  reads: MediumVec<u64>,
  conflict_keys: IndexSet<u64, S>,
  _k: core::marker::PhantomData<K>,
}

impl<K, S: Clone> Clone for HashCm<K, S> {
  fn clone(&self) -> Self {
    Self {
      reads: self.reads.clone(),
      conflict_keys: self.conflict_keys.clone(),
      _k: core::marker::PhantomData,
    }
  }
}

impl<K, S> Cm for HashCm<K, S>
where
  S: BuildHasher,
  K: core::hash::Hash + Eq,
{
  type Error = core::convert::Infallible;
  type Key = K;
  type Options = HashCmOptions<S>;

  #[inline]
  fn new(options: Self::Options) -> Result<Self, Self::Error> {
    Ok(match options.capacity {
      Some(capacity) => Self {
        reads: MediumVec::with_capacity(capacity),
        conflict_keys: IndexSet::with_capacity_and_hasher(capacity, options.hasher),
        _k: core::marker::PhantomData,
      },
      None => Self {
        reads: MediumVec::new(),
        conflict_keys: IndexSet::with_hasher(options.hasher),
        _k: core::marker::PhantomData,
      },
    })
  }

  #[inline]
  fn mark_read(&mut self, key: &K) {
    let fp = self.conflict_keys.hasher().hash_one(key);
    self.reads.push(fp);
  }

  #[inline]
  fn mark_conflict(&mut self, key: &Self::Key) {
    let fp = self.conflict_keys.hasher().hash_one(key);
    self.conflict_keys.insert(fp);
  }

  #[inline]
  fn has_conflict(&self, other: &Self) -> bool {
    if self.reads.is_empty() {
      return false;
    }

    for ro in self.reads.iter() {
      if other.conflict_keys.contains(ro) {
        return true;
      }
    }
    false
  }

  #[inline]
  fn rollback(&mut self) -> Result<(), Self::Error> {
    self.reads.clear();
    self.conflict_keys.clear();
    Ok(())
  }
}

impl<K, S> CmEquivalent for HashCm<K, S>
where
  S: BuildHasher,
  K: core::hash::Hash + Eq,
{
  #[inline]
  fn mark_read_equivalent<Q>(&mut self, key: &Q)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let fp = self.conflict_keys.hasher().hash_one(key);
    self.reads.push(fp);
  }

  #[inline]
  fn mark_conflict_equivalent<Q>(&mut self, key: &Q)
  where
    Self::Key: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    let fp = self.conflict_keys.hasher().hash_one(key);
    self.conflict_keys.insert(fp);
  }
}

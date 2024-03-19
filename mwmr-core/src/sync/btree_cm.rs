use std::collections::BTreeSet;

use super::*;

/// A [`Cm`] conflict manager implementation that based on the [`BTreeSet`](std::collections::BTreeSet).
pub struct BTreeCm<K> {
  reads: MediumVec<K>,
  conflict_keys: BTreeSet<K>,
}

impl<K: Clone> Clone for BTreeCm<K> {
  fn clone(&self) -> Self {
    Self {
      reads: self.reads.clone(),
      conflict_keys: self.conflict_keys.clone(),
    }
  }
}

impl<K> Cm for BTreeCm<K>
where
  K: CheapClone + Ord + 'static,
{
  type Error = core::convert::Infallible;
  type Key = K;
  type Options = ();

  #[inline]
  fn new(_options: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      reads: MediumVec::new(),
      conflict_keys: BTreeSet::new(),
    })
  }

  #[inline]
  fn mark_read(&mut self, key: &K) {
    self.reads.push(key.cheap_clone());
  }

  #[inline]
  fn mark_conflict(&mut self, key: &Self::Key) {
    self.conflict_keys.insert(key.cheap_clone());
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
}

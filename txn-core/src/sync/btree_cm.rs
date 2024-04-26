use alloc::collections::BTreeMap;
use cheap_clone::CheapClone;
use smallvec_wrapper::MediumVec;

use super::*;

/// A [`Cm`] conflict manager implementation that based on the [`BTreeSet`](std::collections::BTreeSet).
pub struct BTreeCm<K> {
  reads: MediumVec<K>,
  conflict_keys: BTreeMap<K, Option<usize>>,
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
  K: CheapClone + Ord,
{
  type Error = core::convert::Infallible;
  type Key = K;
  type Options = ();

  #[inline]
  fn new(_options: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      reads: MediumVec::new(),
      conflict_keys: BTreeMap::new(),
    })
  }

  #[inline]
  fn mark_read(&mut self, key: &K) {
    self.reads.push(key.cheap_clone());
  }

  #[inline]
  fn mark_conflict(&mut self, key: &Self::Key) {
    let idx = if self.reads.is_empty() {
      None
    } else {
      Some(self.reads.len() - 1)
    };
    self.conflict_keys.insert(key.cheap_clone(), idx);
  }

  #[inline]
  fn has_conflict(&self, other: &Self) -> bool {
    if self.reads.is_empty() {
      return false;
    }

    // check direct conflict
    for ro in self.reads.iter() {
      if other.conflict_keys.contains_key(ro) {
        return true;
      }
    }

    // check indirect conflict
    for i in self
      .conflict_keys
      .iter()
      .filter_map(|(_, idx)| idx.map(|idx| idx))
    {
      let happens_before_reads = &other.reads[..i];

      for j in self
        .conflict_keys
        .iter()
        .filter_map(|(_, idx)| idx.map(|idx| idx))
      {
        let other_happens_before_reads = &other.reads[..j];

        if happens_before_reads
          .iter()
          .any(|ro| other_happens_before_reads.contains(ro))
        {
          return true;
        }
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

#[cfg(test)]
mod test {
  use super::{BTreeCm, Cm};

  #[test]
  fn test_btree_cm() {
    let mut cm = BTreeCm::<u64>::new(()).unwrap();
    cm.mark_read(&1);
    cm.mark_read(&2);
    cm.mark_conflict(&2);
    cm.mark_conflict(&3);
    let cm2 = cm.clone();
    assert!(cm.has_conflict(&cm2));
  }
}

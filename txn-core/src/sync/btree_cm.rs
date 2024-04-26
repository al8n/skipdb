use core::ops::Bound;

use alloc::collections::{
  // BTreeMap,
  BTreeSet,
};
use cheap_clone::CheapClone;
use smallvec_wrapper::MediumVec;

use super::*;

#[derive(Clone, Debug)]
enum Read<K> {
  Single(K),
  Range { start: Bound<K>, end: Bound<K> },
  All,
}

/// A [`Cm`] conflict manager implementation that based on the [`BTreeSet`](std::collections::BTreeSet).
#[derive(Debug)]
pub struct BTreeCm<K> {
  reads: MediumVec<Read<K>>,
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
  K: CheapClone + Ord,
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
    self.reads.push(Read::Single(key.cheap_clone()));
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
      match ro {
        Read::Single(k) => {
          if other.conflict_keys.contains(k) {
            return true;
          }
        }
        Read::Range { start, end } => match (start, end) {
          (Bound::Included(start), Bound::Included(end)) => {
            if other
              .conflict_keys
              .range((Bound::Included(start), Bound::Included(end)))
              .next()
              .is_some()
            {
              return true;
            }
          }
          (Bound::Included(start), Bound::Excluded(end)) => {
            if other
              .conflict_keys
              .range((Bound::Included(start), Bound::Excluded(end)))
              .next()
              .is_some()
            {
              return true;
            }
          }
          (Bound::Included(start), Bound::Unbounded) => {
            if other
              .conflict_keys
              .range((Bound::Included(start), Bound::Unbounded))
              .next()
              .is_some()
            {
              return true;
            }
          }
          (Bound::Excluded(start), Bound::Included(end)) => {
            if other
              .conflict_keys
              .range((Bound::Excluded(start), Bound::Included(end)))
              .next()
              .is_some()
            {
              return true;
            }
          }
          (Bound::Excluded(start), Bound::Excluded(end)) => {
            if other
              .conflict_keys
              .range((Bound::Excluded(start), Bound::Excluded(end)))
              .next()
              .is_some()
            {
              return true;
            }
          }
          (Bound::Excluded(start), Bound::Unbounded) => {
            if other
              .conflict_keys
              .range((Bound::Excluded(start), Bound::Unbounded))
              .next()
              .is_some()
            {
              return true;
            }
          }
          (Bound::Unbounded, Bound::Included(end)) => {
            let range = ..=end;
            for write in other.conflict_keys.iter() {
              if range.contains(&write) {
                return true;
              }
            }
          }
          (Bound::Unbounded, Bound::Excluded(end)) => {
            let range = ..end;
            for write in other.conflict_keys.iter() {
              if range.contains(&write) {
                return true;
              }
            }
          }
          (Bound::Unbounded, Bound::Unbounded) => unreachable!(),
        },
        Read::All => {
          if !other.conflict_keys.is_empty() {
            return true;
          }
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

impl<K> CmRange for BTreeCm<K>
where
  K: CheapClone + Ord,
{
  fn mark_range(&mut self, range: impl RangeBounds<<Self as Cm>::Key>) {
    let start = match range.start_bound() {
      Bound::Included(k) => Bound::Included(k.cheap_clone()),
      Bound::Excluded(k) => Bound::Excluded(k.cheap_clone()),
      Bound::Unbounded => Bound::Unbounded,
    };

    let end = match range.end_bound() {
      Bound::Included(k) => Bound::Included(k.cheap_clone()),
      Bound::Excluded(k) => Bound::Excluded(k.cheap_clone()),
      Bound::Unbounded => Bound::Unbounded,
    };

    if start == Bound::Unbounded && end == Bound::Unbounded {
      self.reads.push(Read::All);
      return;
    }

    self.reads.push(Read::Range { start, end });
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

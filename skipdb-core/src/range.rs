use either::Either;
use txn_core::sync::{Cm, Marker};

use super::*;

use crossbeam_skiplist::map::Range as MapRange;
use core::cmp;

/// An iterator over a subset of entries of the database.
pub struct Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  pub(crate) range: MapRange<'a, Q, R, K, Values<V>>,
  pub(crate) version: u64,
}

impl<'a, Q, R, K, V> Iterator for Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next()?;
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(CommittedRef { version, ent }.into());
      }
    }
  }
}

/// An iterator over a subset of entries of the database.
pub struct WriteTransactionRange<'a, Q, R, K, V, C>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q> + 'a,
  Q: Ord + ?Sized,
{
  pub(crate) committed: Range<'a, Q, R, K, V>,
  pub(crate) pendings: BTreeMapRange<'a, K, EntryValue<V>>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<Ref<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, Ref<'a, K, V>>>,
  marker: Option<Marker<'a, C>>,
}

impl<'a, Q, R, K, V, C> WriteTransactionRange<'a, Q, R, K, V, C>
where
  K: Ord + Borrow<Q>,
  Q: Ord + ?Sized,
  R: RangeBounds<Q> + 'a,
  C: Cm<Key = K>,
{
  fn advance_pending(&mut self) {
    self.next_pending = self.pendings.next();
  }

  fn advance_committed(&mut self) {
    self.next_committed = self.committed.next();
    if let (Some(item), Some(marker)) = (&self.next_committed, &mut self.marker) {
      marker.mark(item.key());
    }
  }

  pub fn new(
    pendings: BTreeMapRange<'a, K, EntryValue<V>>,
    committed: Range<'a, Q, R, K, V>,
    marker: Option<Marker<'a, C>>,
  ) -> Self {
    let mut iterator = WriteTransactionRange {
      pendings,
      committed,
      next_pending: None,
      next_committed: None,
      last_yielded_key: None,
      marker,
    };

    iterator.advance_pending();
    iterator.advance_committed();

    iterator
  }
}

impl<'a, Q, R, K, V, C> Iterator for WriteTransactionRange<'a, Q, R, K, V, C>
where
  K: Ord + Borrow<Q>,
  Q: Ord + ?Sized,
  R: RangeBounds<Q> + 'a,
  C: Cm<Key = K>,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      match (self.next_pending, &self.next_committed) {
        // Both pending and committed iterators have items to yield.
        (Some((pending_key, _)), Some(committed)) => {
          match pending_key.cmp(committed.key()) {
            // Pending item has a smaller key, so yield this one.
            cmp::Ordering::Less => {
              let (key, value) = self.next_pending.take().unwrap();
              self.advance_pending();
              self.last_yielded_key = Some(Either::Left(key));
              let version = value.version;
              match &value.value {
                Some(value) => return Some((version, key, value).into()),
                None => continue,
              }
            }
            // Keys are equal, so we prefer the pending item and skip the committed one.
            cmp::Ordering::Equal => {
              // Skip committed if it has the same key as pending
              self.advance_committed();
              // Loop again to check the next item without yielding anything this time.
              continue;
            }
            // Committed item has a smaller key, so we consider yielding this one.
            cmp::Ordering::Greater => {
              let committed = self.next_committed.take().unwrap();
              self.advance_committed(); // Prepare the next committed item for future iterations.
                                        // Yield the committed item if it has not been yielded before.
              if self.last_yielded_key.as_ref().map_or(true, |k| match k {
                Either::Left(k) => *k != committed.key(),
                Either::Right(item) => item.key() != committed.key(),
              }) {
                self.last_yielded_key = Some(Either::Right(committed.clone()));
                return Some(committed);
              }
            }
          }
        }
        // Only pending items are left, so yield the next pending item.
        (Some((_, _)), None) => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending(); // Advance the pending iterator for the next iteration.
          self.last_yielded_key = Some(Either::Left(key)); // Update the last yielded key.
          let version = value.version;
          match &value.value {
            Some(value) => return Some((version, key, value).into()),
            None => continue,
          }
        }
        // Only committed items are left, so yield the next committed item if it hasn't been yielded already.
        (None, Some(committed)) => {
          if self.last_yielded_key.as_ref().map_or(true, |k| match k {
            Either::Left(k) => *k != committed.key(),
            Either::Right(item) => item.key() != committed.key(),
          }) {
            let committed = self.next_committed.take().unwrap();
            self.advance_committed(); // Advance the committed iterator for the next iteration.
            self.last_yielded_key = Some(Either::Right(committed.clone()));
            return Some(committed);
          } else {
            // The key has already been yielded, so move to the next.
            self.advance_committed();
            // Loop again to check the next item without yielding anything this time.
            continue;
          }
        }
        // Both iterators have no items left to yield.
        (None, None) => return None,
      }
    }
  }
}

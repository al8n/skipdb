use either::Either;
use mwmr_core::{
  sync::{Cm, Marker},
  types::{EntryDataRef, EntryRef},
};

use super::*;

use crossbeam_skiplist::map::Range as MapRange;
use std::collections::btree_map::Range as BTreeMapRange;

/// An iterator over a subset of entries of the database.
pub struct Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  pub(crate) range: MapRange<'a, Q, R, K, Arc<SkipMap<u64, Option<V>>>>,
  pub(crate) version: u64,
}

impl<'a, Q, R, K, V> Iterator for Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  type Item = CommittedRef<'a, K, V>;

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
        return Some(CommittedRef { version, ent });
      }
    }
  }
}

impl<'a, Q, R, K, V> DoubleEndedIterator for Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next_back()?;
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(CommittedRef { version, ent });
      }
    }
  }
}

/// An iterator over a subset of entries of the database.
pub struct AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  pub(crate) range: MapRange<'a, Q, R, K, Arc<SkipMap<u64, Option<V>>>>,
  pub(crate) version: u64,
}

impl<'a, Q, R, K, V> Iterator for AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  type Item = AllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next()?;
      let min = *ent.value().front().unwrap().key();
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
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          cursor: AllVersionsCursor::Start,
          entries: ent,
        });
      }
    }
  }
}

impl<'a, Q, R, K, V> DoubleEndedIterator for AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next_back()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          cursor: AllVersionsCursor::Start,
          entries: ent,
        });
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
  next_committed: Option<CommittedRef<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, CommittedRef<'a, K, V>>>,
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
  type Item = Either<(&'a K, &'a V), CommittedRef<'a, K, V>>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      // Retrieve the current keys from pending and committed iterators
      let next_pending_key = self.next_pending.map(|(k, _)| k);
      let next_committed_key = self.next_committed.as_ref().map(|ref_| ref_.key());

      match (next_pending_key, next_committed_key) {
        // If both pending and committed have next items
        (Some(pending_key), Some(committed_key)) if pending_key <= committed_key => {
          // Pending key is smaller or equal, prefer pending
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => l != &key,
              Either::Right(r) => r.key() != key,
            })
          {
            // Update last yielded key and return the item if it hasn't been yielded before
            self.last_yielded_key = Some(Either::Left(key));
            match &value.value {
              Some(value) => return Some(Either::Left((key, value))),
              None => continue,
            }
          }
        }
        // Handle the case where either only committed has items left or both have items and committed key is smaller
        (None, Some(_)) | (Some(_), Some(_)) => {
          let committed = self.next_committed.take().unwrap();
          self.advance_committed();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => *l != committed.key(),
              Either::Right(r) => r.key() != committed.key(),
            })
          {
            // Update last yielded key and return the item if it hasn't been yielded before
            self.last_yielded_key = Some(Either::Right(committed.clone()));
            return Some(Either::Right(committed));
          }
        }
        // If only pending has items left
        (Some(_), None) => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          // Update last yielded key and return the item if it hasn't been yielded before
          self.last_yielded_key = Some(Either::Left(key));
          match &value.value {
            Some(value) => return Some(Either::Left((key, value))),
            None => continue,
          }
        }
        // No items left in either iterator
        (None, None) => return None,
      }
    }
  }
}

impl<'a, Q, R, K, V, C> DoubleEndedIterator for WriteTransactionRange<'a, Q, R, K, V, C>
where
  K: Ord + Borrow<Q>,
  Q: Ord + ?Sized,
  R: RangeBounds<Q> + 'a,
  C: Cm<Key = K>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      // Retrieve the current keys from pending and committed iterators
      let next_pending_key = self.next_pending.map(|(k, _)| k);
      let next_committed_key = self.next_committed.as_ref().map(|ref_| ref_.key());

      match (next_pending_key, next_committed_key) {
        (Some(pending_key), Some(committed_key)) if pending_key >= committed_key => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => *l != key,
              Either::Right(r) => r.key() != key,
            })
          {
            self.last_yielded_key = Some(Either::Left(key));
            match &value.value {
              Some(value) => return Some(Either::Left((key, value))),
              None => continue,
            }
          }
        }
        (Some(_), Some(_)) | (None, Some(_)) => {
          let committed = self.next_committed.take().unwrap();
          self.advance_committed();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => *l != committed.key(),
              Either::Right(r) => r.key() != committed.key(),
            })
          {
            self.last_yielded_key = Some(Either::Right(committed.clone()));
            return Some(Either::Right(committed));
          }
        }
        (Some(_), None) => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          self.last_yielded_key = Some(Either::Left(key));
          match &value.value {
            Some(value) => return Some(Either::Left((key, value))),
            None => continue,
          }
        }
        (None, None) => return None,
      }
    }
  }
}

/// An iterator over a subset of entries of the database.
pub struct WriteTransactionAllVersionsRange<'a, Q, R, K, V, C, D>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q> + 'a,
  Q: Ord + ?Sized,
{
  db: &'a D,
  pendings: BTreeMapRange<'a, K, EntryValue<V>>,
  committed: AllVersionsRange<'a, Q, R, K, V>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<AllVersions<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, AllVersions<'a, K, V>>>,
  marker: Option<Marker<'a, C>>,
  version: u64,
}

impl<'a, Q, R, K, V, C, D> WriteTransactionAllVersionsRange<'a, Q, R, K, V, C, D>
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
    db: &'a D,
    version: u64,
    pendings: BTreeMapRange<'a, K, EntryValue<V>>,
    committed: AllVersionsRange<'a, Q, R, K, V>,
    marker: Option<Marker<'a, C>>,
  ) -> Self {
    let mut iterator = WriteTransactionAllVersionsRange {
      db,
      pendings,
      committed,
      next_pending: None,
      next_committed: None,
      last_yielded_key: None,
      marker,
      version,
    };

    iterator.advance_pending();
    iterator.advance_committed();

    iterator
  }
}

impl<'a, Q, R, K, V, C, D> Iterator for WriteTransactionAllVersionsRange<'a, Q, R, K, V, C, D>
where
  K: Ord + Borrow<Q>,
  Q: Ord + ?Sized + 'a,
  R: RangeBounds<Q> + 'a,
  C: Cm<Key = K>,
  D: Database<K, V>,
{
  type Item = WriteTransactionAllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      // Retrieve the current keys from pending and committed iterators
      let next_pending_key = self.next_pending.map(|(k, _)| k);
      let next_committed_key = self.next_committed.as_ref().map(|ref_| ref_.key());

      match (next_pending_key, next_committed_key) {
        // If both pending and committed have next items
        (Some(pending_key), Some(committed_key)) if pending_key <= committed_key => {
          // Pending key is smaller or equal, prefer pending
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => l != &key,
              Either::Right(r) => r.key() != key,
            })
          {
            // Update last yielded key and return the item if it hasn't been yielded before
            self.last_yielded_key = Some(Either::Left(key));
            return Some(WriteTransactionAllVersions {
              pending: Some(EntryRef {
                data: match value.value {
                  Some(ref value) => EntryDataRef::Insert { key, value },
                  None => EntryDataRef::Remove(key),
                },
                version: value.version,
              }),
              committed: self
                .db
                .as_inner()
                .get_all_versions(key.borrow(), self.version),
              version: self.version,
            });
          }
        }
        // Handle the case where either only committed has items left or both have items and committed key is smaller
        (None, Some(_)) | (Some(_), Some(_)) => {
          let committed = self.next_committed.take().unwrap();
          self.advance_committed();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => *l != committed.key(),
              Either::Right(r) => r.key() != committed.key(),
            })
          {
            // Update last yielded key and return the item if it hasn't been yielded before
            self.last_yielded_key = Some(Either::Right(committed.clone()));
            return Some(WriteTransactionAllVersions {
              pending: None,
              committed: Some(committed),
              version: self.version,
            });
          }
        }
        // If only pending has items left
        (Some(_), None) => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          // Update last yielded key and return the item if it hasn't been yielded before
          self.last_yielded_key = Some(Either::Left(key));

          return Some(WriteTransactionAllVersions {
            pending: Some(EntryRef {
              data: match value.value {
                Some(ref value) => EntryDataRef::Insert { key, value },
                None => EntryDataRef::Remove(key),
              },
              version: value.version,
            }),
            committed: self
              .db
              .as_inner()
              .get_all_versions(key.borrow(), self.version),
            version: self.version,
          });
        }
        // No items left in either iterator
        (None, None) => return None,
      }
    }
  }
}

impl<'a, Q, R, K, V, C, D> DoubleEndedIterator
  for WriteTransactionAllVersionsRange<'a, Q, R, K, V, C, D>
where
  K: core::hash::Hash + Eq + Ord + Borrow<Q>,
  Q: Ord + ?Sized + 'a,
  R: RangeBounds<Q> + 'a,
  C: Cm<Key = K>,
  D: Database<K, V>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      // Retrieve the current keys from pending and committed iterators
      let next_pending_key = self.next_pending.map(|(k, _)| k);
      let next_committed_key = self.next_committed.as_ref().map(|ref_| ref_.key());

      match (next_pending_key, next_committed_key) {
        (Some(pending_key), Some(committed_key)) if pending_key >= committed_key => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => *l != key,
              Either::Right(r) => r.key() != key,
            })
          {
            self.last_yielded_key = Some(Either::Left(key));
            return Some(WriteTransactionAllVersions {
              pending: Some(EntryRef {
                data: match value.value {
                  Some(ref value) => EntryDataRef::Insert { key, value },
                  None => EntryDataRef::Remove(key),
                },
                version: value.version,
              }),
              committed: self
                .db
                .as_inner()
                .get_all_versions(key.borrow(), self.version),
              version: self.version,
            });
          }
        }
        (Some(_), Some(_)) | (None, Some(_)) => {
          let committed = self.next_committed.take().unwrap();
          self.advance_committed();
          if self
            .last_yielded_key
            .as_ref()
            .map_or(true, |either| match either {
              Either::Left(l) => *l != committed.key(),
              Either::Right(r) => r.key() != committed.key(),
            })
          {
            self.last_yielded_key = Some(Either::Right(committed.clone()));
            return Some(WriteTransactionAllVersions {
              pending: None,
              committed: Some(committed),
              version: self.version,
            });
          }
        }
        (Some(_), None) => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending();
          self.last_yielded_key = Some(Either::Left(key));
          return Some(WriteTransactionAllVersions {
            pending: Some(EntryRef {
              data: match value.value {
                Some(ref value) => EntryDataRef::Insert { key, value },
                None => EntryDataRef::Remove(key),
              },
              version: value.version,
            }),
            committed: self
              .db
              .as_inner()
              .get_all_versions(key.borrow(), self.version),
            version: self.version,
          });
        }
        (None, None) => return None,
      }
    }
  }
}

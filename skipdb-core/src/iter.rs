use either::Either;
use mwmr_core::{
  sync::{Cm, Marker},
  types::{EntryDataRef, EntryRef},
};

use super::*;

use crossbeam_skiplist::map::{Entry as MapEntry, Iter as MapIter};
use std::{cmp, collections::btree_map::Iter as BTreeMapIter, iter::FusedIterator};

#[derive(Copy, Clone)]
pub(crate) enum AllVersionsCursor {
  Start,
  Current(u64),
  End,
}

/// An iterator over a all values with the same key in different versions.
pub struct AllVersions<'a, K, V> {
  pub(crate) max_version: u64,
  pub(crate) min_version: u64,
  pub(crate) cursor: AllVersionsCursor,
  pub(crate) entries: MapEntry<'a, K, Arc<SkipMap<u64, Option<V>>>>,
}

impl<'a, K, V> AllVersions<'a, K, V> {
  /// Returns the key of the entries.
  pub fn key(&self) -> &K {
    self.entries.key()
  }
}

impl<'a, K, V> Clone for AllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      max_version: self.max_version,
      min_version: self.min_version,
      cursor: self.cursor,
      entries: self.entries.clone(),
    }
  }
}

impl<'a, K, V> Iterator for AllVersions<'a, K, V> {
  type Item = VersionedRef<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    let current_version = match self.cursor {
      AllVersionsCursor::Start => self.max_version,
      AllVersionsCursor::Current(version) => version,
      AllVersionsCursor::End => {
        return None;
      }
    };

    self
      .entries
      .value()
      .upper_bound(Bound::Included(&current_version))
      .map(|ent| {
        let ent_version = *ent.key();

        if current_version == self.min_version || ent_version == self.min_version {
          self.cursor = AllVersionsCursor::End;
        } else {
          self.cursor = AllVersionsCursor::Current(ent_version - 1);
        }

        VersionedCommittedRef {
          ent: self.entries.clone(),
          version: ent_version,
        }
        .into()
      })
  }

  fn last(self) -> Option<Self::Item>
  where
    Self: Sized,
  {
    self
      .entries
      .value()
      .lower_bound(Bound::Included(&self.max_version))
      .map(|ent| {
        VersionedCommittedRef {
          ent: self.entries.clone(),
          version: *ent.key(),
        }
        .into()
      })
  }
}

impl<'a, K, V> FusedIterator for AllVersions<'a, K, V> {}

/// An iterator over a all values with the same key in different versions.
pub struct WriteTransactionAllVersions<'a, K, V> {
  pub(crate) pending: Option<EntryRef<'a, K, V>>,
  pub(crate) committed: Option<AllVersions<'a, K, V>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Clone for WriteTransactionAllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      pending: self.pending,
      committed: self.committed.clone(),
      version: self.version,
    }
  }
}

impl<'a, K, V> WriteTransactionAllVersions<'a, K, V> {
  /// Create a new instance.
  pub fn new(
    version: u64,
    pending: Option<EntryRef<'a, K, V>>,
    committed: Option<AllVersions<'a, K, V>>,
  ) -> Self {
    Self {
      pending,
      committed,
      version,
    }
  }

  /// Returns the key of the entries.
  #[inline]
  pub fn key(&self) -> &K {
    match &self.pending {
      Some(p) => p.key(),
      None => self.committed.as_ref().unwrap().key(),
    }
  }

  /// Returns the version of the entries.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, K, V> Iterator for WriteTransactionAllVersions<'a, K, V> {
  type Item = VersionedRef<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(p) = self.pending.take() {
      return Some(p.into());
    }

    if let Some(committed) = &mut self.committed {
      committed.next()
    } else {
      None
    }
  }

  fn last(mut self) -> Option<Self::Item>
  where
    Self: Sized,
  {
    if let Some(committed) = self.committed.take() {
      return committed.last();
    }

    self.pending.take().map(Into::into)
  }
}

impl<'a, K, V> FusedIterator for WriteTransactionAllVersions<'a, K, V> {}

/// An iterator over the entries of the database.
pub struct Iter<'a, K, V> {
  pub(crate) iter: MapIter<'a, K, Arc<SkipMap<u64, Option<V>>>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
  K: Ord,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next()?;
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
/// An iterator over the entries of the database.
pub struct AllVersionsIter<'a, K, V> {
  pub(crate) iter: crossbeam_skiplist::map::Iter<'a, K, Arc<SkipMap<u64, Option<V>>>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Iterator for AllVersionsIter<'a, K, V>
where
  K: Ord,
{
  type Item = AllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .map(|ent| *ent.key())
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

/// Iterator over the entries of the write transaction.
pub struct WriteTransactionIter<'a, K, V, C> {
  pendings: BTreeMapIter<'a, K, EntryValue<V>>,
  committed: Iter<'a, K, V>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<Ref<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, Ref<'a, K, V>>>,
  marker: Option<Marker<'a, C>>,
}

impl<'a, K, V, C> WriteTransactionIter<'a, K, V, C>
where
  C: Cm<Key = K>,
  K: Ord,
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
    pendings: BTreeMapIter<'a, K, EntryValue<V>>,
    committed: Iter<'a, K, V>,
    marker: Option<Marker<'a, C>>,
  ) -> Self {
    let mut iterator = WriteTransactionIter {
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

impl<'a, K, V, C> Iterator for WriteTransactionIter<'a, K, V, C>
where
  K: Ord,
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

/// Iterator over the entries of the write transaction.
pub struct WriteTransactionAllVersionsIter<'a, K, V, C, D> {
  db: &'a D,
  pendings: BTreeMapIter<'a, K, EntryValue<V>>,
  committed: AllVersionsIter<'a, K, V>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<AllVersions<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, AllVersions<'a, K, V>>>,
  marker: Option<Marker<'a, C>>,
  version: u64,
}

impl<'a, K, V, C, D> WriteTransactionAllVersionsIter<'a, K, V, C, D>
where
  K: Ord,
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
    pendings: BTreeMapIter<'a, K, EntryValue<V>>,
    committed: AllVersionsIter<'a, K, V>,
    marker: Option<Marker<'a, C>>,
  ) -> Self {
    let mut iterator = WriteTransactionAllVersionsIter {
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

impl<'a, K, V, C, D> Iterator for WriteTransactionAllVersionsIter<'a, K, V, C, D>
where
  K: Ord,
  C: Cm<Key = K>,
  D: Database<K, V>,
{
  type Item = WriteTransactionAllVersions<'a, K, V>;

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
              return Some(WriteTransactionAllVersions {
                pending: Some(EntryRef {
                  data: match value.value {
                    Some(ref value) => EntryDataRef::Insert { key, value },
                    None => EntryDataRef::Remove(key),
                  },
                  version: value.version,
                }),
                committed: self.db.as_inner().get_all_versions(key, self.version),
                version: self.version,
              });
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
                return Some(WriteTransactionAllVersions {
                  pending: None,
                  committed: Some(committed),
                  version: self.version,
                });
              }
            }
          }
        }
        // Only pending items are left, so yield the next pending item.
        (Some((_, _)), None) => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending(); // Advance the pending iterator for the next iteration.
          self.last_yielded_key = Some(Either::Left(key)); // Update the last yielded key.
          match &value.value {
            Some(val) => {
              return Some(WriteTransactionAllVersions {
                pending: Some(EntryRef {
                  data: EntryDataRef::Insert { key, value: val },
                  version: value.version,
                }),
                committed: self.db.as_inner().get_all_versions(key, self.version),
                version: self.version,
              })
            }
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
            return Some(WriteTransactionAllVersions {
              pending: None,
              committed: Some(committed),
              version: self.version,
            });
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

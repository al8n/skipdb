use mwmr::{Cm, EntryDataRef, EntryRef, Marker};

use super::*;

use crossbeam_skiplist::map::Iter as MapIter;
use std::{cmp, collections::btree_map::Iter as BTreeMapIter, iter::Rev};

/// An iterator over the entries of the database.
pub struct RevIter<'a, K, V> {
  pub(crate) iter: Rev<MapIter<'a, K, SkipMap<u64, Option<V>>>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Iterator for RevIter<'a, K, V>
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

/// An iterator over a all values with the same key in different versions.
pub struct RevAllVersions<'a, K, V> {
  pub(crate) max_version: u64,
  pub(crate) min_version: u64,
  pub(crate) cursor: AllVersionsCursor,
  pub(crate) entries: MapEntry<'a, K, SkipMap<u64, Option<V>>>,
}

impl<'a, K, V> RevAllVersions<'a, K, V> {
  /// Returns the key of the entries.
  pub fn key(&self) -> &K {
    self.entries.key()
  }
}

impl<'a, K, V> Clone for RevAllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      max_version: self.max_version,
      min_version: self.min_version,
      cursor: self.cursor,
      entries: self.entries.clone(),
    }
  }
}

impl<'a, K, V> Iterator for RevAllVersions<'a, K, V> {
  type Item = VersionedRef<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    let current_version = match self.cursor {
      AllVersionsCursor::Start => self.min_version,
      AllVersionsCursor::Current(version) => version,
      AllVersionsCursor::End => {
        return None;
      }
    };

    self
      .entries
      .value()
      .lower_bound(Bound::Included(&current_version))
      .map(|ent| {
        let ent_version = *ent.key();

        if current_version == self.max_version || ent_version == self.max_version {
          self.cursor = AllVersionsCursor::End;
        } else {
          self.cursor = AllVersionsCursor::Current(ent_version + 1);
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

impl<'a, K, V> FusedIterator for RevAllVersions<'a, K, V> {}

/// An iterator over the entries of the database.
pub struct RevAllVersionsIter<'a, K, V> {
  pub(crate) iter: Rev<MapIter<'a, K, SkipMap<u64, Option<V>>>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Iterator for RevAllVersionsIter<'a, K, V>
where
  K: Ord,
{
  type Item = RevAllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .map(|ent| *ent.key())
      {
        return Some(RevAllVersions {
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
pub struct WriteTransactionRevIter<'a, K, V, C> {
  pendings: Rev<BTreeMapIter<'a, K, EntryValue<V>>>,
  committed: RevIter<'a, K, V>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<Ref<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, Ref<'a, K, V>>>,
  marker: Option<Marker<'a, C>>,
}

impl<'a, K, V, C> WriteTransactionRevIter<'a, K, V, C>
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

  pub(crate) fn new(
    pendings: Rev<BTreeMapIter<'a, K, EntryValue<V>>>,
    committed: RevIter<'a, K, V>,
    marker: Option<Marker<'a, C>>,
  ) -> Self {
    let mut iterator = WriteTransactionRevIter {
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

impl<'a, K, V, C> Iterator for WriteTransactionRevIter<'a, K, V, C>
where
  K: Ord + 'static + core::fmt::Debug,
  C: Cm<Key = K>,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      match (self.next_pending, &self.next_committed) {
        // Both pending and committed iterators have items to yield.
        (Some((pending_key, _)), Some(committed)) => {
          match pending_key.cmp(committed.key()) {
            // Pending item has a larger key, so yield this one.
            cmp::Ordering::Greater => {
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
            // Committed item has a larger key, so we consider yielding this one.
            cmp::Ordering::Less => {
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

/// An iterator over a all values with the same key in different versions.
pub struct WriteTransactionRevAllVersions<'a, K, V> {
  pub(crate) pending: Option<EntryRef<'a, K, V>>,
  pub(crate) committed: Option<RevAllVersions<'a, K, V>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Clone for WriteTransactionRevAllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      pending: self.pending,
      committed: self.committed.clone(),
      version: self.version,
    }
  }
}

impl<'a, K, V> WriteTransactionRevAllVersions<'a, K, V> {
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

impl<'a, K, V> Iterator for WriteTransactionRevAllVersions<'a, K, V> {
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

impl<'a, K, V> FusedIterator for WriteTransactionRevAllVersions<'a, K, V> {}

/// Iterator over the entries of the write transaction.
pub struct WriteTransactionRevAllVersionsIter<'a, K, V, C, D> {
  db: &'a D,
  pendings: Rev<BTreeMapIter<'a, K, EntryValue<V>>>,
  committed: RevAllVersionsIter<'a, K, V>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<RevAllVersions<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, RevAllVersions<'a, K, V>>>,
  marker: Option<Marker<'a, C>>,
  version: u64,
}

impl<'a, K, V, C, D> WriteTransactionRevAllVersionsIter<'a, K, V, C, D>
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

  pub(crate) fn new(
    db: &'a D,
    version: u64,
    pendings: Rev<BTreeMapIter<'a, K, EntryValue<V>>>,
    committed: RevAllVersionsIter<'a, K, V>,
    marker: Option<Marker<'a, C>>,
  ) -> Self {
    let mut iterator = WriteTransactionRevAllVersionsIter {
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

impl<'a, K, V, C, D> Iterator for WriteTransactionRevAllVersionsIter<'a, K, V, C, D>
where
  K: Ord,
  C: Cm<Key = K>,
  D: Database<K, V>,
{
  type Item = WriteTransactionRevAllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      match (self.next_pending, &self.next_committed) {
        // Both pending and committed iterators have items to yield.
        (Some((pending_key, _)), Some(committed)) => {
          match pending_key.cmp(committed.key()) {
            // Pending item has a larger key, so yield this one.
            cmp::Ordering::Greater => {
              let (key, value) = self.next_pending.take().unwrap();
              self.advance_pending();
              self.last_yielded_key = Some(Either::Left(key));
              return Some(WriteTransactionRevAllVersions {
                pending: Some(EntryRef {
                  data: match value.value {
                    Some(ref value) => EntryDataRef::Insert { key, value },
                    None => EntryDataRef::Remove(key),
                  },
                  version: value.version,
                }),
                committed: self.db.as_inner().get_all_versions_rev(key, self.version),
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
            // Committed item has a larger key, so we consider yielding this one.
            cmp::Ordering::Less => {
              let committed = self.next_committed.take().unwrap();
              self.advance_committed(); // Prepare the next committed item for future iterations.
                                        // Yield the committed item if it has not been yielded before.
              if self.last_yielded_key.as_ref().map_or(true, |k| match k {
                Either::Left(k) => *k != committed.key(),
                Either::Right(item) => item.key() != committed.key(),
              }) {
                self.last_yielded_key = Some(Either::Right(committed.clone()));
                return Some(WriteTransactionRevAllVersions {
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
              return Some(WriteTransactionRevAllVersions {
                pending: Some(EntryRef {
                  data: EntryDataRef::Insert { key, value: val },
                  version: value.version,
                }),
                committed: self.db.as_inner().get_all_versions_rev(key, self.version),
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
            return Some(WriteTransactionRevAllVersions {
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

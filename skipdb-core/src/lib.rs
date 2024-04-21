#![allow(clippy::type_complexity)]

use std::{
  borrow::Borrow,
  collections::BTreeMap,
  ops::{Bound, RangeBounds}, sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc},
};

use mwmr_core::{
  sync::{Pwm, PwmComparable, PwmComparableRange, PwmRange},
  types::{Entry, EntryData, EntryValue},
};
use smallvec_wrapper::OneOrMore;

use crossbeam_skiplist::SkipMap;

pub mod iter;
use iter::*;

pub mod rev_iter;
use rev_iter::*;

pub mod range;
use range::*;

pub mod types;
use types::*;

/// The options used to create a new `EquivalentDB`.
#[derive(Debug, Clone)]
pub struct Options {
  max_batch_size: u64,
  max_batch_entries: u64,
  detect_conflicts: bool,
}

impl Default for Options {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Options {
  /// Creates a new `Options` with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      max_batch_size: u64::MAX,
      max_batch_entries: u64::MAX,
      detect_conflicts: true,
    }
  }

  /// Sets the maximum batch size in bytes.
  #[inline]
  pub fn with_max_batch_size(mut self, max_batch_size: u64) -> Self {
    self.max_batch_size = max_batch_size;
    self
  }

  /// Sets the maximum entries in batch.
  #[inline]
  pub fn with_max_batch_entries(mut self, max_batch_entries: u64) -> Self {
    self.max_batch_entries = max_batch_entries;
    self
  }

  /// Sets the detect conflicts.
  #[inline]
  pub fn with_detect_conflicts(mut self, detect_conflicts: bool) -> Self {
    self.detect_conflicts = detect_conflicts;
    self
  }

  /// Returns the maximum batch size in bytes.
  #[inline]
  pub const fn max_batch_size(&self) -> u64 {
    self.max_batch_size
  }

  /// Returns the maximum entries in batch.
  #[inline]
  pub const fn max_batch_entries(&self) -> u64 {
    self.max_batch_entries
  }

  /// Returns the detect conflicts.
  #[inline]
  pub const fn detect_conflicts(&self) -> bool {
    self.detect_conflicts
  }
}

/// Pending write manger implementation for [`EquivalentDB`] and [`ComparableDB`].
pub struct PendingMap<K, V> {
  map: BTreeMap<K, EntryValue<V>>,
  opts: Options,
}

impl<K: Clone, V: Clone> Clone for PendingMap<K, V> {
  fn clone(&self) -> Self {
    Self {
      map: self.map.clone(),
      opts: self.opts.clone(),
    }
  }
}

impl<K, V> Pwm for PendingMap<K, V>
where
  K: Ord,
{
  type Error = core::convert::Infallible;

  type Key = K;

  type Value = V;

  type Options = Options;

  type Iter<'a> = std::collections::btree_map::Iter<'a, K, EntryValue<V>> where Self: 'a;
  type IntoIter = std::collections::btree_map::IntoIter<K, EntryValue<V>>;

  fn new(options: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self {
      map: BTreeMap::new(),
      opts: options,
    })
  }

  fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  fn len(&self) -> usize {
    self.map.len()
  }

  fn validate_entry(&self, _entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error> {
    Ok(())
  }

  fn max_batch_size(&self) -> u64 {
    self.opts.max_batch_size
  }

  fn max_batch_entries(&self) -> u64 {
    self.opts.max_batch_entries
  }

  fn estimate_size(&self, _entry: &Entry<Self::Key, Self::Value>) -> u64 {
    core::mem::size_of::<Self::Key>() as u64 + core::mem::size_of::<Self::Value>() as u64
  }

  fn contains_key(&self, key: &Self::Key) -> Result<bool, Self::Error> {
    Ok(self.map.contains_key(key))
  }

  fn get(&self, key: &Self::Key) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
    Ok(self.map.get(key))
  }

  fn insert(&mut self, key: Self::Key, value: EntryValue<Self::Value>) -> Result<(), Self::Error> {
    self.map.insert(key, value);
    Ok(())
  }

  fn remove_entry(
    &mut self,
    key: &Self::Key,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error> {
    Ok(self.map.remove_entry(key))
  }
  fn iter(&self) -> Self::Iter<'_> {
    self.map.iter()
  }

  fn into_iter(self) -> Self::IntoIter {
    core::iter::IntoIterator::into_iter(self.map)
  }

  fn rollback(&mut self) -> Result<(), Self::Error> {
    self.map.clear();
    Ok(())
  }
}

impl<K, V> PwmRange for PendingMap<K, V>
where
  K: Ord,
{
  type Range<'a> = std::collections::btree_map::Range<'a, K, EntryValue<V>> where Self: 'a;

  fn range<R: RangeBounds<Self::Key>>(&self, range: R) -> Self::Range<'_> {
    self.map.range(range)
  }
}

impl<K, V> PwmComparableRange for PendingMap<K, V>
where
  K: Ord,
{
  fn range_comparable<T, R>(&self, range: R) -> Self::Range<'_>
  where
    T: ?Sized + Ord,
    Self::Key: Borrow<T> + Ord,
    R: RangeBounds<T>,
  {
    self.map.range(range)
  }
}

impl<K, V> PwmComparable for PendingMap<K, V>
where
  K: Ord,
{
  fn get_comparable<Q>(&self, key: &Q) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(self.map.get(key))
  }

  fn get_entry_comparable<Q>(
    &self,
    key: &Q,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(self.map.get_key_value(key))
  }

  fn contains_key_comparable<Q>(&self, key: &Q) -> Result<bool, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(self.map.contains_key(key))
  }

  fn remove_entry_comparable<Q>(
    &mut self,
    key: &Q,
  ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>
  where
    Self::Key: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    Ok(self.map.remove_entry(key))
  }
}

#[doc(hidden)]
pub trait Database<K, V>: AsSkipCore<K, V> {}

impl<K, V, T: AsSkipCore<K, V>> Database<K, V> for T {}

#[doc(hidden)]
pub trait AsSkipCore<K, V> {
  // This trait is sealed and cannot be implemented for types outside of this crate.
  // So returning a reference to the inner database is ok.
  fn as_inner(&self) -> &SkipCore<K, V>;
}

pub struct SkipCore<K, V> {
  map: SkipMap<K, Arc<SkipMap<u64, Option<V>>>>,
  all_versions_iters: SkipMap<u64, AtomicUsize>,
  last_discard_version: AtomicU64,
}

impl<K, V> Default for SkipCore<K, V> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> SkipCore<K, V> {
  #[inline]
  pub fn new() -> Self {
    Self {
      map: SkipMap::new(),
      all_versions_iters: SkipMap::new(),
      last_discard_version: AtomicU64::new(0),
    }
  }

  #[inline]
  pub fn by_ref(&self) -> &SkipMap<K, Arc<SkipMap<u64, Option<V>>>> {
    &self.map
  }
}

impl<K, V> SkipCore<K, V>
where
  K: Ord,
  V: Send + 'static,
{
  pub fn apply(&self, entries: OneOrMore<Entry<K, V>>) {
    for ent in entries {
      let version = ent.version();
      match ent.data {
        EntryData::Insert { key, value } => {
          let values = self.map.get_or_insert(key, Arc::new(SkipMap::new()));
          values.value().insert(version, Some(value));
        }
        EntryData::Remove(key) => {
          if let Some(values) = self.map.get(&key) {
            let values = values.value();
            if !values.is_empty() {
              values.insert(version, None);
            }
          }
        }
      }
    }
  }
}

impl<K, V> SkipCore<K, V>
where
  K: Ord,
{
  pub fn get<Q>(&self, key: &Q, version: u64) -> Option<CommittedRef<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let ent = self.map.get(key)?;
    let version = ent
      .value()
      .upper_bound(Bound::Included(&version))
      .and_then(|v| {
        if v.value().is_some() {
          Some(*v.key())
        } else {
          None
        }
      })?;

    Some(CommittedRef { ent, version })
  }

  pub fn contains_key<Q>(&self, key: &Q, version: u64) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    match self.map.get(key) {
      None => false,
      Some(values) => values
        .value()
        .upper_bound(Bound::Included(&version))
        .is_some(),
    }
  }

  pub fn get_all_versions<'a, 'b: 'a, Q>(
    &'a self,
    key: &'b Q,
    version: u64,
    discard_version: u64
  ) -> Option<AllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.map.get(key).and_then(move |values| {
      let ents = values.value();
      if ents.is_empty() {
        return None;
      }

      let min = *ents.front().unwrap().key();
      if min > version {
        return None;
      }

      Some(AllVersions {
        max_version: version,
        min_version: min,
        cursor: AllVersionsCursor::Start,
        entries: values,
      })
    })
  }

  pub fn get_all_versions_rev<'a, 'b: 'a, Q>(
    &'a self,
    key: &'b Q,
    version: u64,
    discard_version: u64
  ) -> Option<RevAllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.map.get(key).and_then(move |values| {
      let ents = values.value();
      if ents.is_empty() {
        return None;
      }

      let min = *ents.front().unwrap().key();
      if min > version {
        return None;
      }

      Some(RevAllVersions {
        max_version: version,
        min_version: min,
        cursor: AllVersionsCursor::Start,
        entries: values,
      })
    })
  }

  pub fn iter(&self, version: u64) -> Iter<'_, K, V> {
    let iter = self.map.iter();
    Iter { iter, version }
  }

  pub fn rev_iter(&self, version: u64) -> RevIter<'_, K, V> {
    let iter = self.map.iter();
    RevIter {
      iter: iter.rev(),
      version,
    }
  }

  pub fn iter_all_versions(&self, version: u64, discard_version: u64) -> AllVersionsIter<'_, K, V> {
    let iter = self.map.iter();
    AllVersionsIter { iter, version }
  }

  pub fn rev_iter_all_versions(&self, version: u64, discard_version: u64) -> RevAllVersionsIter<'_, K, V> {
    let iter = self.map.iter().rev();
    RevAllVersionsIter { iter, version }
  }

  pub fn range<Q, R>(&self, range: R, version: u64) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    Range {
      range: self.map.range(range),
      version,
    }
  }

  pub fn range_all_versions<Q, R>(&self, range: R, version: u64, discard_version: u64) -> AllVersionsRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    AllVersionsRange {
      range: self.map.range(range),
      version,
    }
  }

  
}

impl<K, V> SkipCore<K, V>
where
  K: Ord + Send + 'static,
  V: Send + Sync + 'static,
{
  pub fn compact(&self, new_discard_version: u64) -> u64 {
    let mut latest_discard_version = self.last_discard_version.load(Ordering::Acquire);
    match self.last_discard_version.compare_exchange(latest_discard_version, new_discard_version, Ordering::SeqCst, Ordering::Acquire) {
      Ok(_) => {},
      // if we fail to insert the new discard version,
      // which means there is another thread that is compacting the database.
      // To avoid run multiple compacting at the same time, we just return.
      Err(new_discard_version) => return new_discard_version,
    }

    // let num_iters = self.all_versions_iters.len();
    // if num_iters <= 1 {
    //   self.compact_fast_path(discard_version);
    //   return;
    // }
    match self.all_versions_iters.front() {
      // if there are no active all versions iterators, then we can remove all values under the discard version directly.
      None => {
        self.compact_without_all_versions_iterators(new_discard_version);
        return new_discard_version;
      },
      Some(refs) => {
        // if the discard version is the same as the old discard version,
        let old_discard_version = *refs.key();

        if old_discard_version == new_discard_version {
          self.compact_without_all_versions_iterators(new_discard_version);
          return new_discard_version;
        }

        // find the minimum discard version
        let mut actual_discard_version = new_discard_version;
        let mut refs = Some(refs);
        while let Some(current) = refs {
          let version = *current.key();
  
          if version > new_discard_version {
            break;
          }
  
          if current.value().load(Ordering::Acquire) > 0 {
            actual_discard_version = version;
            break;
          }

          refs = current.next();
        }

        // compact slow path

        actual_discard_version
      }
    }
  }

  fn compact_in(&self, actual_discard_version: u64) {
    for ents in self.map.iter() {
      let values = ents.value();

      let upper_bound = values.upper_bound(Bound::Included(&actual_discard_version));

      if let Some(upper_bound) = upper_bound {
        let mut prev = upper_bound.prev();

        while let Some(ent) = prev {
          prev = ent.prev();
          ent.remove();
        }
      }

      if values.is_empty() {
        ents.remove();
      }
    }
    // for ents in self.map.iter() {
    //   let values = ents.value();
    //   let num_values = values.len();
    //   if let Some(first) = values.front() {
    //     let min_version = *first.key();

    //     // if the min version of the entry is larger than the discard version,
    //     // then we can skip this key.
    //     if min_version > actual_discard_version {
    //       continue;
    //     }

    //     // if the key only has one value and the value is not none, then we just skip this key.
    //     if num_values == 1 {
    //       if first.value().is_some() {
    //         continue;
    //       }
          
    //       if min_version > old_discard_version {
    //         first.remove();
    //         continue;
    //       }

    //       continue;
    //     }

    //     // Get the range of values that are under the discard version.
    //     let range = values.range(..actual_discard_version);
    //     for ent in range {
    //       let version = *ent.key();

    //       // if the version of the entry is larger than the discard version of the iterator,
    //       // then we can remove the value. Because the all versions iter will only yield
    //       // values that are larger than or eqaul to the old_discard_version.
    //       if version > old_discard_version {
    //         ent.remove();
    //       }
    //     }
    //   } else {
    //     ents.remove();
    //   }
    // }
  }

  /// No active all versions iterators, then we can remove all values under the discard version directly.
  fn compact_without_all_versions_iterators(&self, discard_version: u64) {
    for ents in self.map.iter() {
      let values = ents.value();
      let num_values = values.len();
      if num_values == 1 {
        let val = values.front().unwrap();
        let version = *val.key();
        if val.value().is_some() {
          continue;
        }

        if version < discard_version {
          crossbeam_skiplist::map::Entry::remove(&ents);
        }

        continue;
      }

      for ent in values.range(..discard_version) {
        ent.remove();
      }
    }
  }

  fn compact_fast_path(&self, discard_version: u64) {
    if let Some(refs) = self.all_versions_iters.front() {
      let old_discard_version = *refs.key();
      if old_discard_version == discard_version {
        self.compact_without_all_versions_iterators(discard_version);
        return;
      }

      for ents in self.map.iter() {
        let values = ents.value();
        let num_values = values.len();
        if let Some(first) = values.front() {
          let min_version = *first.key();

          // if the min version of the entry is larger than the discard version,
          // then we can skip this key.
          if min_version > discard_version {
            continue;
          }

          // if the key only has one value and the value is not none, then we just skip this key.
          if num_values == 1 {
            if first.value().is_some() {
              continue;
            }
            
            if min_version > old_discard_version {
              first.remove();
              continue;
            }

            continue;
          }

          // Get the range of values that are under the discard version.
          let range = values.range(..discard_version);
          for ent in range {
            let version = *ent.key();

            // if the version of the entry is larger than the discard version of the iterator,
            // then we can remove the value. Because the all versions iter will only yield
            // values that are larger than or eqaul to the old_discard_version.
            if version > old_discard_version {
              ent.remove();
            }
          }
        } else {
          ents.remove();
        }
      }
    } else {
      self.compact_without_all_versions_iterators(discard_version);
      return;
    }
  }
}
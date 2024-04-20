use std::{
  borrow::Borrow,
  collections::BTreeMap,
  ops::{Bound, RangeBounds},
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

pub struct SkipCore<K, V>(SkipMap<K, SkipMap<u64, Option<V>>>);

impl<K, V> Default for SkipCore<K, V> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> SkipCore<K, V> {
  #[inline]
  pub fn new() -> Self {
    Self(SkipMap::new())
  }

  #[inline]
  pub fn by_ref(&self) -> &SkipMap<K, SkipMap<u64, Option<V>>> {
    &self.0
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
          let values = self.0.get_or_insert(key, SkipMap::new());
          values.value().insert(version, Some(value));
        }
        EntryData::Remove(key) => {
          if let Some(values) = self.0.get(&key) {
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
    let ent = self.0.get(key)?;
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
    match self.0.get(key) {
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
  ) -> Option<AllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.0.get(key).and_then(move |values| {
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
  ) -> Option<RevAllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.0.get(key).and_then(move |values| {
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
    let iter = self.0.iter();
    Iter { iter, version }
  }

  pub fn rev_iter(&self, version: u64) -> RevIter<'_, K, V> {
    let iter = self.0.iter();
    RevIter {
      iter: iter.rev(),
      version,
    }
  }

  pub fn iter_all_versions(&self, version: u64) -> AllVersionsIter<'_, K, V> {
    let iter = self.0.iter();
    AllVersionsIter { iter, version }
  }

  pub fn rev_iter_all_versions(&self, version: u64) -> RevAllVersionsIter<'_, K, V> {
    let iter = self.0.iter().rev();
    RevAllVersionsIter { iter, version }
  }

  pub fn range<Q, R>(&self, range: R, version: u64) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    Range {
      range: self.0.range(range),
      version,
    }
  }

  pub fn range_all_versions<Q, R>(&self, range: R, version: u64) -> AllVersionsRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    AllVersionsRange {
      range: self.0.range(range),
      version,
    }
  }
}

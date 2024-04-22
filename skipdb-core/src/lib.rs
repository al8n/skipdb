#![allow(clippy::type_complexity)]

use std::{
  borrow::Borrow,
  collections::BTreeMap,
  ops::{Bound, RangeBounds},
  sync::atomic::{AtomicU64, Ordering},
};

use smallvec_wrapper::OneOrMore;
use txn_core::{
  sync::{Pwm, PwmComparable, PwmComparableRange, PwmRange},
  types::{Entry, EntryData, EntryValue},
};

use crossbeam_skiplist::SkipMap;

pub mod iter;
use iter::*;

pub mod rev_iter;
use rev_iter::*;

pub mod range;
use range::*;

pub mod rev_range;
use rev_range::*;

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

  fn get_entry(
    &self,
    key: &Self::Key,
  ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error> {
    Ok(self.map.get_key_value(key))
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
  map: SkipMap<K, Values<V>>,
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
      last_discard_version: AtomicU64::new(0),
    }
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
          let ent = self.map.get_or_insert_with(key, || Values::new());
          let val = ent.value();
          val.lock();
          val.insert(version, Some(value));
          val.unlock();
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

  pub fn iter(&self, version: u64) -> Iter<'_, K, V> {
    let iter = self.map.iter();
    Iter { iter, version }
  }

  pub fn iter_rev(&self, version: u64) -> RevIter<'_, K, V> {
    let iter = self.map.iter();
    RevIter {
      iter: iter.rev(),
      version,
    }
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

  pub fn range_rev<Q, R>(&self, range: R, version: u64) -> RevRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    RevRange {
      range: self.map.range(range).rev(),
      version,
    }
  }
}

impl<K, V> SkipCore<K, V>
where
  K: Ord + Send + 'static,
  V: Send + 'static,
{
  pub fn compact(&self, new_discard_version: u64) {
    let latest_discard_version = self.last_discard_version.load(Ordering::Acquire);
    match self.last_discard_version.compare_exchange(
      latest_discard_version,
      new_discard_version,
      Ordering::SeqCst,
      Ordering::Acquire,
    ) {
      Ok(_) => {}
      // if we fail to insert the new discard version,
      // which means there is another thread that is compacting the database.
      // To avoid run multiple compacting at the same time, we just return.
      Err(_) => return,
    }

    for ent in self.map.iter() {
      let values = ent.value();

      // if the oldest version is larger or equal to the new discard version,
      // then nothing to remove.
      if let Some(oldest) = values.front() {
        let oldest_version = *oldest.key();
        if oldest_version >= new_discard_version {
          continue;
        }
      }

      if let Some(newest) = values.back() {
        let newest_version = *newest.key();

        // if the newest version is smaller than the new discard version,
        if newest_version < new_discard_version {
          // if the newest value is none, then we can try to remove the whole key.
          if newest.value().is_none() {
            // try to lock the entry.
            if values.try_lock() {
              // we get the lock, then we can remove the whole key.
              ent.remove();

              // unlock the entry.
              values.unlock();
              continue;
            }
          }

          // we leave the current newest value and try to remove previous values.
          let mut prev = newest.prev();
          while let Some(ent) = prev {
            prev = ent.prev();
            ent.remove();
          }
          continue;
        }

        // handle the complex case: we have some values that are larger than the new discard version,
        // and some values that are smaller than the new discard version.

        // find the first value that is smaller than the new discard version.
        let mut bound = values.upper_bound(Bound::Excluded(&new_discard_version));

        // means that no value is smaller than the new discard version.
        if bound.is_none() {
          continue;
        }

        // remove all values that are smaller than the new discard version.
        while let Some(ent) = bound {
          bound = ent.prev();
          ent.remove();
        }
      } else {
        // we do not have any value in the entry, then we can try to remove the whole key.

        // try to lock the entry.
        if values.try_lock() {
          // we get the lock, then we can remove the whole key.
          ent.remove();

          // unlock the entry.
          values.unlock();
        }
      }
    }
  }
}

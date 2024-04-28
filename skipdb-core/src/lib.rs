#![cfg_attr(not(feature = "std"), no_std)]
#![deny(warnings)]
#![forbid(unsafe_code)]
#![allow(clippy::type_complexity)]

extern crate alloc;

use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
  sync::atomic::{AtomicU64, Ordering},
};

use alloc::collections::btree_map::{Iter as BTreeMapIter, Range as BTreeMapRange};

use smallvec_wrapper::OneOrMore;
use txn_core::types::{Entry, EntryData, EntryValue};

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

  #[inline]
  #[doc(hidden)]
  #[allow(private_interfaces)]
  pub fn __by_ref(&self) -> &SkipMap<K, Values<V>> {
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
      Some(values) => match values.value().upper_bound(Bound::Included(&version)) {
        None => false,
        Some(ent) => ent.value().is_some(),
      },
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
  Values<V>: Send,
{
  pub fn compact(&self, new_discard_version: u64) {
    match self
      .last_discard_version
      .fetch_update(Ordering::SeqCst, Ordering::Acquire, |val| {
        if val >= new_discard_version {
          None
        } else {
          Some(new_discard_version)
        }
      }) {
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

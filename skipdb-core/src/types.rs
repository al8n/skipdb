use core::sync::atomic::{AtomicU8, Ordering};

use crossbeam_skiplist::{map::Entry as MapEntry, SkipMap};
use txn_core::types::EntryRef;

mod reference;
use either::Either;
pub use reference::*;

const UNINITIALIZED: u8 = 0;
const LOCKED: u8 = 1;
const UNLOCKED: u8 = 2;

#[derive(Debug)]
pub(crate) struct Values<V> {
  pub(crate) op: AtomicU8,
  values: SkipMap<u64, Option<V>>,
}

unsafe impl<V: Send> Send for Values<V> {}

impl<V> Values<V> {
  pub(crate) fn new() -> Self {
    Self {
      op: AtomicU8::new(UNINITIALIZED),
      values: SkipMap::new(),
    }
  }

  pub(crate) fn lock(&self) {
    let mut current = UNLOCKED;
    // Spin lock is ok here because the lock is expected to be held for a very short time.
    // and it is hardly contended.
    loop {
      match self
        .op
        .compare_exchange_weak(current, LOCKED, Ordering::SeqCst, Ordering::Acquire)
      {
        Ok(_) => return,
        Err(old) => {
          // If the current state is uninitialized, we can directly return.
          // as we are based on SkipMap, let it to handle concurrent write is engouth.
          if old == UNINITIALIZED {
            return;
          }

          current = old;
        },
      }
    }
  }

  pub(crate) fn try_lock(&self) -> bool {
    self
      .op
      .compare_exchange(UNLOCKED, LOCKED, Ordering::AcqRel, Ordering::Relaxed)
      .is_ok()
  }

  pub(crate) fn unlock(&self) {
    self.op.store(UNLOCKED, Ordering::Release);
  }
}

impl<V> core::ops::Deref for Values<V> {
  type Target = SkipMap<u64, Option<V>>;

  fn deref(&self) -> &Self::Target {
    &self.values
  }
}

/// A reference to an entry in the write transaction.
pub struct Entry<'a, K, V> {
  ent: MapEntry<'a, u64, Option<V>>,
  key: &'a K,
  version: u64,
}

impl<'a, K, V> Clone for Entry<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
      key: self.key,
    }
  }
}

impl<'a, K, V> Entry<'a, K, V> {
  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> Option<&V> {
    self.ent.value().as_ref()
  }

  /// Get the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K {
    self.key
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// A reference to an entry in the write transaction.
pub struct ValueRef<'a, K, V>(Either<&'a V, Entry<'a, K, V>>);

impl<'a, K, V: core::fmt::Debug> core::fmt::Debug for ValueRef<'a, K, V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    core::ops::Deref::deref(self).fmt(f)
  }
}

impl<'a, K, V: core::fmt::Display> core::fmt::Display for ValueRef<'a, K, V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    core::ops::Deref::deref(self).fmt(f)
  }
}

impl<'a, K, V> Clone for ValueRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<'a, K, V> core::ops::Deref for ValueRef<'a, K, V> {
  type Target = V;

  #[inline]
  fn deref(&self) -> &Self::Target {
    match &self.0 {
      Either::Left(v) => v,
      Either::Right(ent) => ent
        .value()
        .expect("the value of `Entry` in `ValueRef` cannot be `None`"),
    }
  }
}

impl<'a, K, V> ValueRef<'a, K, V> {
  /// Returns `true` if the value was commited.
  #[inline]
  pub const fn is_committed(&self) -> bool {
    matches!(self.0, Either::Right(_))
  }
}

impl<'a, K, V> PartialEq<V> for ValueRef<'a, K, V>
where
  V: PartialEq,
{
  #[inline]
  fn eq(&self, other: &V) -> bool {
    core::ops::Deref::deref(self).eq(other)
  }
}

impl<'a, K, V> PartialEq<&V> for ValueRef<'a, K, V>
where
  V: PartialEq,
{
  #[inline]
  fn eq(&self, other: &&V) -> bool {
    core::ops::Deref::deref(self).eq(other)
  }
}

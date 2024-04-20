use crossbeam_skiplist::{map::Entry as MapEntry, SkipMap};
use mwmr_core::types::EntryRef;

mod reference;
use either::Either;
pub use reference::*;

mod versioned_reference;
pub use versioned_reference::*;

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

use std::sync::Arc;

use super::*;

/// A reference to an entry in the write transaction.
#[derive(Debug)]
pub struct VersionedCommittedRef<'a, K, V> {
  pub(crate) ent: MapEntry<'a, K, Arc<SkipMap<u64, Option<V>>>>,
  pub(crate) version: u64,
}

impl<'a, K, V> Clone for VersionedCommittedRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
    }
  }
}

impl<'a, K, V> VersionedCommittedRef<'a, K, V> {
  /// Get the value of the entry.
  #[inline]
  fn entry(&self) -> Option<Entry<'_, K, V>> {
    let ent = self.ent.value().get(&self.version)?;

    Some(Entry {
      ent,
      key: self.ent.key(),
      version: self.version,
    })
  }

  /// Get the key of the ref.
  #[inline]
  pub fn value(&self) -> Option<ValueRef<'_, K, V>> {
    self.entry().and_then(|ent| {
      if ent.value().is_some() {
        Some(ValueRef(Either::Right(ent)))
      } else {
        None
      }
    })
  }

  /// Get the key of the ref.
  #[inline]
  pub fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

enum VersionedRefKind<'a, K, V> {
  PendingIter {
    version: u64,
    key: &'a K,
    value: Option<&'a V>,
  },
  Pending(EntryRef<'a, K, V>),
  Committed(VersionedCommittedRef<'a, K, V>),
}

impl<'a, K, V> Clone for VersionedRefKind<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    match self {
      Self::Committed(ent) => Self::Committed(ent.clone()),
      Self::Pending(ent) => Self::Pending(*ent),
      Self::PendingIter {
        version,
        key,
        value,
      } => Self::PendingIter {
        version: *version,
        key: *key,
        value: *value,
      },
    }
  }
}

impl<'a, K, V> VersionedRefKind<'a, K, V> {
  #[inline]
  fn key(&self) -> &K {
    match self {
      Self::PendingIter { key, .. } => key,
      Self::Pending(ent) => ent.key(),
      Self::Committed(ent) => ent.key(),
    }
  }

  #[inline]
  fn version(&self) -> u64 {
    match self {
      Self::PendingIter { version, .. } => *version,
      Self::Pending(ent) => ent.version(),
      Self::Committed(ent) => ent.version(),
    }
  }

  #[inline]
  fn value(&self) -> Option<ValueRef<'_, K, V>> {
    match self {
      Self::PendingIter { value, .. } => value.as_ref().map(|value| ValueRef(Either::Left(*value))),
      Self::Pending(ent) => ent.value().map(|v| ValueRef(Either::Left(v))),
      Self::Committed(ent) => ent.value(),
    }
  }

  #[inline]
  fn is_committed(&self) -> bool {
    !matches!(self, Self::Committed(_))
  }
}

/// A reference to an entry in the write transaction.
pub struct VersionedRef<'a, K, V>(VersionedRefKind<'a, K, V>);

impl<'a, K, V> Clone for VersionedRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<'a, K, V> From<(u64, &'a K, Option<&'a V>)> for VersionedRef<'a, K, V> {
  #[inline]
  fn from((version, k, v): (u64, &'a K, Option<&'a V>)) -> Self {
    Self(VersionedRefKind::PendingIter {
      version,
      key: k,
      value: v,
    })
  }
}

impl<'a, K, V> From<EntryRef<'a, K, V>> for VersionedRef<'a, K, V> {
  #[inline]
  fn from(ent: EntryRef<'a, K, V>) -> Self {
    Self(VersionedRefKind::Pending(ent))
  }
}

impl<'a, K, V> From<VersionedCommittedRef<'a, K, V>> for VersionedRef<'a, K, V> {
  #[inline]
  fn from(ent: VersionedCommittedRef<'a, K, V>) -> Self {
    Self(VersionedRefKind::Committed(ent))
  }
}

impl<'a, K, V> VersionedRef<'a, K, V> {
  /// Returns the value of the key.
  #[inline]
  pub fn key(&self) -> &K {
    self.0.key()
  }

  /// Returns the read version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.0.version()
  }

  /// Returns the value of the entry.
  ///
  /// Returns `None` if the entry is removed at the current version.
  #[inline]
  pub fn value(&self) -> Option<ValueRef<'_, K, V>> {
    self.0.value()
  }

  /// Returns `true` if the entry was commited.
  #[inline]
  pub fn is_committed(&self) -> bool {
    self.0.is_committed()
  }
}

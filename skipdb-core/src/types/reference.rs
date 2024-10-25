use super::*;

/// A reference to an entry in the write transaction.
#[derive(Debug)]
pub struct CommittedRef<'a, K, V> {
  pub(crate) ent: MapEntry<'a, K, Values<V>>,
  pub(crate) version: u64,
}

impl<K, V> Clone for CommittedRef<'_, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
    }
  }
}

impl<K, V> CommittedRef<'_, K, V> {
  /// Get the value of the entry.
  #[inline]
  fn entry(&self) -> Entry<'_, K, V> {
    let ent = self.ent.value().get(&self.version).unwrap();

    Entry {
      ent,
      key: self.ent.key(),
      version: self.version,
    }
  }

  /// Get the key of the ref.
  #[inline]
  pub fn value(&self) -> ValueRef<'_, K, V> {
    ValueRef(Either::Right(self.entry()))
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

enum RefKind<'a, K, V> {
  PendingIter {
    version: u64,
    key: &'a K,
    value: &'a V,
  },
  Pending(EntryRef<'a, K, V>),
  Committed(CommittedRef<'a, K, V>),
}

impl<K: core::fmt::Debug, V: core::fmt::Debug> core::fmt::Debug for Ref<'_, K, V> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Ref")
      .field("key", self.0.key())
      .field("version", &self.0.version())
      .field("value", &self.0.value())
      .finish()
  }
}

impl<K, V> Clone for RefKind<'_, K, V> {
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

impl<K, V> RefKind<'_, K, V> {
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
  fn value(&self) -> ValueRef<'_, K, V> {
    match self {
      Self::PendingIter { value, .. } => ValueRef(Either::Left(value)),
      Self::Pending(ent) => ValueRef(Either::Left(
        ent
          .value()
          .expect("value of pending entry cannot be `None`"),
      )),
      Self::Committed(ent) => ValueRef(Either::Right(ent.entry())),
    }
  }

  #[inline]
  fn is_committed(&self) -> bool {
    matches!(self, Self::Committed(_))
  }
}

/// A reference to an entry in the write transaction.
pub struct Ref<'a, K, V>(RefKind<'a, K, V>);

impl<K, V> Clone for Ref<'_, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<'a, K, V> From<(u64, &'a K, &'a V)> for Ref<'a, K, V> {
  #[inline]
  fn from((version, k, v): (u64, &'a K, &'a V)) -> Self {
    Self(RefKind::PendingIter {
      version,
      key: k,
      value: v,
    })
  }
}

impl<'a, K, V> From<EntryRef<'a, K, V>> for Ref<'a, K, V> {
  #[inline]
  fn from(ent: EntryRef<'a, K, V>) -> Self {
    Self(RefKind::Pending(ent))
  }
}

impl<'a, K, V> From<CommittedRef<'a, K, V>> for Ref<'a, K, V> {
  #[inline]
  fn from(ent: CommittedRef<'a, K, V>) -> Self {
    Self(RefKind::Committed(ent))
  }
}

impl<K, V> Ref<'_, K, V> {
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
  #[inline]
  pub fn value(&self) -> ValueRef<'_, K, V> {
    self.0.value()
  }

  /// Returns `true` if the entry was commited.
  #[inline]
  pub fn is_committed(&self) -> bool {
    self.0.is_committed()
  }
}

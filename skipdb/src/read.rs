use super::*;

/// A read only transaction over the [`EquivalentDB`],
pub struct ReadTransaction<K, V, I, C> {
  pub(crate) db: I,
  pub(crate) rtm: Rtm<K, V, C, PendingMap<K, V>>,
}

impl<K, V, I, C> ReadTransaction<K, V, I, C> {
  #[inline]
  pub(super) fn new(db: I, rtm: Rtm<K, V, C, PendingMap<K, V>>) -> Self {
    Self { db, rtm }
  }
}

impl<K, V, I, C> ReadTransaction<K, V, I, C>
where
  K: Ord,
  I: Database<K, V>,
{
  /// Returns the version of the transaction.
  #[inline]
  pub fn version(&self) -> u64 {
    self.rtm.version()
  }

  /// Get a value from the database.
  #[inline]
  pub fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().get(key, version).map(Into::into)
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key<Q>(&self, key: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().contains_key(key, version)
  }

  /// Get all the values in different versions for the given key.
  ///
  /// This function returns an iterator in higher version to lower version order.
  #[inline]
  pub fn get_all_versions<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<AllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().get_all_versions(key, version)
  }

  /// Get all the values in different versions for the given key.
  ///
  /// This function returns an iterator in lower version to higher version order.
  #[inline]
  pub fn get_all_versions_rev<'a, 'b: 'a, Q>(
    &'a self,
    key: &'b Q,
  ) -> Option<RevAllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().get_all_versions_rev(key, version)
  }

  /// Returns an iterator over the entries of the database.
  #[inline]
  pub fn iter(&self) -> Iter<'_, K, V> {
    let version = self.rtm.version();
    self.db.as_inner().iter(version)
  }

  /// Returns a reverse iterator over the entries of the database.
  #[inline]
  pub fn iter_rev(&self) -> RevIter<'_, K, V> {
    let version = self.rtm.version();
    self.db.as_inner().rev_iter(version)
  }

  /// Returns an iterator over the entries (all versions, including removed one) of the database.
  #[inline]
  pub fn iter_all_versions(&self) -> AllVersionsIter<'_, K, V> {
    let version = self.rtm.version();
    self.db.as_inner().iter_all_versions(version)
  }

  /// Returns a reverse iterator over the entries (all versions, including removed one) of the database.
  #[inline]
  pub fn iter_all_versions_rev(&self) -> RevAllVersionsIter<'_, K, V> {
    let version = self.rtm.version();
    self.db.as_inner().rev_iter_all_versions(version)
  }

  /// Returns an iterator over the subset of entries of the database.
  #[inline]
  pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().range(range, version)
  }

  /// Returns an iterator over the subset of entries (all versions, including removed one) of the database.
  #[inline]
  pub fn range_all_versions<Q, R>(&self, range: R) -> AllVersionsRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().range_all_versions(range, version)
  }
}

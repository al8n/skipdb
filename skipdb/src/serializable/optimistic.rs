use skipdb_core::rev_range::WriteTransactionRevRange;
use txn::{error::WtmError, PwmComparableRange};

use std::convert::Infallible;

use super::*;

#[cfg(test)]
mod tests;

/// A optimistic concurrency control transaction over the [`SerializableDb`].
pub struct OptimisticTransaction<K, V> {
  pub(super) db: SerializableDb<K, V>,
  pub(super) wtm: Wtm<K, V, BTreeCm<K>, BTreePwm<K, V>>,
}

impl<K, V> OptimisticTransaction<K, V>
where
  K: CheapClone + Ord,
{
  #[inline]
  pub(super) fn new(db: SerializableDb<K, V>) -> Self {
    let wtm = db.inner.tm.write((), ()).unwrap();
    Self { db, wtm }
  }
}

impl<K, V> OptimisticTransaction<K, V>
where
  K: CheapClone + Ord,
  V: Send + 'static,
{
  /// Commits the transaction, following these steps:
  ///
  /// 1. If there are no writes, return immediately.
  ///
  /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
  ///
  /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
  ///
  /// 4. Batch up all writes, write them to database.
  ///
  /// 5. If callback is provided, Badger will return immediately after checking
  ///    for conflicts. Writes to the database will happen in the background.  If
  ///    there is a conflict, an error will be returned and the callback will not
  ///    run. If there are no conflicts, the callback will be called in the
  ///    background upon successful completion of writes or any error during write.
  #[inline]
  pub fn commit(&mut self) -> Result<(), WtmError<Infallible, Infallible, Infallible>> {
    self.wtm.commit(|ents| {
      self.db.inner.map.apply(ents);
      Ok(())
    })
  }
}

impl<K, V> OptimisticTransaction<K, V>
where
  K: CheapClone + Ord + Send + Sync + 'static,
  V: Send + Sync + 'static,
{
  /// Acts like [`commit`](WriteTransaction::commit), but takes a callback, which gets run via a
  /// thread to avoid blocking this function. Following these steps:
  ///
  /// 1. If there are no writes, return immediately, callback will be invoked.
  ///
  /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
  ///
  /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
  ///
  /// 4. Batch up all writes, write them to database.
  ///
  /// 5. Return immediately after checking for conflicts.
  ///    If there is a conflict, an error will be returned immediately and the callback will not
  ///    run. If there are no conflicts, the callback will be called in the
  ///    background upon successful completion of writes or any error during write.
  #[inline]
  pub fn commit_with_callback<E, R>(
    &mut self,
    callback: impl FnOnce(Result<(), E>) -> R + Send + 'static,
  ) -> Result<std::thread::JoinHandle<R>, WtmError<Infallible, Infallible, E>>
  where
    E: std::error::Error,
    R: Send + 'static,
  {
    let db = self.db.clone();

    self.wtm.commit_with_callback(
      move |ents| {
        db.inner.map.apply(ents);
        Ok(())
      },
      callback,
    )
  }
}

impl<K, V> OptimisticTransaction<K, V>
where
  K: CheapClone + Ord,
{
  /// Returns the read version of the transaction.
  #[inline]
  pub fn version(&self) -> u64 {
    self.wtm.version()
  }

  /// Rollback the transaction.
  #[inline]
  pub fn rollback(&mut self) -> Result<(), TransactionError<Infallible, Infallible>> {
    self.wtm.rollback()
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key(
    &mut self,
    key: &K,
  ) -> Result<bool, TransactionError<Infallible, Infallible>> {
    let version = self.wtm.version();
    match self.wtm.contains_key(key)? {
      Some(true) => Ok(true),
      Some(false) => Ok(false),
      None => Ok(self.db.inner.map.contains_key(key, version)),
    }
  }

  /// Get a value from the database.
  #[inline]
  pub fn get<'a, 'b: 'a>(
    &'a mut self,
    key: &'b K,
  ) -> Result<Option<Ref<'a, K, V>>, TransactionError<Infallible, Infallible>> {
    let version = self.wtm.version();
    match self.wtm.get(key)? {
      Some(v) => {
        if v.value().is_some() {
          Ok(Some(v.into()))
        } else {
          Ok(None)
        }
      }
      None => Ok(self.db.inner.map.get(key, version).map(Into::into)),
    }
  }

  /// Insert a new key-value pair.
  #[inline]
  pub fn insert(
    &mut self,
    key: K,
    value: V,
  ) -> Result<(), TransactionError<Infallible, Infallible>> {
    self.wtm.insert(key, value)
  }

  /// Remove a key.
  #[inline]
  pub fn remove(&mut self, key: K) -> Result<(), TransactionError<Infallible, Infallible>> {
    self.wtm.remove(key)
  }

  /// Iterate over the entries of the write transaction.
  #[inline]
  pub fn iter(
    &mut self,
  ) -> Result<TransactionIter<'_, K, V, BTreeCm<K>>, TransactionError<Infallible, Infallible>> {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
    let committed = self.db.inner.map.iter(version);
    let pendings = pm.iter();

    Ok(TransactionIter::new(pendings, committed, Some(marker)))
  }

  /// Iterate over the entries of the write transaction in reverse order.
  #[inline]
  pub fn iter_rev(
    &mut self,
  ) -> Result<WriteTransactionRevIter<'_, K, V, BTreeCm<K>>, TransactionError<Infallible, Infallible>>
  {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
    let committed = self.db.inner.map.iter_rev(version);
    let pendings = pm.iter().rev();

    Ok(WriteTransactionRevIter::new(
      pendings,
      committed,
      Some(marker),
    ))
  }

  /// Returns an iterator over the subset of entries of the database.
  #[inline]
  pub fn range<'a, Q, R>(
    &'a mut self,
    range: R,
  ) -> Result<TransactionRange<'a, Q, R, K, V, BTreeCm<K>>, TransactionError<Infallible, Infallible>>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q> + 'a,
    Q: Ord + ?Sized,
  {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
    let start = range.start_bound();
    let end = range.end_bound();
    let pendings = pm.range_comparable((start, end));
    let committed = self.db.inner.map.range(range, version);

    Ok(TransactionRange::new(pendings, committed, Some(marker)))
  }

  /// Returns an iterator over the subset of entries of the database.
  #[inline]
  pub fn range_ssi<'a, R>(
    &'a mut self,
    range: R,
  ) -> Result<TransactionRange<'a, K, R, K, V, BTreeCm<K>>, TransactionError<Infallible, Infallible>>
  where
    R: RangeBounds<K> + 'a,
  {
    let version = self.wtm.version();
    let (mut marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
    let start = range.start_bound();
    let end = range.end_bound();
    marker.mark_range((start, end));
    let pendings = pm.range_comparable((start, end));
    let committed = self.db.inner.map.range(range, version);

    Ok(TransactionRange::new(pendings, committed, Some(marker)))
  }

  /// Returns an iterator over the subset of entries of the database in reverse order.
  #[inline]
  pub fn range_rev<'a, Q, R>(
    &'a mut self,
    range: R,
  ) -> Result<
    WriteTransactionRevRange<'a, Q, R, K, V, BTreeCm<K>>,
    TransactionError<Infallible, Infallible>,
  >
  where
    K: Borrow<Q>,
    R: RangeBounds<Q> + 'a,
    Q: Ord + ?Sized,
  {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
    let start = range.start_bound();
    let end = range.end_bound();
    let pendings = pm.range_comparable((start, end)).rev();
    let committed = self.db.inner.map.range_rev(range, version);

    Ok(WriteTransactionRevRange::new(
      pendings,
      committed,
      Some(marker),
    ))
  }
}

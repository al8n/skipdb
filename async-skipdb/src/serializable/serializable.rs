use async_txn::{error::WtmError, PwmComparableRange};
use skipdb_core::rev_range::WriteTransactionRevRange;

use std::{convert::Infallible, future::Future, ops::Bound};

use super::*;

#[cfg(all(test, any(feature = "tokio", feature = "smol", feature = "async-std")))]
mod tests;

/// A serializable snapshot isolation transaction over the [`SerializableDb`].
pub struct SerializableTransaction<K, V, S: AsyncSpawner> {
  pub(super) db: SerializableDb<K, V, S>,
  pub(super) wtm: AsyncWtm<K, V, BTreeCm<K>, BTreePwm<K, V>, S>,
}

impl<K, V, S> SerializableTransaction<K, V, S>
where
  K: CheapClone + Ord,
  S: AsyncSpawner,
{
  #[inline]
  pub(super) async fn new(db: SerializableDb<K, V, S>) -> Self {
    let wtm = db
      .inner
      .tm
      .write_with_blocking_cm_and_pwm((), ())
      .await
      .unwrap();
    Self { db, wtm }
  }
}

impl<K, V, S> SerializableTransaction<K, V, S>
where
  K: CheapClone + Ord + Send + Sync + 'static,
  V: Send + Sync + 'static,
  S: AsyncSpawner,
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
  /// 5. If callback is provided, database will return immediately after checking
  /// for conflicts. Writes to the database will happen in the background.  If
  /// there is a conflict, an error will be returned and the callback will not
  /// run. If there are no conflicts, the callback will be called in the
  /// background upon successful completion of writes or any error during write.
  #[inline]
  pub async fn commit(&mut self) -> Result<(), WtmError<Infallible, Infallible, Infallible>> {
    let db = self.db.clone();
    self
      .wtm
      .commit(|ents| async move {
        db.inner.map.apply(ents);
        Ok(())
      })
      .await
  }
}

impl<K, V, S> SerializableTransaction<K, V, S>
where
  K: CheapClone + Ord + Send + Sync + 'static,
  V: Send + Sync + 'static,
  S: AsyncSpawner,
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
  /// If there is a conflict, an error will be returned immediately and the callback will not
  /// run. If there are no conflicts, the callback will be called in the
  /// background upon successful completion of writes or any error during write.
  #[inline]
  pub async fn commit_with_task<Fut, E, R>(
    &mut self,
    callback: impl FnOnce(Result<(), E>) -> Fut + Send + 'static,
  ) -> Result<S::JoinHandle<R>, WtmError<Infallible, Infallible, E>>
  where
    E: std::error::Error + Send,
    Fut: Future<Output = R> + Send + 'static,
    R: Send + 'static,
  {
    let db = self.db.clone();

    self
      .wtm
      .commit_with_task(
        move |ents| async move {
          db.inner.map.apply(ents);
          Ok(())
        },
        callback,
      )
      .await
  }
}

impl<K, V, S> SerializableTransaction<K, V, S>
where
  K: CheapClone + Ord,
  S: AsyncSpawner,
{
  /// Returns the read version of the transaction.
  #[inline]
  pub fn version(&self) -> u64 {
    self.wtm.version()
  }

  /// Rollback the transaction.
  #[inline]
  pub fn rollback(&mut self) -> Result<(), TransactionError<Infallible, Infallible>> {
    self.wtm.rollback_blocking()
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key(
    &mut self,
    key: &K,
  ) -> Result<bool, TransactionError<Infallible, Infallible>> {
    let version = self.wtm.version();
    match self.wtm.contains_key_blocking(key)? {
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
    match self.wtm.get_blocking(key)? {
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
    self.wtm.insert_blocking(key, value)
  }

  /// Remove a key.
  #[inline]
  pub fn remove(&mut self, key: K) -> Result<(), TransactionError<Infallible, Infallible>> {
    self.wtm.remove_blocking(key)
  }

  /// Iterate over the entries of the write transaction.
  #[inline]
  pub fn iter(
    &mut self,
  ) -> Result<TransactionIter<'_, K, V, BTreeCm<K>>, TransactionError<Infallible, Infallible>> {
    let version = self.wtm.version();
    let (mut marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;

    let start: Bound<K> = Bound::Unbounded;
    let end: Bound<K> = Bound::Unbounded;
    marker.mark_range((start, end));
    let committed = self.db.inner.map.iter(version);
    let pendings = pm.iter();

    Ok(TransactionIter::new(pendings, committed, None))
  }

  /// Iterate over the entries of the write transaction in reverse order.
  #[inline]
  pub fn iter_rev(
    &mut self,
  ) -> Result<WriteTransactionRevIter<'_, K, V, BTreeCm<K>>, TransactionError<Infallible, Infallible>>
  {
    let version = self.wtm.version();
    let (mut marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;

    let start: Bound<K> = Bound::Unbounded;
    let end: Bound<K> = Bound::Unbounded;
    marker.mark_range((start, end));
    let committed = self.db.inner.map.iter_rev(version);
    let pendings = pm.iter().rev();

    Ok(WriteTransactionRevIter::new(pendings, committed, None))
  }

  /// Returns an iterator over the subset of entries of the database.
  #[inline]
  pub fn range<'a, R>(
    &'a mut self,
    range: R,
  ) -> Result<TransactionRange<'a, K, R, K, V, BTreeCm<K>>, TransactionError<Infallible, Infallible>>
  where
    R: RangeBounds<K> + 'a,
  {
    let version = self.wtm.version();
    let (mut marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;
    let start = range.start_bound();
    let end = range.end_bound();
    marker.mark_range((start, end));
    let pendings = pm.range_comparable((start, end));
    let committed = self.db.inner.map.range(range, version);

    Ok(TransactionRange::new(pendings, committed, None))
  }

  /// Returns an iterator over the subset of entries of the database in reverse order.
  #[inline]
  pub fn range_rev<'a, R>(
    &'a mut self,
    range: R,
  ) -> Result<
    WriteTransactionRevRange<'a, K, R, K, V, BTreeCm<K>>,
    TransactionError<Infallible, Infallible>,
  >
  where
    R: RangeBounds<K> + 'a,
  {
    let version = self.wtm.version();
    let (mut marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;
    let start = range.start_bound();
    let end = range.end_bound();
    marker.mark_range((start, end));
    let pendings = pm.range_comparable((start, end)).rev();
    let committed = self.db.inner.map.range_rev(range, version);

    Ok(WriteTransactionRevRange::new(
      pendings,
      committed,
      Some(marker),
    ))
  }
}

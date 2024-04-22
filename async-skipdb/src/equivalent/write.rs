use std::{convert::Infallible, future::Future};

use async_txn::{error::WtmError, PwmComparableRange};
use skipdb_core::rev_range::WriteTransactionRevRange;

use super::*;

/// A read only transaction over the [`EquivalentDB`],
pub struct WriteTransaction<K, V, SP, S = RandomState> {
  db: EquivalentDB<K, V, SP, S>,
  pub(super) wtm: AsyncWtm<K, V, HashCm<K, S>, PendingMap<K, V>, SP>,
}

impl<K, V, SP, S> WriteTransaction<K, V, SP, S>
where
  K: Ord + core::hash::Hash + Eq,
  S: BuildHasher + Clone,
  SP: AsyncSpawner,
{
  #[inline]
  pub(super) async fn new(db: EquivalentDB<K, V, SP, S>, cap: Option<usize>) -> Self {
    let wtm = db
      .inner
      .tm
      .write_with_blocking_cm_and_pwm(
        Options::default()
          .with_max_batch_entries(db.inner.max_batch_entries)
          .with_max_batch_size(db.inner.max_batch_size),
        Some(HashCmOptions::with_capacity(
          db.inner.hasher.clone(),
          cap.unwrap_or(8),
        )),
      )
      .await
      .unwrap();
    Self { db, wtm }
  }
}

impl<K, V, SP, S> WriteTransaction<K, V, SP, S>
where
  K: Ord + core::hash::Hash + Eq + Send + Sync + 'static,
  V: Send + Sync + 'static,
  S: BuildHasher + Send + Sync + 'static,
  SP: AsyncSpawner,
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
  /// for conflicts. Writes to the database will happen in the background.  If
  /// there is a conflict, an error will be returned and the callback will not
  /// run. If there are no conflicts, the callback will be called in the
  /// background upon successful completion of writes or any error during write.
  #[inline]
  pub async fn commit(
    &mut self,
  ) -> Result<(), WtmError<Infallible, Infallible, core::convert::Infallible>> {
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

impl<K, V, SP, S> WriteTransaction<K, V, SP, S>
where
  K: Ord + core::hash::Hash + Eq + Send + Sync + 'static,
  V: Send + Sync + 'static,
  S: BuildHasher + Send + Sync + 'static,
  SP: AsyncSpawner,
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
  ) -> Result<SP::JoinHandle<R>, WtmError<Infallible, Infallible, E>>
  where
    Fut: Future<Output = R> + Send + 'static,
    E: std::error::Error + Send,
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

impl<K, V, SP, S> WriteTransaction<K, V, SP, S>
where
  K: Ord + core::hash::Hash + Eq,
  V: 'static,
  S: BuildHasher,
  SP: AsyncSpawner,
{
  /// Returns the read version of the transaction.
  #[inline]
  pub fn version(&self) -> u64 {
    self.wtm.version()
  }

  /// Rollback the transaction.
  #[inline]
  pub fn rollback_blocking(&mut self) -> Result<(), TransactionError<Infallible, Infallible>> {
    self.wtm.rollback_blocking()
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key<Q>(
    &mut self,
    key: &Q,
  ) -> Result<bool, TransactionError<Infallible, Infallible>>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Eq + Ord + ?Sized + Sync,
  {
    let version = self.wtm.version();
    match self
      .wtm
      .contains_key_equivalent_cm_comparable_pm_blocking(key)?
    {
      Some(true) => Ok(true),
      Some(false) => Ok(false),
      None => Ok(self.db.inner.map.contains_key(key, version)),
    }
  }

  /// Get a value from the database.
  #[inline]
  pub fn get<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<Ref<'a, K, V>>, TransactionError<Infallible, Infallible>>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Eq + Ord + ?Sized + Sync,
  {
    let version = self.wtm.version();
    match self.wtm.get_equivalent_cm_comparable_pm_blocking(key)? {
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
  ) -> Result<WriteTransactionIter<'_, K, V, HashCm<K, S>>, TransactionError<Infallible, Infallible>>
  {
    let version = self.wtm.version();
    let (marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;

    let committed = self.db.inner.map.iter(version);
    let pendings = pm.iter();

    Ok(WriteTransactionIter::new(pendings, committed, Some(marker)))
  }

  /// Iterate over the entries of the write transaction in reverse order.
  #[inline]
  pub fn iter_rev(
    &mut self,
  ) -> Result<
    WriteTransactionRevIter<'_, K, V, HashCm<K, S>>,
    TransactionError<Infallible, Infallible>,
  > {
    let version = self.wtm.version();
    let (marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;

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
  ) -> Result<
    WriteTransactionRange<'a, Q, R, K, V, HashCm<K, S>>,
    TransactionError<Infallible, Infallible>,
  >
  where
    K: Borrow<Q>,
    R: RangeBounds<Q> + 'a,
    Q: Ord + ?Sized,
  {
    let version = self.wtm.version();
    let (marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;
    let start = range.start_bound();
    let end = range.end_bound();
    let pendings = pm.range_comparable((start, end));
    let committed = self.db.inner.map.range(range, version);

    Ok(WriteTransactionRange::new(
      pendings,
      committed,
      Some(marker),
    ))
  }

  /// Returns an iterator over the subset of entries of the database in reverse order.
  #[inline]
  pub fn range_rev<'a, Q, R>(
    &'a mut self,
    range: R,
  ) -> Result<
    WriteTransactionRevRange<'a, Q, R, K, V, HashCm<K, S>>,
    TransactionError<Infallible, Infallible>,
  >
  where
    K: Borrow<Q>,
    R: RangeBounds<Q> + 'a,
    Q: Ord + ?Sized,
  {
    let version = self.wtm.version();
    let (marker, pm) = self
      .wtm
      .blocking_marker_with_pm()
      .ok_or(TransactionError::Discard)?;
    let start = range.start_bound();
    let end = range.end_bound();
    let pendings = pm.range_comparable((start, end));
    let committed = self.db.inner.map.range_rev(range, version);

    Ok(WriteTransactionRevRange::new(
      pendings.rev(),
      committed,
      Some(marker),
    ))
  }
}

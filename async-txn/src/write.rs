use self::error::WtmError;

use core::{borrow::Borrow, future::Future, hash::Hash};

use super::*;

mod blocking;

/// AsyncWtm is used to perform writes to the database. It is created by
/// calling [`AsyncTm::write`].
pub struct AsyncWtm<K, V, C, P, S>
where
  S: AsyncSpawner,
{
  pub(super) read_ts: u64,
  pub(super) size: u64,
  pub(super) count: u64,
  pub(super) orc: Arc<Oracle<C, S>>,
  pub(super) conflict_manager: Option<C>,

  // buffer stores any writes done by txn.
  pub(super) pending_writes: Option<P>,
  // Used in managed mode to store duplicate entries.
  pub(super) duplicate_writes: OneOrMore<Entry<K, V>>,

  pub(super) discarded: bool,
  pub(super) done_read: bool,
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  S: AsyncSpawner,
{
  /// Returns the version of this read transaction.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.read_ts
  }

  /// Sets the current read version of the transaction manager.
  // This should be used only for testing purposes.
  #[doc(hidden)]
  #[inline]
  pub fn __set_read_version(&mut self, version: u64) {
    self.read_ts = version;
  }

  /// Returns the pending writes manager.
  ///
  /// `None` means the transaction has already been discarded.
  #[inline]
  pub fn pwm(&self) -> Option<&P> {
    self.pending_writes.as_ref()
  }

  /// Returns the conflict manager.
  ///
  /// `None` means the transaction has already been discarded.
  #[inline]
  pub fn cm(&self) -> Option<&C> {
    self.conflict_manager.as_ref()
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCm<Key = K>,
  S: AsyncSpawner,
{
  /// This method is used to create a marker for the keys that are operated.
  /// It must be used to mark keys when end user is implementing iterators to
  /// make sure the transaction manager works correctly.
  ///
  /// `None` means the transaction has already been discarded.
  ///
  /// e.g.
  ///
  /// ```ignore, rust
  /// let mut txn = custom_database.write(conflict_manger_opts, pending_manager_opts).unwrap();
  /// let mut marker = txn.marker();
  /// custom_database.iter().map(|k, v| marker.mark(&k));
  /// ```
  pub fn marker(&mut self) -> Option<AsyncMarker<'_, C>> {
    self.conflict_manager.as_mut().map(AsyncMarker::new)
  }

  /// Returns a marker for the keys that are operated and the pending writes manager.
  ///
  /// `None` means the transaction has already been discarded.
  ///
  /// As Rust's borrow checker does not allow to borrow mutable marker and the immutable pending writes manager at the same
  /// time, this method is used to solve this problem.
  pub fn marker_with_pm(&mut self) -> Option<(AsyncMarker<'_, C>, &P)> {
    self.conflict_manager.as_mut().map(|marker| {
      (
        AsyncMarker::new(marker),
        self.pending_writes.as_ref().unwrap(),
      )
    })
  }

  /// Marks a key is read.
  pub async fn mark_read(&mut self, k: &K) {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read(k).await;
    }
  }

  /// Marks a key is conflict.
  pub async fn mark_conflict(&mut self, k: &K) {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict(k).await;
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCm<Key = K>,
  P: AsyncPwm<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Rolls back the transaction.
  #[inline]
  pub async fn rollback(&mut self) -> Result<(), TransactionError<C::Error, P::Error>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    self
      .pending_writes
      .as_mut()
      .unwrap()
      .rollback()
      .await
      .map_err(TransactionError::Pwm)?;
    self
      .conflict_manager
      .as_mut()
      .unwrap()
      .rollback()
      .await
      .map_err(TransactionError::Cm)?;
    Ok(())
  }

  /// Insert a key-value pair to the transaction.
  pub async fn insert(
    &mut self,
    key: K,
    value: V,
  ) -> Result<(), TransactionError<C::Error, P::Error>> {
    self.insert_with_in(key, value).await
  }

  /// Removes a key.
  ///
  /// This is done by adding a delete marker for the key at commit timestamp.  Any
  /// reads happening before this timestamp would be unaffected. Any reads after
  /// this commit would see the deletion.
  pub async fn remove(&mut self, key: K) -> Result<(), TransactionError<C::Error, P::Error>> {
    self
      .modify(Entry {
        data: EntryData::Remove(key),
        version: 0,
      })
      .await
  }

  /// Returns `true` if the pending writes contains the key.
  pub async fn contains_key(
    &mut self,
    key: &K,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get(key)
      .await
      .map_err(TransactionError::pending)?
    {
      Some(ent) => {
        // If the value is None, it means that the key is removed.
        if ent.value.is_none() {
          return Ok(Some(false));
        }

        // Fulfill from buffer.
        Ok(Some(true))
      }
      None => {
        // track reads. No need to track read if txn serviced it
        // internally.
        if let Some(ref mut conflict_manager) = self.conflict_manager {
          conflict_manager.mark_read(key).await;
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub async fn get<'a, 'b: 'a>(
    &'a mut self,
    key: &'b K,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    if let Some(e) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get(key)
      .await
      .map_err(TransactionError::Pwm)?
    {
      // If the value is None, it means that the key is removed.
      if e.value.is_none() {
        return Ok(None);
      }

      // Fulfill from buffer.
      Ok(Some(EntryRef {
        data: match &e.value {
          Some(value) => EntryDataRef::Insert { key, value },
          None => EntryDataRef::Remove(key),
        },
        version: e.version,
      }))
    } else {
      // track reads. No need to track read if txn serviced it
      // internally.
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read(key).await;
      }

      Ok(None)
    }
  }

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
  pub async fn commit<F, Fut, O, E>(
    &mut self,
    apply: F,
  ) -> Result<O, WtmError<C::Error, P::Error, E>>
  where
    Fut: Future<Output = Result<O, E>>,
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Fut,
    E: std::error::Error,
  {
    if self.pending_writes.as_ref().unwrap().is_empty().await {
      // Nothing to commit
      self.discard();
      return apply(Default::default()).await.map_err(WtmError::commit);
    }

    match self.commit_entries().await {
      Ok((commit_ts, entries)) => match apply(entries).await {
        Ok(output) => {
          self.orc.done_commit(commit_ts);
          self.discard();
          Ok(output)
        }
        Err(e) => {
          self.orc.done_commit(commit_ts);
          self.discard();
          Err(WtmError::commit(e))
        }
      },
      Err(e) => {
        self.discard();
        Err(WtmError::transaction(e))
      }
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCmEquivalent<Key = K>,
  P: AsyncPwm<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Marks a key is read.
  pub async fn mark_read_equivalent<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Hash,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read_equivalent(k).await;
    }
  }

  /// Marks a key is conflict.
  pub async fn mark_conflict_equivalent<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Hash,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict_equivalent(k).await;
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCmEquivalent<Key = K>,
  P: AsyncPwmEquivalent<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub async fn contains_key_equivalent<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Hash,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_equivalent(key)
      .await
      .map_err(TransactionError::pending)?
    {
      Some(ent) => {
        // If the value is None, it means that the key is removed.
        if ent.value.is_none() {
          return Ok(Some(false));
        }

        // Fulfill from buffer.
        Ok(Some(true))
      }
      None => {
        // track reads. No need to track read if txn serviced it
        // internally.
        if let Some(ref mut conflict_manager) = self.conflict_manager {
          conflict_manager.mark_read_equivalent(key).await;
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub async fn get_equivalent<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Hash,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    if let Some((k, e)) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_entry_equivalent(key)
      .await
      .map_err(TransactionError::Pwm)?
    {
      // If the value is None, it means that the key is removed.
      if e.value.is_none() {
        return Ok(None);
      }

      // Fulfill from buffer.
      Ok(Some(EntryRef {
        data: match &e.value {
          Some(value) => EntryDataRef::Insert { key: k, value },
          None => EntryDataRef::Remove(k),
        },
        version: e.version,
      }))
    } else {
      // track reads. No need to track read if txn serviced it
      // internally.
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read_equivalent(key).await;
      }

      Ok(None)
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCmComparable<Key = K>,
  P: AsyncPwmEquivalent<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub async fn contains_key_comparable_cm_equivalent_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
  {
    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_equivalent(key)
      .await
      .map_err(TransactionError::pending)?
    {
      Some(ent) => {
        // If the value is None, it means that the key is removed.
        if ent.value.is_none() {
          return Ok(Some(false));
        }

        // Fulfill from buffer.
        Ok(Some(true))
      }
      None => {
        // track reads. No need to track read if txn serviced it
        // internally.
        if let Some(ref mut conflict_manager) = self.conflict_manager {
          conflict_manager.mark_read_comparable(key).await;
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub async fn get_comparable_cm_equivalent_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
  {
    if let Some((k, e)) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_entry_equivalent(key)
      .await
      .map_err(TransactionError::Pwm)?
    {
      // If the value is None, it means that the key is removed.
      if e.value.is_none() {
        return Ok(None);
      }

      // Fulfill from buffer.
      Ok(Some(EntryRef {
        data: match &e.value {
          Some(value) => EntryDataRef::Insert { key: k, value },
          None => EntryDataRef::Remove(k),
        },
        version: e.version,
      }))
    } else {
      // track reads. No need to track read if txn serviced it
      // internally.
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read_comparable(key).await;
      }

      Ok(None)
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCmComparable<Key = K>,
  S: AsyncSpawner,
{
  /// Marks a key is read.
  pub async fn mark_read_comparable<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read_comparable(k).await;
    }
  }

  /// Marks a key is conflict.
  pub async fn mark_conflict_comparable<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict_comparable(k).await;
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCmComparable<Key = K>,
  P: AsyncPwmComparable<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub async fn contains_key_comparable<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_comparable(key)
      .await
      .map_err(TransactionError::pending)?
    {
      Some(ent) => {
        // If the value is None, it means that the key is removed.
        if ent.value.is_none() {
          return Ok(Some(false));
        }

        // Fulfill from buffer.
        Ok(Some(true))
      }
      None => {
        // track reads. No need to track read if txn serviced it
        // internally.
        if let Some(ref mut conflict_manager) = self.conflict_manager {
          conflict_manager.mark_read_comparable(key).await;
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub async fn get_comparable<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if let Some((k, e)) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_entry_comparable(key)
      .await
      .map_err(TransactionError::Pwm)?
    {
      // If the value is None, it means that the key is removed.
      if e.value.is_none() {
        return Ok(None);
      }

      // Fulfill from buffer.
      Ok(Some(EntryRef {
        data: match &e.value {
          Some(value) => EntryDataRef::Insert { key: k, value },
          None => EntryDataRef::Remove(k),
        },
        version: e.version,
      }))
    } else {
      // track reads. No need to track read if txn serviced it
      // internally.
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read_comparable(key).await;
      }

      Ok(None)
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCmEquivalent<Key = K>,
  P: AsyncPwmComparable<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub async fn contains_key_equivalent_cm_comparable_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
  {
    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_comparable(key)
      .await
      .map_err(TransactionError::pending)?
    {
      Some(ent) => {
        // If the value is None, it means that the key is removed.
        if ent.value.is_none() {
          return Ok(Some(false));
        }

        // Fulfill from buffer.
        Ok(Some(true))
      }
      None => {
        // track reads. No need to track read if txn serviced it
        // internally.
        if let Some(ref mut conflict_manager) = self.conflict_manager {
          conflict_manager.mark_read_equivalent(key).await;
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub async fn get_equivalent_cm_comparable_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
  {
    if let Some((k, e)) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_entry_comparable(key)
      .await
      .map_err(TransactionError::Pwm)?
    {
      // If the value is None, it means that the key is removed.
      if e.value.is_none() {
        return Ok(None);
      }

      // Fulfill from buffer.
      Ok(Some(EntryRef {
        data: match &e.value {
          Some(value) => EntryDataRef::Insert { key: k, value },
          None => EntryDataRef::Remove(k),
        },
        version: e.version,
      }))
    } else {
      // track reads. No need to track read if txn serviced it
      // internally.
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read_equivalent(key).await;
      }

      Ok(None)
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCm<Key = K> + Send,
  P: AsyncPwm<Key = K, Value = V> + Send,
  S: AsyncSpawner,
{
  /// Acts like [`commit`](AsyncWtm::commit), but takes a future and a spawner, which gets run via a
  /// task to avoid blocking this function. Following these steps:
  ///
  /// 1. If there are no writes, return immediately, a new task will be spawned, and future will be invoked.
  ///
  /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
  ///
  /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
  ///
  /// 4. Batch up all writes, write them to database.
  ///
  /// 5. Return immediately after checking for conflicts.
  ///    If there is a conflict, an error will be returned immediately and the no task will be spawned
  ///    run. If there are no conflicts, a task will be spawned and the future will be called in the
  ///    background upon successful completion of writes or any error during write.
  pub async fn commit_with_task<F, Fut, CFut, E, R>(
    &mut self,
    apply: F,
    fut: impl FnOnce(Result<(), E>) -> CFut + Send + 'static,
  ) -> Result<<S as AsyncSpawner>::JoinHandle<R>, WtmError<C::Error, P::Error, E>>
  where
    K: Send + 'static,
    V: Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send,
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Fut + Send + 'static,
    CFut: Future<Output = R> + Send + 'static,
    E: std::error::Error + Send,
    C: 'static,
    R: Send + 'static,
  {
    if self.discarded {
      return Err(WtmError::transaction(TransactionError::Discard));
    }

    if self.pending_writes.as_ref().unwrap().is_empty().await {
      // Nothing to commit
      self.discard();
      return Ok(S::spawn(async move { fut(Ok(())).await }));
    }

    match self.commit_entries().await {
      Ok((commit_ts, entries)) => {
        let orc = self.orc.clone();
        Ok(S::spawn(async move {
          match apply(entries).await {
            Ok(_) => {
              orc.done_commit(commit_ts);
              fut(Ok(())).await
            }
            Err(e) => {
              orc.done_commit(commit_ts);
              fut(Err(e)).await
            }
          }
        }))
      }
      Err(e) => match e {
        TransactionError::Conflict => Err(WtmError::transaction(e)),
        _ => {
          self.discard();
          Err(WtmError::transaction(e))
        }
      },
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: AsyncCm<Key = K>,
  P: AsyncPwm<Key = K, Value = V>,
  S: AsyncSpawner,
{
  async fn insert_with_in(
    &mut self,
    key: K,
    value: V,
  ) -> Result<(), TransactionError<C::Error, P::Error>> {
    let ent = Entry {
      data: EntryData::Insert { key, value },
      version: self.read_ts,
    };

    self.modify(ent).await
  }

  async fn modify(&mut self, ent: Entry<K, V>) -> Result<(), TransactionError<C::Error, P::Error>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    let pending_writes = self.pending_writes.as_mut().unwrap();
    pending_writes
      .validate_entry(&ent)
      .await
      .map_err(TransactionError::Pwm)?;

    let cnt = self.count + 1;
    // Extra bytes for the version in key.
    let size = self.size + pending_writes.estimate_size(&ent);
    if cnt >= pending_writes.max_batch_entries() || size >= pending_writes.max_batch_size() {
      return Err(TransactionError::LargeTxn);
    }

    self.count = cnt;
    self.size = size;

    // The conflict_manager is used for conflict detection. If conflict detection
    // is disabled, we don't need to store key hashes in the conflict_manager.
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict(ent.key()).await;
    }

    // If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
    // Add the entry to duplicateWrites only if both the entries have different versions. For
    // same versions, we will overwrite the existing entry.
    let eversion = ent.version;
    let (ek, ev) = ent.split();

    if let Some((old_key, old_value)) = pending_writes
      .remove_entry(&ek)
      .await
      .map_err(TransactionError::Pwm)?
    {
      if old_value.version != eversion {
        self
          .duplicate_writes
          .push(Entry::unsplit(old_key, old_value));
      }
    }
    pending_writes
      .insert(ek, ev)
      .await
      .map_err(TransactionError::Pwm)?;

    Ok(())
  }

  async fn commit_entries(
    &mut self,
  ) -> Result<(u64, OneOrMore<Entry<K, V>>), TransactionError<C::Error, P::Error>> {
    // Ensure that the order in which we get the commit timestamp is the same as
    // the order in which we push these updates to the write channel. So, we
    // acquire a writeChLock before getting a commit timestamp, and only release
    // it after pushing the entries to it.
    let _write_lock = self.orc.write_serialize_lock.lock().await;

    let conflict_manager = if self.conflict_manager.is_none() {
      None
    } else {
      mem::take(&mut self.conflict_manager)
    };

    match self
      .orc
      .new_commit_ts(&mut self.done_read, self.read_ts, conflict_manager)
      .await
    {
      CreateCommitTimestampResult::Conflict(conflict_manager) => {
        // If there is a conflict, we should not send the updates to the write channel.
        // Instead, we should return the conflict error to the user.
        self.conflict_manager = conflict_manager;
        Err(TransactionError::Conflict)
      }
      CreateCommitTimestampResult::Timestamp(commit_ts) => {
        let pending_writes = mem::take(&mut self.pending_writes).unwrap();
        let duplicate_writes = mem::take(&mut self.duplicate_writes);
        let mut entries =
          OneOrMore::with_capacity(pending_writes.len().await + self.duplicate_writes.len());

        let process_entry = |entries: &mut OneOrMore<Entry<K, V>>, mut ent: Entry<K, V>| {
          ent.version = commit_ts;
          entries.push(ent);
        };
        pending_writes
          .into_iter()
          .await
          .for_each(|(k, v)| process_entry(&mut entries, Entry::unsplit(k, v)));
        duplicate_writes
          .into_iter()
          .for_each(|ent| process_entry(&mut entries, ent));

        // CommitTs should not be zero if we're inserting transaction markers.
        assert_ne!(commit_ts, 0);

        Ok((commit_ts, entries))
      }
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  S: AsyncSpawner,
{
  fn done_read(&mut self) {
    if !self.done_read {
      self.done_read = true;
      self.orc().read_mark.done(self.read_ts).unwrap();
    }
  }

  #[inline]
  fn orc(&self) -> &Oracle<C, S> {
    &self.orc
  }

  /// Discards a created transaction. This method is very important and must be called. `commit*`
  /// methods calls this internally.
  ///
  /// NOTE: If any operations are run on a discarded transaction, [`TransactionError::Discard`] is returned.
  pub fn discard(&mut self) {
    if self.discarded {
      return;
    }
    self.discarded = true;
    self.done_read();
  }

  /// Returns true if the transaction is discarded.
  #[inline]
  pub const fn is_discard(&self) -> bool {
    self.discarded
  }
}

impl<K, V, C, P, S> Drop for AsyncWtm<K, V, C, P, S>
where
  S: AsyncSpawner,
{
  fn drop(&mut self) {
    if !self.discarded {
      self.discard();
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{collections::BTreeSet, convert::Infallible, marker::PhantomData};

  use super::*;

  #[async_std::test]
  async fn wtm() {
    let tm = AsyncTm::<String, u64, HashCm<String>, IndexMapPwm<String, u64>, wmark::AsyncStdSpawner>::new("test", 0).await;
    let mut wtm = tm
      .write(Default::default(), Default::default())
      .await
      .unwrap();
    assert!(!wtm.is_discard());
    assert!(wtm.pwm().is_some());
    assert!(wtm.cm().is_some());

    let mut marker = wtm.marker().unwrap();

    marker.mark(&"1".to_owned()).await;
    marker.mark_equivalent("3").await;
    marker.mark_conflict(&"2".to_owned()).await;
    marker.mark_conflict_equivalent("4").await;
    wtm.mark_read(&"2".to_owned()).await;
    wtm.mark_conflict(&"1".to_owned()).await;
    wtm.mark_conflict_equivalent("2").await;
    wtm.mark_read_equivalent("3").await;

    wtm.insert("5".into(), 5).await.unwrap();

    assert_eq!(wtm.contains_key_equivalent("5").await.unwrap(), Some(true));
    assert_eq!(
      wtm
        .get_equivalent("5")
        .await
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    assert_eq!(wtm.contains_key(&"5".to_owned()).await.unwrap(), Some(true));
    assert_eq!(
      wtm
        .get(&"5".to_owned())
        .await
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    assert_eq!(wtm.contains_key_equivalent("6").await.unwrap(), None);
    assert_eq!(wtm.get_equivalent("6").await.unwrap(), None);
    assert_eq!(wtm.contains_key_blocking(&"6".to_owned()).unwrap(), None);

    wtm.remove("5".into()).await.unwrap();
    wtm.rollback().await.unwrap();

    wtm
      .commit::<_, _, _, Infallible>(|_| async { Ok(()) })
      .await
      .unwrap();

    assert!(wtm.is_discard());
  }

  #[async_std::test]
  async fn wtm2() {
    let tm =
      AsyncTm::<String, u64, HashCm<String>, BTreePwm<String, u64>, wmark::AsyncStdSpawner>::new(
        "test", 0,
      )
      .await;
    let mut wtm = tm.write((), Default::default()).await.unwrap();
    assert!(!wtm.is_discard());
    assert!(wtm.pwm().is_some());
    assert!(wtm.cm().is_some());
    assert!(wtm.blocking_marker().is_some());
    assert!(wtm.marker_with_pm().is_some());

    let mut marker = wtm.marker().unwrap();

    marker.mark(&"1".to_owned()).await;
    marker.mark_blocking(&"3".to_owned());
    marker.mark_equivalent("3").await;
    marker.mark_equivalent_blocking("3");
    marker.mark_conflict(&"2".to_owned()).await;
    marker.mark_conflict_equivalent_blocking("4");
    marker.mark_conflict_equivalent("4").await;
    wtm.mark_read(&"2".to_owned()).await;
    wtm.mark_read_blocking(&"3".to_owned());
    wtm.mark_read_equivalent_blocking("3");
    wtm.mark_conflict(&"1".to_owned()).await;
    wtm.mark_conflict_equivalent("2").await;
    wtm.mark_conflict_equivalent_blocking("2");
    wtm.mark_read_equivalent("3").await;

    wtm.insert("5".into(), 5).await.unwrap();

    assert_eq!(
      wtm
        .contains_key_equivalent_cm_comparable_pm("5")
        .await
        .unwrap(),
      Some(true)
    );
    assert_eq!(
      wtm
        .get_equivalent_cm_comparable_pm("5")
        .await
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    assert_eq!(wtm.contains_key(&"5".to_owned()).await.unwrap(), Some(true));
    assert_eq!(
      wtm
        .get(&"5".to_owned())
        .await
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    assert_eq!(
      wtm
        .contains_key_equivalent_cm_comparable_pm("6")
        .await
        .unwrap(),
      None
    );
    assert_eq!(
      wtm.get_equivalent_cm_comparable_pm("6").await.unwrap(),
      None
    );
    assert_eq!(wtm.contains_key(&"6".to_owned()).await.unwrap(), None);
    assert_eq!(wtm.get(&"6".to_owned()).await.unwrap(), None);

    wtm.remove("5".into()).await.unwrap();
    wtm.rollback().await.unwrap();

    wtm
      .commit::<_, _, _, Infallible>(|_| async { Ok(()) })
      .await
      .unwrap();

    assert!(wtm.is_discard());
  }

  struct TestCm<K> {
    conflict_keys: BTreeSet<usize>,
    reads: BTreeSet<usize>,
    _m: PhantomData<K>,
  }

  impl<K> Cm for TestCm<K> {
    type Error = Infallible;

    type Key = K;

    type Options = ();

    fn new(_options: Self::Options) -> Result<Self, Self::Error> {
      Ok(Self {
        conflict_keys: BTreeSet::new(),
        reads: BTreeSet::new(),
        _m: PhantomData,
      })
    }

    fn mark_read(&mut self, key: &Self::Key) {
      self.reads.insert(key as *const K as usize);
    }

    fn mark_conflict(&mut self, key: &Self::Key) {
      self.conflict_keys.insert(key as *const K as usize);
    }

    fn has_conflict(&self, other: &Self) -> bool {
      if self.reads.is_empty() {
        return false;
      }

      for ro in self.reads.iter() {
        if other.conflict_keys.contains(ro) {
          return true;
        }
      }
      false
    }

    fn rollback(&mut self) -> Result<(), Self::Error> {
      self.conflict_keys.clear();
      self.reads.clear();
      Ok(())
    }
  }

  impl<K> CmComparable for TestCm<K> {
    fn mark_read_comparable<Q>(&mut self, key: &Q)
    where
      Self::Key: Borrow<Q>,
      Q: Ord + ?Sized,
    {
      self.reads.insert(key as *const Q as *const () as usize);
    }

    fn mark_conflict_comparable<Q>(&mut self, key: &Q)
    where
      Self::Key: Borrow<Q>,
      Q: Ord + ?Sized,
    {
      self
        .conflict_keys
        .insert(key as *const Q as *const () as usize);
    }
  }

  #[async_std::test]
  async fn wtm3() {
    let tm = AsyncTm::<
      Arc<u64>,
      u64,
      TestCm<Arc<u64>>,
      IndexMapPwm<Arc<u64>, u64>,
      wmark::AsyncStdSpawner,
    >::new("test", 0)
    .await;
    let mut wtm = tm.write(Default::default(), ()).await.unwrap();
    assert!(!wtm.is_discard());
    assert!(wtm.pwm().is_some());
    assert!(wtm.cm().is_some());

    let mut marker = wtm.marker().unwrap();

    let one = Arc::new(1);
    let two = Arc::new(2);
    let three = Arc::new(3);
    let four = Arc::new(4);
    let five = Arc::new(5);
    marker.mark(&one).await;
    marker.mark_comparable(&three).await;
    marker.mark_conflict(&two).await;
    marker.mark_conflict_comparable(&four).await;
    wtm.mark_read(&two).await;
    wtm.mark_conflict(&one).await;
    wtm.mark_conflict_comparable(&two).await;
    wtm.mark_read_comparable(&three).await;

    wtm.insert(five.clone(), 5).await.unwrap();

    assert_eq!(
      wtm
        .contains_key_comparable_cm_equivalent_pm(&five)
        .await
        .unwrap(),
      Some(true)
    );
    assert_eq!(
      wtm
        .get_comparable_cm_equivalent_pm(&five)
        .await
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    assert_eq!(
      wtm
        .contains_key_comparable_cm_equivalent_pm_blocking(&five)
        .unwrap(),
      Some(true)
    );
    assert_eq!(
      wtm
        .get_comparable_cm_equivalent_pm_blocking(&five)
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    let six = Arc::new(6);

    assert_eq!(
      wtm
        .contains_key_comparable_cm_equivalent_pm(&six)
        .await
        .unwrap(),
      None
    );
    assert_eq!(
      wtm.get_comparable_cm_equivalent_pm(&six).await.unwrap(),
      None
    );
    assert_eq!(
      wtm
        .contains_key_comparable_cm_equivalent_pm_blocking(&six)
        .unwrap(),
      None
    );
    assert_eq!(
      wtm.get_comparable_cm_equivalent_pm_blocking(&six).unwrap(),
      None
    );
  }

  #[async_std::test]
  async fn wtm4() {
    let tm = AsyncTm::<
      Arc<u64>,
      u64,
      TestCm<Arc<u64>>,
      BTreePwm<Arc<u64>, u64>,
      wmark::AsyncStdSpawner,
    >::new("test", 0)
    .await;
    let mut wtm = tm.write((), ()).await.unwrap();
    assert!(!wtm.is_discard());
    assert!(wtm.pwm().is_some());
    assert!(wtm.cm().is_some());

    let mut marker = wtm.marker().unwrap();

    let one = Arc::new(1);
    let two = Arc::new(2);
    let three = Arc::new(3);
    let four = Arc::new(4);
    let five = Arc::new(5);
    marker.mark(&one).await;
    marker.mark_blocking(&one);
    marker.mark_comparable(&three).await;
    marker.mark_comparable_blocking(&three);
    marker.mark_conflict(&two).await;
    marker.mark_conflict_blocking(&two);
    marker.mark_conflict_comparable(&four).await;
    marker.mark_conflict_comparable_blocking(&four);
    wtm.mark_read(&two).await;
    wtm.mark_read_blocking(&two);
    wtm.mark_read_comparable_blocking(&two);
    wtm.mark_conflict(&one).await;
    wtm.mark_conflict_blocking(&one);
    wtm.mark_conflict_comparable(&two).await;
    wtm.mark_conflict_comparable_blocking(&two);
    wtm.mark_read_comparable(&three).await;

    wtm.insert(five.clone(), 5).await.unwrap();

    assert_eq!(
      wtm.contains_key_comparable(&five).await.unwrap(),
      Some(true)
    );
    assert_eq!(
      wtm
        .get_comparable(&five)
        .await
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    assert_eq!(
      wtm.contains_key_comparable_blocking(&five).unwrap(),
      Some(true)
    );
    assert_eq!(
      wtm
        .get_comparable_blocking(&five)
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    let six = Arc::new(6);

    assert_eq!(wtm.contains_key_comparable(&six).await.unwrap(), None);
    assert_eq!(wtm.get_comparable(&six).await.unwrap(), None);
    assert_eq!(wtm.contains_key_comparable_blocking(&six).unwrap(), None);
    assert_eq!(wtm.get_comparable_blocking(&six).unwrap(), None);
  }
}

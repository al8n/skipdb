use self::error::WtmError;

use core::borrow::Borrow;

use super::*;

/// A marker used to mark the keys that are read.
pub struct Marker<'a, C> {
  marker: &'a mut C,
}

impl<'a, C: Cm> Marker<'a, C> {
  /// Marks a key is operated.
  pub fn mark(&mut self, k: &C::Key) {
    self.marker.mark_read(k);
  }
}

/// Wtm is used to perform writes to the database. It is created by
/// calling [`Tm::write`].
pub struct Wtm<K, V, C, P> {
  pub(super) read_ts: u64,
  pub(super) size: u64,
  pub(super) count: u64,
  pub(super) orc: Arc<Oracle<C>>,

  // // contains fingerprints of keys read.
  // pub(super) reads: MediumVec<u64>,
  // // contains fingerprints of keys written. This is used for conflict detection.
  // pub(super) conflict_keys: Option<IndexSet<u64, S>>,
  pub(super) conflict_manager: Option<C>,

  // buffer stores any writes done by txn.
  pub(super) pending_writes: Option<P>,
  // Used in managed mode to store duplicate entries.
  pub(super) duplicate_writes: OneOrMore<Entry<K, V>>,

  pub(super) discarded: bool,
  pub(super) done_read: bool,
}

impl<K, V, C, P> Drop for Wtm<K, V, C, P> {
  fn drop(&mut self) {
    if !self.discarded {
      self.discard();
    }
  }
}

impl<K, V, C, P> Wtm<K, V, C, P> {
  /// Returns the version of this read transaction.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.read_ts
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  /// Returns the pending writes manager.
  #[inline]
  pub fn pwm(&self) -> Result<&P, TransactionError<C, P>> {
    self
      .pending_writes
      .as_ref()
      .ok_or(TransactionError::Discard)
  }

  /// Returns the conflict manager.
  #[inline]
  pub fn cm(&self) -> Result<&C, TransactionError<C, P>> {
    self
      .conflict_manager
      .as_ref()
      .ok_or(TransactionError::Discard)
  }

  /// Insert a key-value pair to the transaction.
  pub fn insert(&mut self, key: K, value: V) -> Result<(), TransactionError<C, P>> {
    self.insert_with_in(key, value)
  }

  /// Removes a key.
  ///
  /// This is done by adding a delete marker for the key at commit timestamp.  Any
  /// reads happening before this timestamp would be unaffected. Any reads after
  /// this commit would see the deletion.
  pub fn remove(&mut self, key: K) -> Result<(), TransactionError<C, P>> {
    self.modify(Entry {
      data: EntryData::Remove(key),
      version: 0,
    })
  }

  /// Marks a key is read.
  pub fn mark_read(&mut self, k: &K) {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read(k);
    }
  }

  /// Marks a key is conflict.
  pub fn mark_conflict(&mut self, k: &K) {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict(k);
    }
  }

  /// Returns `true` if the pending writes contains the key.
  pub fn contains_key(&mut self, key: &K) -> Result<bool, TransactionError<C, P>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    let has = self
      .pending_writes
      .as_ref()
      .unwrap()
      .contains_key(key)
      .map_err(TransactionError::Pwm)?;

    if !has {
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read(key);
      }
    }
    Ok(has)
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get<'a, 'b: 'a>(
    &'a mut self,
    key: &'b K,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C, P>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    if let Some(e) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get(key)
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
        conflict_manager.mark_read(key);
      }

      Ok(None)
    }
  }

  /// This method is used to create a marker for the keys that are operated.
  /// It must be used to mark keys when end user is implementing iterators to
  /// make sure the transaction manager works correctly.
  ///
  /// e.g.
  ///
  /// ```no_compile, rust
  /// let mut txn = custom_database.write(conflict_manger_opts, pending_manager_opts).unwrap();
  /// let mut marker = txn.marker();
  /// custom_database.iter().map(|k, v| marker.mark(&k));
  /// ```
  pub fn marker(&mut self) -> Option<Marker<'_, C>> {
    self
      .conflict_manager
      .as_mut()
      .map(|marker| Marker { marker })
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
  /// for conflicts. Writes to the database will happen in the background.  If
  /// there is a conflict, an error will be returned and the callback will not
  /// run. If there are no conflicts, the callback will be called in the
  /// background upon successful completion of writes or any error during write.
  ///
  /// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
  /// tree won't be updated, so there's no need for any rollback.
  pub fn commit<F, E>(&mut self, apply: F) -> Result<(), WtmError<C, P, E>>
  where
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Result<(), E>,
    E: std::error::Error,
  {
    if self.discarded {
      return Err(TransactionError::Discard.into());
    }

    if self.pending_writes.as_ref().unwrap().is_empty() {
      // Nothing to commit
      self.discard();
      return Ok(());
    }

    let (commit_ts, entries) = self.commit_entries().map_err(|e| match e {
      TransactionError::Conflict => e,
      _ => {
        self.discard();
        e
      }
    })?;

    apply(entries)
      .map(|_| {
        self.orc().done_commit(commit_ts);
        self.discard();
      })
      .map_err(|e| {
        self.orc().done_commit(commit_ts);
        self.discard();
        WtmError::commit(e)
      })
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: CmEquivalent<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  /// Marks a key is read.
  pub fn mark_read_equivalent<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + core::hash::Hash,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read_equivalent(k);
    }
  }

  /// Marks a key is conflict.
  pub fn mark_conflict_equivalent<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + core::hash::Hash,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict_equivalent(k);
    }
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: CmEquivalent<Key = K>,
  P: PwmEquivalent<Key = K, Value = V>,
{
  /// Returns `true` if the pending writes contains the key.
  pub fn contains_key_equivalent<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<bool, TransactionError<C, P>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + core::hash::Hash,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    let has = self
      .pending_writes
      .as_ref()
      .unwrap()
      .contains_key_equivalent(key)
      .map_err(TransactionError::Pwm)?;

    if !has {
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read_equivalent(key);
      }
    }
    Ok(has)
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get_equivalent<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C, P>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + core::hash::Hash,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    if let Some((k, e)) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_entry_equivalent(key)
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
        conflict_manager.mark_read_equivalent(key);
      }

      Ok(None)
    }
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: CmComparable<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  /// Marks a key is read.
  pub fn mark_read_comparable<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read_comparable(k);
    }
  }

  /// Marks a key is conflict.
  pub fn mark_conflict_comparable<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict_comparable(k);
    }
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: CmComparable<Key = K>,
  P: PwmComparable<Key = K, Value = V>,
{
  /// Returns `true` if the pending writes contains the key.
  pub fn contains_key_comparable<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<bool, TransactionError<C, P>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    let has = self
      .pending_writes
      .as_ref()
      .unwrap()
      .contains_key_comparable(key)
      .map_err(TransactionError::Pwm)?;

    if !has {
      if let Some(ref mut conflict_manager) = self.conflict_manager {
        conflict_manager.mark_read_comparable(key);
      }
    }
    Ok(has)
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get_comparable<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C, P>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    if let Some((k, e)) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_entry_comparable(key)
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
        conflict_manager.mark_read_comparable(key);
      }

      Ok(None)
    }
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K> + Send,
  P: Pwm<Key = K, Value = V> + Send,
{
  /// Acts like [`commit`](Wtm::commit), but takes a future and a spawner, which gets run via a
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
  /// If there is a conflict, an error will be returned immediately and the no task will be spawned
  /// run. If there are no conflicts, a task will be spawned and the future will be called in the
  /// background upon successful completion of writes or any error during write.
  ///
  /// If error does not occur, the transaction is successfully committed. In case of an error, the DB
  /// should not be updated (The implementors of [`Database`] must promise this), so there's no need for any rollback.
  pub fn commit_with_task<F, E, R, S, JH>(
    &mut self,
    apply: F,
    fut: impl FnOnce(Result<(), E>) -> R + Send + 'static,
    spawner: S,
  ) -> Result<JH, WtmError<C, P, E>>
  where
    K: Send + 'static,
    V: Send + 'static,
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Result<(), E> + Send + 'static,
    E: std::error::Error,
    R: Send + 'static,
    S: FnOnce(core::pin::Pin<Box<dyn core::future::Future<Output = R> + Send>>) -> JH,
  {
    if self.discarded {
      return Err(TransactionError::Discard.into());
    }

    if self.pending_writes.as_ref().unwrap().is_empty() {
      // Nothing to commit
      self.discard();
      return Ok(spawner(Box::pin(async move { fut(Ok(())) })));
    }

    let (commit_ts, entries) = self.commit_entries().map_err(|e| match e {
      TransactionError::Conflict => e,
      _ => {
        self.discard();
        e
      }
    })?;

    let orc = self.orc.clone();
    Ok(spawner(Box::pin(async move {
      fut(
        apply(entries)
          .map(|_| {
            orc.done_commit(commit_ts);
          })
          .map_err(|e| {
            orc.done_commit(commit_ts);
            e
          }),
      )
    })))
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K> + Send,
  P: Pwm<Key = K, Value = V> + Send,
{
  /// Acts like [`commit`](Wtm::commit), but takes a callback, which gets run via a
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
  ///
  /// If error does not occur, the transaction is successfully committed. In case of an error, the DB
  /// should not be updated (The implementors of [`Database`] must promise this), so there's no need for any rollback.
  pub fn commit_with_callback<F, E, R>(
    &mut self,
    apply: F,
    callback: impl FnOnce(Result<(), E>) -> R + Send + 'static,
  ) -> Result<std::thread::JoinHandle<R>, WtmError<C, P, E>>
  where
    K: Send + 'static,
    V: Send + 'static,
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Result<(), E> + Send + 'static,
    E: std::error::Error,
    R: Send + 'static,
  {
    if self.discarded {
      return Err(WtmError::transaction(TransactionError::Discard));
    }

    if self.pending_writes.as_ref().unwrap().is_empty() {
      // Nothing to commit
      self.discard();
      return Ok(std::thread::spawn(move || callback(Ok(()))));
    }

    let (commit_ts, entries) = self.commit_entries().map_err(|e| match e {
      TransactionError::Conflict => e,
      _ => {
        self.discard();
        e
      }
    })?;

    let orc = self.orc.clone();

    Ok(std::thread::spawn(move || {
      callback(
        apply(entries)
          .map(|_| {
            orc.done_commit(commit_ts);
          })
          .map_err(|e| {
            orc.done_commit(commit_ts);
            e
          }),
      )
    }))
  }
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  fn insert_with_in(&mut self, key: K, value: V) -> Result<(), TransactionError<C, P>> {
    let ent = Entry {
      data: EntryData::Insert { key, value },
      version: self.read_ts,
    };

    self.modify(ent)
  }

  fn modify(&mut self, ent: Entry<K, V>) -> Result<(), TransactionError<C, P>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    let pending_writes = self.pending_writes.as_mut().unwrap();
    pending_writes
      .validate_entry(&ent)
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
      conflict_manager.mark_conflict(ent.key());
    }

    // If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
    // Add the entry to duplicateWrites only if both the entries have different versions. For
    // same versions, we will overwrite the existing entry.
    let eversion = ent.version;
    let (ek, ev) = ent.split();

    if let Some((old_key, old_value)) = pending_writes
      .remove_entry(&ek)
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
      .map_err(TransactionError::Pwm)?;

    Ok(())
  }

  fn commit_entries(&mut self) -> Result<(u64, OneOrMore<Entry<K, V>>), TransactionError<C, P>> {
    // Ensure that the order in which we get the commit timestamp is the same as
    // the order in which we push these updates to the write channel. So, we
    // acquire a writeChLock before getting a commit timestamp, and only release
    // it after pushing the entries to it.
    let _write_lock = self.orc.write_serialize_lock.lock();

    let conflict_manager = if self.conflict_manager.is_none() {
      None
    } else {
      mem::take(&mut self.conflict_manager)
    };

    match self
      .orc
      .new_commit_ts(&mut self.done_read, self.read_ts, conflict_manager)
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
        let entries = RefCell::new(OneOrMore::with_capacity(
          pending_writes.len() + self.duplicate_writes.len(),
        ));

        let process_entry = |mut ent: Entry<K, V>| {
          ent.version = commit_ts;
          entries.borrow_mut().push(ent);
        };
        pending_writes
          .into_iter()
          .for_each(|(k, v)| process_entry(Entry::unsplit(k, v)));
        duplicate_writes.into_iter().for_each(process_entry);

        // CommitTs should not be zero if we're inserting transaction markers.
        assert_ne!(commit_ts, 0);

        Ok((commit_ts, entries.into_inner()))
      }
    }
  }
}

impl<K, V, C, P> Wtm<K, V, C, P> {
  fn done_read(&mut self) {
    if !self.done_read {
      self.done_read = true;
      self.orc().read_mark.done_unchecked(self.read_ts);
    }
  }

  #[inline]
  fn orc(&self) -> &Oracle<C> {
    &self.orc
  }

  /// Discards a created transaction. This method is very important and must be called. `commit*`
  /// methods calls this internally, however, calling this multiple times doesn't cause any issues. So,
  /// this can safely be called via a defer right when transaction is created.
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

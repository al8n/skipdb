use self::error::WtmError;

use core::{borrow::Borrow, hash::Hash};

use super::*;

/// Wtm is used to perform writes to the database. It is created by
/// calling [`Tm::write`].
pub struct Wtm<K, V, C, P> {
  pub(super) read_ts: u64,
  pub(super) size: u64,
  pub(super) count: u64,
  pub(super) orc: Arc<Oracle<C>>,
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
  /// Returns the read version of this transaction.
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

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K>,
{
  /// This method is used to create a marker for the keys that are operated.
  /// It must be used to mark keys when end user is implementing iterators to
  /// make sure the transaction manager works correctly.
  ///
  /// `None` means the transaction has already been discarded.
  pub fn marker(&mut self) -> Option<Marker<'_, C>> {
    self.conflict_manager.as_mut().map(Marker::new)
  }

  /// Returns a marker for the keys that are operated and the pending writes manager.
  ///
  /// `None` means the transaction has already been discarded.
  ///
  /// As Rust's borrow checker does not allow to borrow mutable marker and the immutable pending writes manager at the same
  pub fn marker_with_pm(&mut self) -> Option<(Marker<'_, C>, &P)> {
    self
      .conflict_manager
      .as_mut()
      .map(|marker| (Marker::new(marker), self.pending_writes.as_ref().unwrap()))
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
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  /// Insert a key-value pair to the transaction.
  pub fn insert(&mut self, key: K, value: V) -> Result<(), TransactionError<C::Error, P::Error>> {
    self.insert_with_in(key, value)
  }

  /// Removes a key.
  ///
  /// This is done by adding a delete marker for the key at commit timestamp.  Any
  /// reads happening before this timestamp would be unaffected. Any reads after
  /// this commit would see the deletion.
  pub fn remove(&mut self, key: K) -> Result<(), TransactionError<C::Error, P::Error>> {
    self.modify(Entry {
      data: EntryData::Remove(key),
      version: 0,
    })
  }

  /// Rolls back the transaction.
  pub fn rollback(&mut self) -> Result<(), TransactionError<C::Error, P::Error>> {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    self
      .pending_writes
      .as_mut()
      .unwrap()
      .rollback()
      .map_err(TransactionError::Pwm)?;
    self
      .conflict_manager
      .as_mut()
      .unwrap()
      .rollback()
      .map_err(TransactionError::Cm)?;
    Ok(())
  }

  /// Returns `true` if the pending writes contains the key.
  pub fn contains_key(
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
          conflict_manager.mark_read(key);
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get<'a, 'b: 'a>(
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
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
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
  pub fn commit<F, E>(&mut self, apply: F) -> Result<(), WtmError<C::Error, P::Error, E>>
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
    Q: ?Sized + Eq + Hash,
  {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read_equivalent(k);
    }
  }

  /// Marks a key is conflict.
  pub fn mark_conflict_equivalent<Q>(&mut self, k: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Hash,
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
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub fn contains_key_equivalent<'a, 'b: 'a, Q>(
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
          conflict_manager.mark_read_equivalent(key);
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get_equivalent<'a, 'b: 'a, Q>(
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
  P: PwmEquivalent<Key = K, Value = V>,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub fn contains_key_comparable_cm_equivalent_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_equivalent(key)
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
          conflict_manager.mark_read_comparable(key);
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get_comparable_cm_equivalent_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
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
        conflict_manager.mark_read_comparable(key);
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
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub fn contains_key_comparable<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Ord,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_comparable(key)
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
          conflict_manager.mark_read_comparable(key);
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get_comparable<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
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
  C: CmEquivalent<Key = K>,
  P: PwmComparable<Key = K, Value = V>,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub fn contains_key_equivalent_cm_comparable_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
  {
    if self.discarded {
      return Err(TransactionError::Discard);
    }

    match self
      .pending_writes
      .as_ref()
      .unwrap()
      .get_comparable(key)
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
          conflict_manager.mark_read_equivalent(key);
        }

        Ok(None)
      }
    }
  }

  /// Looks for the key in the pending writes, if such key is not in the pending writes,
  /// the end user can read the key from the database.
  pub fn get_equivalent_cm_comparable_pm<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + Hash,
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
        conflict_manager.mark_read_equivalent(key);
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
  ) -> Result<std::thread::JoinHandle<R>, WtmError<C::Error, P::Error, E>>
  where
    K: Send + 'static,
    V: Send + 'static,
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Result<(), E> + Send + 'static,
    E: std::error::Error,
    R: Send + 'static,
    C: 'static,
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
  fn insert_with_in(
    &mut self,
    key: K,
    value: V,
  ) -> Result<(), TransactionError<C::Error, P::Error>> {
    let ent = Entry {
      data: EntryData::Insert { key, value },
      version: self.read_ts,
    };

    self.modify(ent)
  }

  fn modify(&mut self, ent: Entry<K, V>) -> Result<(), TransactionError<C::Error, P::Error>> {
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
}

impl<K, V, C, P> Wtm<K, V, C, P>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
{
  fn commit_entries(
    &mut self,
  ) -> Result<(u64, OneOrMore<Entry<K, V>>), TransactionError<C::Error, P::Error>> {
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
        let mut entries =
          OneOrMore::with_capacity(pending_writes.len() + self.duplicate_writes.len());

        let process_entry = |entries: &mut OneOrMore<Entry<K, V>>, mut ent: Entry<K, V>| {
          ent.version = commit_ts;
          entries.push(ent);
        };
        pending_writes
          .into_iter()
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

impl<K, V, C, P> Wtm<K, V, C, P> {
  fn done_read(&mut self) {
    if !self.done_read {
      self.done_read = true;
      self.orc().read_mark.done(self.read_ts).unwrap();
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

#[cfg(test)]
mod tests {
  use std::{collections::BTreeSet, convert::Infallible, marker::PhantomData};

  use super::*;

  #[test]
  fn wtm() {
    let tm = Tm::<String, u64, HashCm<String>, IndexMapPwm<String, u64>>::new("test", 0);
    let mut wtm = tm.write(Default::default(), Default::default()).unwrap();
    assert!(!wtm.is_discard());
    assert!(wtm.pwm().is_some());
    assert!(wtm.cm().is_some());

    let mut marker = wtm.marker().unwrap();

    marker.mark(&"1".to_owned());
    marker.mark_equivalent("3");
    marker.mark_conflict(&"2".to_owned());
    marker.mark_conflict_equivalent("4");
    wtm.mark_read(&"2".to_owned());
    wtm.mark_conflict(&"1".to_owned());
    wtm.mark_conflict_equivalent("2");
    wtm.mark_read_equivalent("3");

    wtm.insert("5".into(), 5).unwrap();

    assert_eq!(wtm.contains_key_equivalent("5").unwrap(), Some(true));
    assert_eq!(
      wtm.get_equivalent("5").unwrap().unwrap().value().unwrap(),
      &5
    );

    assert_eq!(wtm.contains_key_equivalent("6").unwrap(), None);
    assert_eq!(wtm.get_equivalent("6").unwrap(), None);
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

  #[test]
  fn wtm2() {
    let tm = Tm::<Arc<u64>, u64, TestCm<Arc<u64>>, IndexMapPwm<Arc<u64>, u64>>::new("test", 0);
    let mut wtm = tm.write(Default::default(), ()).unwrap();
    assert!(!wtm.is_discard());
    assert!(wtm.pwm().is_some());
    assert!(wtm.cm().is_some());

    let mut marker = wtm.marker().unwrap();

    let one = Arc::new(1);
    let two = Arc::new(2);
    let three = Arc::new(3);
    let four = Arc::new(4);
    let five = Arc::new(5);
    marker.mark(&one);
    marker.mark_comparable(&three);
    marker.mark_conflict(&two);
    marker.mark_conflict_comparable(&four);
    wtm.mark_read(&two);
    wtm.mark_conflict(&one);
    wtm.mark_conflict_comparable(&two);
    wtm.mark_read_comparable(&three);

    wtm.insert(five.clone(), 5).unwrap();

    assert_eq!(
      wtm.contains_key_comparable_cm_equivalent_pm(&five).unwrap(),
      Some(true)
    );
    assert_eq!(
      wtm
        .get_comparable_cm_equivalent_pm(&five)
        .unwrap()
        .unwrap()
        .value()
        .unwrap(),
      &5
    );

    let six = Arc::new(6);

    assert_eq!(
      wtm.contains_key_comparable_cm_equivalent_pm(&six).unwrap(),
      None
    );
    assert_eq!(wtm.get_comparable_cm_equivalent_pm(&six).unwrap(), None);
  }

  #[test]
  fn wtm3() {
    let tm = Tm::<Arc<u64>, u64, TestCm<Arc<u64>>, BTreePwm<Arc<u64>, u64>>::new("test", 0);
    let mut wtm = tm.write((), ()).unwrap();
    assert!(!wtm.is_discard());
    assert!(wtm.pwm().is_some());
    assert!(wtm.cm().is_some());

    let mut marker = wtm.marker().unwrap();

    let one = Arc::new(1);
    let two = Arc::new(2);
    let three = Arc::new(3);
    let four = Arc::new(4);
    let five = Arc::new(5);
    marker.mark(&one);
    marker.mark_comparable(&three);
    marker.mark_conflict(&two);
    marker.mark_conflict_comparable(&four);
    wtm.mark_read(&two);
    wtm.mark_conflict(&one);
    wtm.mark_conflict_comparable(&two);
    wtm.mark_read_comparable(&three);

    wtm.insert(five.clone(), 5).unwrap();

    assert_eq!(wtm.contains_key_comparable(&five).unwrap(), Some(true));
    assert_eq!(
      wtm.get_comparable(&five).unwrap().unwrap().value().unwrap(),
      &5
    );

    let six = Arc::new(6);

    assert_eq!(wtm.contains_key_comparable(&six).unwrap(), None);
    assert_eq!(wtm.get_comparable(&six).unwrap(), None);
  }
}

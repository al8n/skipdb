use super::*;

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: Cm<Key = K>,
  S: AsyncSpawner,
{
  /// This method is used to create a marker for the keys that are operated.
  /// It must be used to mark keys when end user is implementing iterators to
  /// make sure the transaction manager works correctly.
  ///
  /// `None` means the transaction has already been discarded.
  pub fn blocking_marker(&mut self) -> Option<Marker<'_, C>> {
    self.conflict_manager.as_mut().map(Marker::new)
  }

  /// Returns a marker for the keys that are operated and the pending writes manager.
  ///
  /// `None` means the transaction has already been discarded.
  ///
  /// As Rust's borrow checker does not allow to borrow mutable marker and the immutable pending writes manager at the same
  /// time, this method is used to solve this problem.
  pub fn blocking_marker_with_pm(&mut self) -> Option<(Marker<'_, C>, &P)> {
    self
      .conflict_manager
      .as_mut()
      .map(|marker| (Marker::new(marker), self.pending_writes.as_ref().unwrap()))
  }

  /// Marks a key is read.
  pub fn mark_read_blocking(&mut self, k: &K) {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_read(k);
    }
  }

  /// Marks a key is conflict.
  pub fn mark_conflict_blocking(&mut self, k: &K) {
    if let Some(ref mut conflict_manager) = self.conflict_manager {
      conflict_manager.mark_conflict(k);
    }
  }
}

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: Cm<Key = K>,
  P: Pwm<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Rolls back the transaction.
  #[inline]
  pub fn rollback_blocking(&mut self) -> Result<(), TransactionError<C::Error, P::Error>> {
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

  /// Insert a key-value pair to the transaction.
  pub fn insert_blocking(
    &mut self,
    key: K,
    value: V,
  ) -> Result<(), TransactionError<C::Error, P::Error>> {
    self.insert_with_blocking_in(key, value)
  }

  /// Removes a key.
  ///
  /// This is done by adding a delete marker for the key at commit timestamp.  Any
  /// reads happening before this timestamp would be unaffected. Any reads after
  /// this commit would see the deletion.
  pub fn remove_blocking(&mut self, key: K) -> Result<(), TransactionError<C::Error, P::Error>> {
    self.modify_blocking(Entry {
      data: EntryData::Remove(key),
      version: 0,
    })
  }

  /// Returns `true` if the pending writes contains the key.
  pub fn contains_key_blocking(
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
  pub fn get_blocking<'a, 'b: 'a>(
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

  fn insert_with_blocking_in(
    &mut self,
    key: K,
    value: V,
  ) -> Result<(), TransactionError<C::Error, P::Error>> {
    let ent = Entry {
      data: EntryData::Insert { key, value },
      version: self.read_ts,
    };

    self.modify_blocking(ent)
  }

  fn modify_blocking(
    &mut self,
    ent: Entry<K, V>,
  ) -> Result<(), TransactionError<C::Error, P::Error>> {
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

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: CmComparable<Key = K>,
  P: PwmEquivalent<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub fn contains_key_comparable_cm_equivalent_pm_blocking<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + core::hash::Hash,
  {
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
  pub fn get_comparable_cm_equivalent_pm_blocking<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + core::hash::Hash,
  {
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

impl<K, V, C, P, S> AsyncWtm<K, V, C, P, S>
where
  C: CmEquivalent<Key = K>,
  P: PwmComparable<Key = K, Value = V>,
  S: AsyncSpawner,
{
  /// Returns `true` if the pending writes contains the key.
  ///
  /// - `Ok(None)`: means the key is not in the pending writes, the end user can read the key from the database.
  /// - `Ok(Some(true))`: means the key is in the pending writes.
  /// - `Ok(Some(false))`: means the key is in the pending writes and but is a remove entry.
  pub fn contains_key_equivalent_cm_comparable_pm_blocking<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<bool>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + core::hash::Hash,
  {
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
  pub fn get_equivalent_cm_comparable_pm_blocking<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<EntryRef<'a, K, V>>, TransactionError<C::Error, P::Error>>
  where
    K: Borrow<Q>,
    Q: ?Sized + Eq + Ord + core::hash::Hash,
  {
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

use self::error::{Error, TransactionError};

use super::*;

/// WriteTransaction is used to perform writes to the database. It is created by
/// calling [`TransactionDB::write`].
pub struct WriteTransaction<D: Database, C, W, S = std::hash::RandomState> {
  pub(super) db: TransactionDB<D, S>,
  pub(super) read_ts: u64,
  pub(super) size: u64,
  pub(super) count: u64,

  // contains fingerprints of keys read.
  pub(super) reads: MediumVec<u64>,
  // contains fingerprints of keys written. This is used for conflict detection.
  pub(super) conflict_keys: Option<IndexSet<u64, S>>,

  // buffer stores any writes done by txn.
  pub(super) pending_writes: Option<W>,
  // Used in managed mode to store duplicate entries.
  pub(super) duplicate_writes: OneOrMore<Entry<D::Key, D::Value>>,

  pub(super) discarded: bool,
  pub(super) done_read: bool,
}

impl<D, C, W, S> Drop for WriteTransaction<D, C, W, S>
where
  D: Database,
{
  fn drop(&mut self) {
    if !self.discarded {
      self.discard();
    }
  }
}

impl<D, W, S> WriteTransaction<D, W, S>
where
  D: Database,
  W: PendingManager<Key = D::Key, Value = D::Value>,
  S: BuildHasher + Default,
{
  /// Returns the reference manager.
  pub fn manager(&self) -> Result<&W, Error<D, W>> {
    self.pending_writes.as_ref().ok_or(Error::transaction(TransactionError::Discard))
  }

  /// Insert a key-value pair to the database.
  pub fn insert(&mut self, key: D::Key, value: D::Value) -> Result<(), Error<D, W>> {
    self.insert_with_in(key, value)
  }

  /// Removes a key.
  ///
  /// This is done by adding a delete marker for the key at commit timestamp.  Any
  /// reads happening before this timestamp would be unaffected. Any reads after
  /// this commit would see the deletion.
  pub fn remove(&mut self, key: D::Key) -> Result<(), Error<D, W>> {
    self.modify(Entry {
      data: EntryData::Remove(key),
      version: 0,
    })
  }

  /// Marks a key as conflict. This is used for conflict detection.
  pub fn mark_conflict(&mut self, k: &D::Key) {
    if let Some(ref mut conflict_keys) = self.conflict_keys {
      let fp = self.db.inner.db.fingerprint(k);
      conflict_keys.insert(fp);
    }
  }

  /// Returns the version of this read transaction.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.read_ts
  }

  /// Returns the database.
  #[inline]
  pub fn database(&self) -> &TransactionDB<D, S> {
    &self.db
  } 

  /// Looks for key and returns corresponding Item.
  pub fn get<'a, 'b: 'a>(
    &'a mut self,
    key: &'b D::Key,
  ) -> Result<Option<Item<'a, D::Key, D::Value, D::ItemRef<'a>, D::Item>>, Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    if let Some(e) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get(key)
      .map_err(TransactionError::Manager)?
    {
      // If the value is None, it means that the key is removed.
      if e.value.is_none() {
        return Ok(None);
      }

      // Fulfill from buffer.
      return Ok(Some(Item::Pending(EntryRef {
        data: match &e.value {
          Some(value) => EntryDataRef::Insert { key, value },
          None => EntryDataRef::Remove(key),
        },
        version: e.version,
      })));
    } else {
      // track reads. No need to track read if txn serviced it
      // internally.
      let fp = self.inner_database().fingerprint(key);
      self.reads.push(fp);
    }

    self
      .db
      .inner
      .db
      .get(key, self.read_ts)
      .map_err(Error::database)
      .map(move |item| {
        item.map(|item| match item {
          Either::Left(item) => Item::Borrowed(item),
          Either::Right(item) => Item::Owned(item),
        })
      })
  }

  /// Returns an iterator.
  pub fn iter(&self, opts: IteratorOptions) -> Result<D::Iterator<'_>, Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    Ok(
      self.inner_database().iter(
        self
          .pending_writes
          .as_ref()
          .unwrap()
          .iter()
          .map(|(k, v)| {
            EntryRef {
              data: match &v.value {
                Some(value) => EntryDataRef::Insert { key: k, value },
                None => EntryDataRef::Remove(k),
              },
              version: self.read_ts,
            }
          }),
        self.read_ts,
        opts,
      ),
    )
  }

  /// Returns an iterator over keys.
  pub fn keys(&self, opts: KeysOptions) -> Result<D::Keys<'_>, Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    Ok(
      self.db.inner.db.keys(
        self
          .pending_writes
          .as_ref()
          .unwrap()
          .keys()
          .map(|k| KeyRef {
            key: k,
            version: self.read_ts,
          }),
        self.read_ts,
        opts,
      ),
    )
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
  pub fn commit(&mut self) -> Result<(), Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
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
    self
      .db
      .inner
      .db
      .apply(entries)
      .map(|_| {
        self.orc().done_commit(commit_ts);
        self.discard();
      })
      .map_err(|e| {
        self.orc().done_commit(commit_ts);
        self.discard();
        Error::database(e)
      })
  }
}

impl<D, W, H> WriteTransaction<D, W, H>
where
  D: Database + Send + Sync,
  D::Key: Send,
  D::Value: Send,
  W: PendingManager<Key = D::Key, Value = D::Value> + Send,
  H: BuildHasher + Default + Send + Sync + 'static,
{
  /// Acts like [`commit`](WriteTransaction::commit), but takes a future and a spawner, which gets run via a
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
  pub fn commit_with_task<R, S, JH>(
    &mut self,
    fut: impl FnOnce(Result<(), D::Error>) -> R + Send + 'static,
    spawner: S,
  ) -> Result<JH, Error<D, W>>
  where
    R: Send + 'static,
    S: FnOnce(core::pin::Pin<Box<dyn core::future::Future<Output = R> + Send>>) -> JH,
  {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
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

    let db = self.db.clone();

    Ok(spawner(Box::pin(async move {
      fut(
        db.database()
          .apply(entries)
          .map(|_| {
            db.orc().done_commit(commit_ts);
          })
          .map_err(|e| {
            db.orc().done_commit(commit_ts);
            e
          }),
      )
    })))
  }
}

impl<D, W, H> WriteTransaction<D, W, H>
where
  D: Database + Send + Sync,
  D::Key: Send,
  D::Value: Send,
  W: PendingManager<Key = D::Key, Value = D::Value> + Send,
  H: BuildHasher + Default + Send + Sync + 'static,
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
  ///
  /// If error does not occur, the transaction is successfully committed. In case of an error, the DB
  /// should not be updated (The implementors of [`Database`] must promise this), so there's no need for any rollback.
  pub fn commit_with_callback<R>(
    &mut self,
    callback: impl FnOnce(Result<(), D::Error>) -> R + Send + 'static,
  ) -> Result<std::thread::JoinHandle<R>, Error<D, W>>
  where
    R: Send + 'static,
  {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
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

    let db = self.db.clone();

    Ok(std::thread::spawn(move || {
      callback(
        db.database()
          .apply(entries)
          .map(|_| {
            db.orc().done_commit(commit_ts);
          })
          .map_err(|e| {
            db.orc().done_commit(commit_ts);
            e
          }),
      )
    }))
  }
}

impl<D, W, H> WriteTransaction<D, W, H>
where
  D: Database,
  W: PendingManager<Key = D::Key, Value = D::Value>,
  H: BuildHasher + Default,
{
  fn insert_with_in(&mut self, key: D::Key, value: D::Value) -> Result<(), Error<D, W>> {
    let ent = Entry {
      data: EntryData::Insert { key, value },
      version: self.read_ts,
    };

    self.modify(ent)
  }

  fn check_and_update_size(&mut self, ent: &Entry<D::Key, D::Value>) -> Result<(), Error<D, W>> {
    let cnt = self.count + 1;
    let database = self.inner_database();
    // Extra bytes for the version in key.
    let size = self.size + database.estimate_size(ent);
    if cnt >= database.max_batch_entries() || size >= database.max_batch_size() {
      return Err(Error::transaction(TransactionError::LargeTxn));
    }

    self.count = cnt;
    self.size = size;
    Ok(())
  }

  fn modify(&mut self, ent: Entry<D::Key, D::Value>) -> Result<(), Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    self
      .db
      .inner
      .db
      .validate_entry(&ent)
      .map_err(Error::database)?;

    self.check_and_update_size(&ent)?;

    // The txn.conflictKeys is used for conflict detection. If conflict detection
    // is disabled, we don't need to store key hashes in this map.
    if let Some(ref mut conflict_keys) = self.conflict_keys {
      let fp = self.db.inner.db.fingerprint(ent.key());
      conflict_keys.insert(fp);
    }

    // If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
    // Add the entry to duplicateWrites only if both the entries have different versions. For
    // same versions, we will overwrite the existing entry.
    let eversion = ent.version;
    let (ek, ev) = ent.split();

    let pending_writes = self.pending_writes.as_mut().unwrap();
    if let Some((old_key, old_value)) = pending_writes
      .remove_entry(&ek)
      .map_err(TransactionError::Manager)?
    {
      if old_value.version != eversion {
        self
          .duplicate_writes
          .push(Entry::unsplit(old_key, old_value));
      }
    }
    pending_writes
      .insert(ek, ev)
      .map_err(TransactionError::Manager)?;

    Ok(())
  }

  fn commit_entries(
    &mut self,
  ) -> Result<(u64, OneOrMore<Entry<D::Key, D::Value>>), TransactionError<W>> {
    // Ensure that the order in which we get the commit timestamp is the same as
    // the order in which we push these updates to the write channel. So, we
    // acquire a writeChLock before getting a commit timestamp, and only release
    // it after pushing the entries to it.
    let _write_lock = self.db.inner.orc.write_serialize_lock.lock();

    let reads = if self.reads.is_empty() {
      MediumVec::new()
    } else {
      mem::take(&mut self.reads)
    };

    let conflict_keys = if self.conflict_keys.is_none() {
      None
    } else {
      mem::take(&mut self.conflict_keys)
    };

    match self
      .db
      .inner
      .orc
      .new_commit_ts(&mut self.done_read, self.read_ts, reads, conflict_keys)
    {
      CreateCommitTimestampResult::Conflict {
        conflict_keys,
        reads,
      } => {
        // If there is a conflict, we should not send the updates to the write channel.
        // Instead, we should return the conflict error to the user.
        self.reads = reads;
        self.conflict_keys = conflict_keys;
        Err(TransactionError::Conflict)
      }
      CreateCommitTimestampResult::Timestamp(commit_ts) => {
        let pending_writes = mem::take(&mut self.pending_writes).unwrap();
        let duplicate_writes = mem::take(&mut self.duplicate_writes);
        let entries = RefCell::new(OneOrMore::with_capacity(
          pending_writes.len() + self.duplicate_writes.len(),
        ));

        let process_entry = |mut ent: Entry<D::Key, D::Value>| {
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

impl<D, C, W, S> WriteTransaction<D, C, W, S>
where
  D: Database,
{
  fn done_read(&mut self) {
    if !self.done_read {
      self.done_read = true;
      self.orc().read_mark.done_unchecked(self.read_ts);
    }
  }

  #[inline]
  fn inner_database(&self) -> &D {
    &self.db.inner.db
  }

  #[inline]
  fn orc(&self) -> &Oracle<S> {
    &self.db.inner.orc
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

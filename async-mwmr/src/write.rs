use std::collections::BTreeMap;

use pollster::FutureExt;

use self::error::{Error, TransactionError};

use super::*;

/// A pending writes manager that can be used to store pending writes in a transaction.
///
/// By default, there are two implementations of this trait:
/// - [`IndexMap`]: A hash map with consistent ordering and fast lookups.
/// - [`BTreeMap`]: A balanced binary tree with ordered keys and fast lookups.
///
/// But, users can create their own implementations by implementing this trait.
/// e.g. if you want to implement a recovery transaction manager, you can use a persistent
/// storage to store the pending writes.
pub trait PendingManager: Send + Sync + 'static {
  /// The error type returned by the pending manager.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The key type.
  type Key: Send + Sync + 'static;
  /// The value type.
  type Value: Send + Sync + 'static;

  /// Returns true if the buffer is empty.
  fn is_empty(&self) -> bool;

  /// Returns the number of elements in the buffer.
  fn len(&self) -> usize;

  /// Returns a reference to the value corresponding to the key.
  fn get(
    &self,
    key: &Self::Key,
  ) -> impl Future<Output = Result<Option<&EntryValue<Self::Value>>, Self::Error>> + Send;

  /// Inserts a key-value pair into the buffer.
  fn insert(
    &mut self,
    key: Self::Key,
    value: EntryValue<Self::Value>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Removes a key from the buffer, returning the key-value pair if the key was previously in the buffer.
  fn remove_entry(
    &mut self,
    key: &Self::Key,
  ) -> impl Future<Output = Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>> + Send;

  /// Returns an iterator over the keys in the buffer.
  fn keys(&self) -> impl Future<Output = impl Iterator<Item = &'_ Self::Key>> + Send;

  /// Returns an iterator over the key-value pairs in the buffer.
  fn iter(
    &self,
  ) -> impl Future<Output = impl Iterator<Item = (&'_ Self::Key, &'_ EntryValue<Self::Value>)>> + Send;

  /// Returns an iterator that consumes the buffer.
  fn into_iter(
    self,
  ) -> impl Future<Output = impl Iterator<Item = (Self::Key, EntryValue<Self::Value>)>> + Send;
}

/// A type alias for [`PendingManager`] that based on the [`IndexMap`].
pub type IndexMapManager<K, V, S = std::hash::RandomState> = IndexMap<K, EntryValue<V>, S>;
/// A type alias for [`PendingManager`] that based on the [`BTreeMap`].
pub type BTreeMapManager<K, V> = BTreeMap<K, EntryValue<V>>;

impl<K, V, S> PendingManager for IndexMap<K, EntryValue<V>, S>
where
  K: Eq + core::hash::Hash + Send + Sync + 'static,
  V: Send + Sync + 'static,
  S: BuildHasher + Default + Send + Sync + 'static,
{
  type Error = std::convert::Infallible;
  type Key = K;
  type Value = V;

  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  fn len(&self) -> usize {
    self.len()
  }

  async fn get(&self, key: &K) -> Result<Option<&EntryValue<V>>, Self::Error> {
    Ok(self.get(key))
  }

  async fn insert(&mut self, key: K, value: EntryValue<V>) -> Result<(), Self::Error> {
    self.insert(key, value);
    Ok(())
  }

  async fn remove_entry(&mut self, key: &K) -> Result<Option<(K, EntryValue<V>)>, Self::Error> {
    Ok(self.shift_remove_entry(key))
  }

  async fn keys(&self) -> impl Iterator<Item = &K> {
    self.keys()
  }

  async fn iter(&self) -> impl Iterator<Item = (&K, &EntryValue<V>)> {
    self.iter()
  }

  async fn into_iter(self) -> impl Iterator<Item = (K, EntryValue<V>)> {
    core::iter::IntoIterator::into_iter(self)
  }
}

impl<K, V> PendingManager for BTreeMap<K, EntryValue<V>>
where
  K: Eq + core::hash::Hash + Ord + Send + Sync + 'static,
  V: Send + Sync + 'static,
{
  type Error = std::convert::Infallible;
  type Key = K;
  type Value = V;

  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  fn len(&self) -> usize {
    self.len()
  }

  async fn get(&self, key: &K) -> Result<Option<&EntryValue<Self::Value>>, Self::Error> {
    Ok(self.get(key))
  }

  async fn insert(&mut self, key: K, value: EntryValue<Self::Value>) -> Result<(), Self::Error> {
    self.insert(key, value);
    Ok(())
  }

  async fn remove_entry(
    &mut self,
    key: &K,
  ) -> Result<Option<(K, EntryValue<Self::Value>)>, Self::Error> {
    Ok(self.remove_entry(key))
  }

  async fn keys(&self) -> impl Iterator<Item = &K> {
    self.keys()
  }

  async fn iter(&self) -> impl Iterator<Item = (&K, &EntryValue<Self::Value>)> {
    self.iter()
  }

  async fn into_iter(self) -> impl Iterator<Item = (K, EntryValue<Self::Value>)> {
    core::iter::IntoIterator::into_iter(self)
  }
}

/// WriteTransaction is used to perform writes to the database. It is created by
/// calling [`TransactionDB::write`].
pub struct WriteTransaction<
  D: Database,
  W: PendingManager,
  S: AsyncSpawner,
  H = std::hash::RandomState,
> {
  pub(super) db: TransactionDB<D, S, H>,
  pub(super) read_ts: u64,
  pub(super) size: u64,
  pub(super) count: u64,

  // contains fingerprints of keys read.
  pub(super) reads: MediumVec<u64>,
  // contains fingerprints of keys written. This is used for conflict detection.
  pub(super) conflict_keys: Option<IndexSet<u64, H>>,

  // buffer stores any writes done by txn.
  pub(super) pending_writes: Option<W>,
  // Used in managed mode to store duplicate entries.
  pub(super) duplicate_writes: OneOrMore<Entry<D>>,

  pub(super) discarded: bool,
  pub(super) done_read: bool,
}

impl<D, W, S, H> Drop for WriteTransaction<D, W, S, H>
where
  D: Database,
  W: PendingManager,
  S: AsyncSpawner,
{
  fn drop(&mut self) {
    if !self.discarded {
      self.discard().block_on();
    }
  }
}

impl<D, W, S, H> WriteTransaction<D, W, S, H>
where
  D: Database,
  W: PendingManager<Key = D::Key, Value = D::Value>,
  S: AsyncSpawner,
  H: BuildHasher + Default,
{
  /// Insert a key-value pair to the database.
  pub async fn insert(&mut self, key: D::Key, value: D::Value) -> Result<(), Error<D, W>> {
    self.insert_with_in(key, value).await
  }

  /// Removes a key.
  ///
  /// This is done by adding a delete marker for the key at commit timestamp.  Any
  /// reads happening before this timestamp would be unaffected. Any reads after
  /// this commit would see the deletion.
  pub async fn remove(&mut self, key: D::Key) -> Result<(), Error<D, W>> {
    self
      .modify(Entry {
        data: EntryData::Remove(key),
        version: 0,
      })
      .await
  }

  /// Looks for key and returns corresponding Item.
  pub async fn get<'a, 'b: 'a>(
    &'a mut self,
    key: &'b D::Key,
  ) -> Result<Option<Item<'a, D>>, Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    if let Some(e) = self
      .pending_writes
      .as_ref()
      .unwrap()
      .get(key)
      .await
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
      let fp = self.database().fingerprint(key);
      self.reads.push(fp);
    }

    self
      .db
      .inner
      .db
      .get(key, self.read_ts)
      .await
      .map_err(Error::database)
      .map(move |item| {
        item.map(|item| match item {
          Either::Left(item) => Item::Borrowed(item),
          Either::Right(item) => Item::Owned(item),
        })
      })
  }

  /// Returns an iterator.
  pub async fn iter(&self, opts: IteratorOptions) -> Result<D::Iterator<'_>, Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    Ok(
      self
        .database()
        .iter(
          self
            .pending_writes
            .as_ref()
            .unwrap()
            .iter()
            .await
            .map(|(k, v)| EntryRef {
              data: match &v.value {
                Some(value) => EntryDataRef::Insert { key: k, value },
                None => EntryDataRef::Remove(k),
              },
              version: self.read_ts,
            }),
          self.read_ts,
          opts,
        )
        .await,
    )
  }

  /// Returns an iterator over keys.
  pub async fn keys(&self, opts: KeysOptions) -> Result<D::Keys<'_>, Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    Ok(
      self
        .db
        .inner
        .db
        .keys(
          self
            .pending_writes
            .as_ref()
            .unwrap()
            .keys()
            .await
            .map(|k| KeyRef {
              key: k,
              version: self.read_ts,
            }),
          self.read_ts,
          opts,
        )
        .await,
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
  pub async fn commit(&mut self) -> Result<(), Error<D, W>> {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    if self.pending_writes.as_ref().unwrap().is_empty() {
      // Nothing to commit
      self.discard().await;
      return Ok(());
    }

    let (commit_ts, entries) = match self.commit_entries().await {
      Ok((commit_ts, entries)) => (commit_ts, entries),
      Err(e) => {
        return Err(match e {
          TransactionError::Conflict => Error::Transaction(e),
          _ => {
            self.discard().await;
            Error::Transaction(e)
          }
        });
      }
    };
    match self.db.inner.db.apply(entries).await {
      Ok(_) => {
        self.orc().done_commit(commit_ts).await;
        self.discard().await;
        Ok(())
      }
      Err(e) => {
        self.orc().done_commit(commit_ts).await;
        Err(Error::database(e))
      }
    }
  }
}

impl<D, W, S, H> WriteTransaction<D, W, S, H>
where
  D: Database + Send + Sync,
  D::Key: Send,
  D::Value: Send,
  W: PendingManager<Key = D::Key, Value = D::Value> + Send,
  S: AsyncSpawner,
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
  pub async fn commit_with_task<R>(
    &mut self,
    fut: impl FnOnce(Result<(), D::Error>) -> R + Send + 'static,
  ) -> Result<S::JoinHandle<R>, Error<D, W>>
  where
    R: Send + 'static,
  {
    if self.discarded {
      return Err(Error::transaction(TransactionError::Discard));
    }

    if self.pending_writes.as_ref().unwrap().is_empty() {
      // Nothing to commit
      self.discard().await;
      return Ok(S::spawn(async move { fut(Ok(())) }));
    }

    let (commit_ts, entries) = match self.commit_entries().await {
      Ok((commit_ts, entries)) => (commit_ts, entries),
      Err(e) => {
        return Err(match e {
          TransactionError::Conflict => Error::Transaction(e),
          _ => {
            self.discard().await;
            Error::Transaction(e)
          }
        });
      }
    };

    let db = self.db.clone();

    Ok(S::spawn(async move {
      fut(match db.database().apply(entries).await {
        Ok(_) => {
          db.orc().done_commit(commit_ts).await;
          Ok(())
        }
        Err(e) => {
          db.orc().done_commit(commit_ts).await;
          Err(e)
        }
      })
    }))
  }
}

impl<D, W, S, H> WriteTransaction<D, W, S, H>
where
  D: Database,
  W: PendingManager<Key = D::Key, Value = D::Value>,
  S: AsyncSpawner,
  H: BuildHasher + Default,
{
  async fn insert_with_in(&mut self, key: D::Key, value: D::Value) -> Result<(), Error<D, W>> {
    let ent = Entry {
      data: EntryData::Insert { key, value },
      version: self.read_ts,
    };

    self.modify(ent).await
  }

  fn check_and_update_size(&mut self, ent: &Entry<D>) -> Result<(), Error<D, W>> {
    let cnt = self.count + 1;
    let database = self.database();
    // Extra bytes for the version in key.
    let size = self.size + database.estimate_size(ent);
    if cnt >= database.max_batch_entries() || size >= database.max_batch_size() {
      return Err(Error::transaction(TransactionError::LargeTxn));
    }

    self.count = cnt;
    self.size = size;
    Ok(())
  }

  async fn modify(&mut self, ent: Entry<D>) -> Result<(), Error<D, W>> {
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
      .await
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
      .await
      .map_err(TransactionError::Manager)?;

    Ok(())
  }

  async fn commit_entries(&mut self) -> Result<(u64, OneOrMore<Entry<D>>), TransactionError<W>> {
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
      .await
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
        let mut entries =
          OneOrMore::with_capacity(pending_writes.len() + self.duplicate_writes.len());

        let mut process_entry = |mut ent: Entry<D>| {
          ent.version = commit_ts;
          entries.push(ent);
        };
        pending_writes
          .into_iter()
          .await
          .for_each(|(k, v)| process_entry(Entry::unsplit(k, v)));
        duplicate_writes.into_iter().for_each(process_entry);

        // CommitTs should not be zero if we're inserting transaction markers.
        assert_ne!(commit_ts, 0);

        Ok((commit_ts, entries))
      }
    }
  }
}

impl<D, W, S, H> WriteTransaction<D, W, S, H>
where
  D: Database,
  W: PendingManager,
  S: AsyncSpawner,
{
  async fn done_read(&mut self) {
    if !self.done_read {
      self.done_read = true;
      self.orc().read_mark.done_unchecked(self.read_ts).await;
    }
  }

  #[inline]
  fn database(&self) -> &D {
    &self.db.inner.db
  }

  #[inline]
  fn orc(&self) -> &Oracle<S, H> {
    &self.db.inner.orc
  }

  /// Discards a created transaction. This method is very important and must be called. `commit*`
  /// methods calls this internally, however, calling this multiple times doesn't cause any issues. So,
  /// this can safely be called via a defer right when transaction is created.
  ///
  /// NOTE: If any operations are run on a discarded transaction, [`TransactionError::Discard`] is returned.
  pub async fn discard(&mut self) {
    if self.discarded {
      return;
    }
    self.discarded = true;
    self.done_read().await;
  }
}

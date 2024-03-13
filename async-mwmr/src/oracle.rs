use core::{hash::BuildHasher, ops::AddAssign};
use std::borrow::Cow;

use futures::lock::Mutex;
use indexmap::IndexSet;
use pollster::FutureExt;
use smallvec_wrapper::{MediumVec, TinyVec};

use wmark::{AsyncCloser, AsyncSpawner, AsyncWaterMark};

#[derive(Debug)]
pub(super) struct OracleInner<H> {
  next_txn_ts: u64,

  last_cleanup_ts: u64,

  /// Contains all committed writes (contains fingerprints
  /// of keys written and their latest commit counter).
  pub(super) committed_txns: TinyVec<CommittedTxn<H>>,
}

impl<H> OracleInner<H>
where
  H: BuildHasher,
{
  fn has_conflict(&self, reads: &[u64], read_ts: u64) -> bool {
    if reads.is_empty() {
      return false;
    }

    for committed_txn in self.committed_txns.iter() {
      // If the committed_txn.ts is less than txn.read_ts that implies that the
      // committed_txn finished before the current transaction started.
      // We don't need to check for conflict in that case.
      // This change assumes linearizability. Lack of linearizability could
      // cause the read ts of a new txn to be lower than the commit ts of
      // a txn before it (@mrjn).
      if committed_txn.ts <= read_ts {
        continue;
      }

      for ro in reads {
        if let Some(conflict_keys) = &committed_txn.conflict_keys {
          if conflict_keys.contains(ro) {
            return true;
          }
        }
      }
    }
    false
  }

  #[inline]
  fn cleanup_committed_transactions<S>(
    &mut self,
    detect_conflicts: bool,
    read_mark: &AsyncWaterMark<S>,
  ) {
    if !detect_conflicts {
      // When detectConflicts is set to false, we do not store any
      // committedTxns and so there's nothing to clean up.
      return;
    }

    let max_read_ts = read_mark.done_until_unchecked();

    assert!(max_read_ts >= self.last_cleanup_ts);

    // do not run clean up if the max_read_ts (read timestamp of the
    // oldest transaction that is still in flight) has not increased
    if max_read_ts == self.last_cleanup_ts {
      return;
    }

    self.last_cleanup_ts = max_read_ts;

    self.committed_txns.retain(|txn| txn.ts > max_read_ts);
  }
}

pub(super) enum CreateCommitTimestampResult<S> {
  Timestamp(u64),
  Conflict {
    reads: MediumVec<u64>,
    conflict_keys: Option<IndexSet<u64, S>>,
  },
}

#[derive(Debug)]
pub(super) struct Oracle<S: AsyncSpawner, H> {
  /// if the txns should be checked for conflicts.
  detect_conflicts: bool,

  // write_serialize_lock is for ensuring that transactions go to the write
  // channel in the same order as their commit timestamps.
  pub(super) write_serialize_lock: Mutex<()>,

  pub(super) inner: Mutex<OracleInner<H>>,

  /// Used by DB
  pub(super) read_mark: AsyncWaterMark<S>,

  /// Used to block new transaction, so all previous commits are visible to a new read.
  pub(super) txn_mark: AsyncWaterMark<S>,

  /// closer is used to stop watermarks.
  closer: AsyncCloser<S>,
}

impl<S, H> Oracle<S, H>
where
  S: AsyncSpawner,
  H: BuildHasher,
{
  pub(super) async fn new_commit_ts(
    &self,
    done_read: &mut bool,
    read_ts: u64,
    reads: MediumVec<u64>,
    conflict_keys: Option<IndexSet<u64, H>>,
  ) -> CreateCommitTimestampResult<H> {
    let mut inner = self.inner.lock().await;

    if inner.has_conflict(&reads, read_ts) {
      return CreateCommitTimestampResult::Conflict {
        reads,
        conflict_keys,
      };
    }

    let ts = {
      if !*done_read {
        self.read_mark.done_unchecked(read_ts).await;
        *done_read = true;
      }

      inner.cleanup_committed_transactions(self.detect_conflicts, &self.read_mark);

      // This is the general case, when user doesn't specify the read and commit ts.
      let ts = inner.next_txn_ts;
      inner.next_txn_ts += 1;
      self.txn_mark.begin_unchecked(ts).await;
      ts
    };

    assert!(ts >= inner.last_cleanup_ts);

    if self.detect_conflicts {
      // We should ensure that txns are not added to o.committedTxns slice when
      // conflict detection is disabled otherwise this slice would keep growing.
      inner
        .committed_txns
        .push(CommittedTxn { ts, conflict_keys });
    }

    CreateCommitTimestampResult::Timestamp(ts)
  }
}

impl<S, H> Oracle<S, H>
where
  S: AsyncSpawner,
{
  #[inline]
  pub async fn new(
    read_mark_name: Cow<'static, str>,
    txn_mark_name: Cow<'static, str>,
    detect_conflicts: bool,
    next_txn_ts: u64,
  ) -> Self {
    let closer = AsyncCloser::new(2);
    let orc = Self {
      detect_conflicts,
      write_serialize_lock: Mutex::new(()),
      inner: Mutex::new(OracleInner {
        next_txn_ts,
        last_cleanup_ts: 0,
        committed_txns: TinyVec::new(),
      }),
      read_mark: AsyncWaterMark::new(read_mark_name),
      txn_mark: AsyncWaterMark::new(txn_mark_name),
      closer,
    };
    orc.read_mark.init(orc.closer.clone());
    orc.txn_mark.init(orc.closer.clone());
    orc
  }

  #[inline]
  pub(super) async fn read_ts(&self) -> u64 {
    let read_ts = {
      let inner = self.inner.lock().await;

      let read_ts = inner.next_txn_ts - 1;
      self.read_mark.begin_unchecked(read_ts).await;
      read_ts
    };

    // Wait for all txns which have no conflicts, have been assigned a commit
    // timestamp and are going through the write to value log and LSM tree
    // process. Not waiting here could mean that some txns which have been
    // committed would not be read.
    if let Err(e) = self.txn_mark.wait_for_mark_unchecked(read_ts).await {
      panic!("{e}");
    }

    read_ts
  }

  #[inline]
  pub(super) async fn increment_next_ts(&self) {
    self.inner.lock().await.next_txn_ts.add_assign(1);
  }

  #[inline]
  pub(super) fn discard_at_or_below(&self) -> u64 {
    self.read_mark.done_until_unchecked()
  }

  #[inline]
  pub(super) async fn done_read(&self, read_ts: u64) {
    self.read_mark.done_unchecked(read_ts).await;
  }

  #[inline]
  pub(super) async fn done_commit(&self, cts: u64) {
    self.txn_mark.done_unchecked(cts).await;
  }

  #[inline]
  async fn stop(&self) {
    self.closer.signal_and_wait().await;
  }
}

impl<S: AsyncSpawner, H> Drop for Oracle<S, H> {
  fn drop(&mut self) {
    self.stop().block_on();
  }
}

pub(super) struct CommittedTxn<S> {
  ts: u64,
  /// Keeps track of the entries written at timestamp ts.
  conflict_keys: Option<IndexSet<u64, S>>,
}

impl<S> core::fmt::Debug for CommittedTxn<S> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("CommittedTxn")
      .field("ts", &self.ts)
      .field("conflict_keys", &self.conflict_keys)
      .finish()
  }
}

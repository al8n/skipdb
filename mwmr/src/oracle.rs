use core::{hash::BuildHasher, ops::AddAssign};
use std::borrow::Cow;

use indexmap::IndexSet;
use parking_lot::{Mutex, MutexGuard};
use smallvec_wrapper::{MediumVec, TinyVec};

use wmark::{Closer, WaterMark};

#[derive(Debug)]
pub(super) struct OracleInner<C> {
  next_txn_ts: u64,

  last_cleanup_ts: u64,

  /// Contains all committed writes (contains fingerprints
  /// of keys written and their latest commit counter).
  pub(super) committed_txns: TinyVec<CommittedTxn<C>>,
}

impl<S> OracleInner<S>
where
  S: BuildHasher,
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
        if let Some(conflict_keys) = &committed_txn.conflict_manager {
          if conflict_keys.contains(ro) {
            return true;
          }
        }
      }
    }
    false
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
pub(super) struct Oracle<S> {
  /// if the txns should be checked for conflicts.
  detect_conflicts: bool,

  // write_serialize_lock is for ensuring that transactions go to the write
  // channel in the same order as their commit timestamps.
  pub(super) write_serialize_lock: Mutex<()>,

  pub(super) inner: Mutex<OracleInner<S>>,

  /// Used by DB
  pub(super) read_mark: WaterMark,

  /// Used to block new transaction, so all previous commits are visible to a new read.
  pub(super) txn_mark: WaterMark,

  /// closer is used to stop watermarks.
  closer: Closer,
}

impl<S> Oracle<S>
where
  S: BuildHasher,
{
  pub(super) fn new_commit_ts(
    &self,
    done_read: &mut bool,
    read_ts: u64,
    reads: MediumVec<u64>,
    conflict_keys: Option<IndexSet<u64, S>>,
  ) -> CreateCommitTimestampResult<S> {
    let mut inner = self.inner.lock();

    if inner.has_conflict(&reads, read_ts) {
      return CreateCommitTimestampResult::Conflict {
        reads,
        conflict_keys,
      };
    }

    let ts = {
      if !*done_read {
        self.read_mark.done_unchecked(read_ts);
        *done_read = true;
      }

      self.cleanup_committed_transactions(&mut inner);

      // This is the general case, when user doesn't specify the read and commit ts.
      let ts = inner.next_txn_ts;
      inner.next_txn_ts += 1;
      self.txn_mark.begin_unchecked(ts);
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

  #[inline]
  fn cleanup_committed_transactions(&self, inner: &mut MutexGuard<OracleInner<S>>) {
    if !self.detect_conflicts {
      // When detectConflicts is set to false, we do not store any
      // committedTxns and so there's nothing to clean up.
      return;
    }

    let max_read_ts = self.read_mark.done_until_unchecked();

    assert!(max_read_ts >= inner.last_cleanup_ts);

    // do not run clean up if the max_read_ts (read timestamp of the
    // oldest transaction that is still in flight) has not increased
    if max_read_ts == inner.last_cleanup_ts {
      return;
    }

    inner.last_cleanup_ts = max_read_ts;

    inner.committed_txns.retain(|txn| txn.ts > max_read_ts);
  }
}

impl<S> Oracle<S> {
  #[inline]
  pub fn new(
    read_mark_name: Cow<'static, str>,
    txn_mark_name: Cow<'static, str>,
    detect_conflicts: bool,
    next_txn_ts: u64,
  ) -> Self {
    let closer = Closer::new(2);
    let orc = Self {
      detect_conflicts,
      write_serialize_lock: Mutex::new(()),
      inner: Mutex::new(OracleInner {
        next_txn_ts,
        last_cleanup_ts: 0,
        committed_txns: TinyVec::new(),
      }),
      read_mark: WaterMark::new(read_mark_name),
      txn_mark: WaterMark::new(txn_mark_name),
      closer,
    };

    orc.read_mark.init(orc.closer.clone());
    orc.txn_mark.init(orc.closer.clone());
    orc
  }

  #[inline]
  pub(super) fn read_ts(&self) -> u64 {
    let read_ts = {
      let inner = self.inner.lock();

      let read_ts = inner.next_txn_ts - 1;
      self.read_mark.begin_unchecked(read_ts);
      read_ts
    };

    // Wait for all txns which have no conflicts, have been assigned a commit
    // timestamp and are going through the write to value log and LSM tree
    // process. Not waiting here could mean that some txns which have been
    // committed would not be read.
    if let Err(e) = self.txn_mark.wait_for_mark_unchecked(read_ts) {
      panic!("{e}");
    }

    read_ts
  }

  #[inline]
  pub(super) fn increment_next_ts(&self) {
    self.inner.lock().next_txn_ts.add_assign(1);
  }

  #[inline]
  pub(super) fn discard_at_or_below(&self) -> u64 {
    self.read_mark.done_until_unchecked()
  }

  #[inline]
  pub(super) fn done_read(&self, read_ts: u64) {
    self.read_mark.done_unchecked(read_ts);
  }

  #[inline]
  pub(super) fn done_commit(&self, cts: u64) {
    self.txn_mark.done_unchecked(cts);
  }

  #[inline]
  fn stop(&self) {
    self.closer.signal_and_wait();
  }
}

impl<S> Drop for Oracle<S> {
  fn drop(&mut self) {
    self.stop();
  }
}

pub(super) struct CommittedTxn<C> {
  ts: u64,
  /// Keeps track of the entries written at timestamp ts.
  conflict_manager: Option<C>,
}

impl<C: core::fmt::Debug> core::fmt::Debug for CommittedTxn<C> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("CommittedTxn")
      .field("committed_version", &self.ts)
      .field("conflict_manager", &self.conflict_manager)
      .finish()
  }
}

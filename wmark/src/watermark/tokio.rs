use ::tokio::{
  select,
  sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
  },
};
use crossbeam_utils::CachePadded;
use smallvec_wrapper::MediumVec;

use core::{
  cmp::Reverse,
  sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

#[cfg(feature = "std")]
use std::{
  borrow::Cow,
  collections::{BinaryHeap, HashMap},
  sync::Arc,
};

#[cfg(not(feature = "std"))]
use alloc::{borrow::Cow, collections::BinaryHeap};

#[cfg(not(feature = "std"))]
use hashbrown::HashMap;

use crate::{TokioCloser, WaterMarkError};

type Result<T> = core::result::Result<T, WaterMarkError>;

#[derive(Debug)]
enum MarkIndex {
  Single(u64),
  Multiple(MediumVec<u64>),
}

#[derive(Debug)]
struct Mark {
  index: MarkIndex,
  waiter: Option<oneshot::Sender<()>>,
  done: bool,
}

#[derive(Debug)]
struct Inner {
  inited: AtomicBool,
  done_until: CachePadded<AtomicU64>,
  last_index: CachePadded<AtomicU64>,
  name: Cow<'static, str>,
  mark_tx: UnboundedSender<Mark>,
}

impl Inner {
  async fn process(&self, mut rx: UnboundedReceiver<Mark>, closer: TokioCloser) {
    macro_rules! process_one {
      ($pending:ident,$waiters:ident,$indices:ident,$idx: ident, $done: ident) => {{
        if !$pending.contains_key(&$idx) {
          $indices.push(Reverse($idx));
        }

        let prev = $pending.entry($idx).or_insert(0);
        if $done {
          (*prev) = (*prev).saturating_sub(1);
        } else {
          (*prev) += 1;
        }

        let mut delta = 1;
        if $done {
          delta = -1;
        }
        $pending
          .entry($idx)
          .and_modify(|v| *v += delta)
          .or_insert(delta);

        // Update mark by going through all indices in order; and checking if they have
        // been done. Stop at the first index, which isn't done.
        let done_until = self.done_until.load(Ordering::SeqCst);
        assert!(
          done_until <= $idx,
          "name: {}, done_until: {}, idx: {}",
          self.name,
          done_until,
          $idx
        );

        let mut until = done_until;

        while !$indices.is_empty() {
          let min = $indices.peek().unwrap().0;
          if let Some(done) = $pending.get(&min) {
            if done.gt(&0) {
              break; // len(indices) will be > 0.
            }
          }
          // Even if done is called multiple times causing it to become
          // negative, we should still pop the index.
          $indices.pop();
          $pending.remove(&min);
          until = min;
        }

        if until != done_until {
          assert_eq!(
            self.done_until.compare_exchange(
              done_until,
              until,
              Ordering::SeqCst,
              Ordering::Acquire
            ),
            Ok(done_until)
          );
        }

        if until - done_until <= $waiters.len() as u64 {
          // Close channel and remove from waiters.
          (done_until + 1..=until).for_each(|idx| {
            let _ = $waiters.remove(&idx);
          });
        } else {
          // Close and drop idx <= util channels.
          $waiters.retain(|idx, _| *idx > until);
        }
      }};
    }
    scopeguard::defer!(closer.done(););

    self.inited.store(true, Ordering::SeqCst);

    let mut indices: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
    // pending maps raft proposal index to the number of pending mutations for this proposal.
    let mut pending: HashMap<u64, i64> = HashMap::new();
    let mut waiters: HashMap<u64, MediumVec<oneshot::Sender<()>>> = HashMap::new();

    loop {
      select! {
        _ = closer.has_been_closed() => return,
        mark = rx.recv() => match mark {
          Some(mark) => {
            if let Some(wait_tx) = mark.waiter {
              if let MarkIndex::Single(index) = mark.index {
                let done_until = self.done_until.load(Ordering::SeqCst);
                if done_until >= index {
                  let _ = wait_tx; // Close channel.
                } else {
                  waiters.entry(index).or_default().push(wait_tx);
                }
              }
            } else {
              let done = mark.done;
              match mark.index {
                MarkIndex::Single(idx) => process_one!(pending, waiters, indices, idx, done),
                MarkIndex::Multiple(bunch) => bunch.into_iter().for_each(|idx| process_one!(pending, waiters, indices, idx, done)),
              }
            }
          },
          None => {
            // Channel closed.
            #[cfg(feature = "tracing")]
            tracing::error!(target: "watermark", err = "watermark has been dropped.");
            return;
          }
        },
      }
    }
  }
}

/// TokioWaterMark is used to keep track of the minimum un-finished index. Typically, an index k becomes
/// finished or "done" according to a TokioWaterMark once `done(k)` has been called
///  1. as many times as `begin(k)` has, AND
///  2. a positive number of times.
///
/// An index may also become "done" by calling `set_done_until` at a time such that it is not
/// inter-mingled with `begin/done` calls.
///
/// Since `done_until` and `last_index` addresses are passed to sync/atomic packages, we ensure that they
/// are 64-bit aligned by putting them at the beginning of the structure.
#[derive(Debug, Clone)]
pub struct TokioWaterMark {
  inner: Arc<Inner>,
}

impl TokioWaterMark {
  /// Create a new TokioWaterMark with the given name.
  ///
  /// **Note**: Before using the watermark, you must call `init` to start the background thread.
  #[inline]
  pub fn new(name: Cow<'static, str>, closer: TokioCloser) -> Self {
    let (mark_tx, mark_rx) = unbounded_channel();
    let inner = Arc::new(Inner {
      done_until: CachePadded::new(AtomicU64::new(0)),
      last_index: CachePadded::new(AtomicU64::new(0)),
      name,
      mark_tx,
      inited: AtomicBool::new(false),
    });
    let inner1 = inner.clone();
    ::tokio::spawn(async move { inner1.process(mark_rx, closer).await });

    Self { inner }
  }

  /// Returns the name of the watermark.
  #[inline(always)]
  pub fn name(&self) -> &str {
    self.inner.name.as_ref()
  }

  /// Sets the last index to the given value.
  #[inline]
  pub fn begin(&self, index: u64) -> Result<()> {
    self.inner.last_index.store(index, Ordering::SeqCst);
    self
      .inner
      .mark_tx
      .send(Mark {
        index: MarkIndex::Single(index),
        waiter: None,
        done: false,
      })
      .map_err(|_| WaterMarkError::Canceled)
  }

  /// Works like [`begin`] but accepts multiple indices.
  #[inline]
  pub fn begin_many(&self, indices: MediumVec<u64>) -> Result<()> {
    let last_index = *indices.last().unwrap();
    self.inner.last_index.store(last_index, Ordering::SeqCst);
    self
      .inner
      .mark_tx
      .send(Mark {
        index: MarkIndex::Multiple(indices),
        waiter: None,
        done: false,
      })
      .map_err(|_| WaterMarkError::Canceled)
  }

  /// Sets a single index as done.
  #[inline]
  pub fn done(&self, index: u64) -> Result<()> {
    self
      .inner
      .mark_tx
      .send(Mark {
        index: MarkIndex::Single(index),
        waiter: None,
        done: true,
      })
      .map_err(|_| WaterMarkError::Canceled) // unwrap is safe because self also holds a receiver
  }

  /// Sets multiple indices as done.
  #[inline]
  pub fn done_many(&self, indices: MediumVec<u64>) -> Result<()> {
    self
      .inner
      .mark_tx
      .send(Mark {
        index: MarkIndex::Multiple(indices),
        waiter: None,
        done: true,
      })
      .map_err(|_| WaterMarkError::Canceled) // unwrap is safe because self also holds a receiver
  }

  /// Returns the maximum index that has the property that all indices
  /// less than or equal to it are done.
  #[inline]
  pub fn done_until(&self) -> u64 {
    self.inner.done_until.load(Ordering::SeqCst)
  }

  /// Sets the maximum index that has the property that all indices
  /// less than or equal to it are done.
  #[inline]
  pub fn set_done_until(&self, val: u64) {
    self.inner.done_until.store(val, Ordering::SeqCst)
  }

  /// Returns the last index for which `begin` has been called.
  #[inline]
  pub fn last_index(&self) -> u64 {
    self.inner.last_index.load(Ordering::SeqCst)
  }

  /// Waits until the given index is marked as done.
  #[inline]
  pub async fn wait_for_mark(&self, index: u64) -> Result<()> {
    if self.inner.done_until.load(Ordering::SeqCst) >= index {
      return Ok(());
    }

    let (wait_tx, wait_rx) = oneshot::channel();
    self
      .inner
      .mark_tx
      .send(Mark {
        index: MarkIndex::Single(index),
        waiter: Some(wait_tx),
        done: false,
      })
      .map_err(|_| WaterMarkError::Canceled)?; // unwrap is safe because self also holds a receiver

    let _ = wait_rx.await;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::future::Future;

  async fn init_and_close<Fut, F>(f: F)
  where
    Fut: Future,
    F: FnOnce(TokioWaterMark) -> Fut,
  {
    let closer = TokioCloser::new(1);

    let watermark = TokioWaterMark::new("watermark".into(), closer.clone());

    f(watermark.clone()).await;

    closer.signal_and_wait().await;
  }

  #[tokio::test]
  async fn test_basic() {
    init_and_close::<_, _>(|_| async {}).await;
  }

  #[tokio::test]
  async fn test_begin_done() {
    init_and_close::<_, _>(|watermark| async move {
      watermark.begin(1).unwrap();
      watermark.begin_many([2, 3].into_iter().collect()).unwrap();

      watermark.done(1).unwrap();
      watermark.done_many([2, 3].into_iter().collect()).unwrap();
    })
    .await;
  }

  #[tokio::test]
  async fn test_wait_for_mark() {
    init_and_close::<_, _>(|watermark| async move {
      watermark
        .begin_many([1, 2, 3].into_iter().collect())
        .unwrap();
      watermark.done_many([2, 3].into_iter().collect()).unwrap();

      assert_eq!(watermark.done_until(), 0);

      watermark.done(1).unwrap();
      watermark.wait_for_mark(1).await.unwrap();
      watermark.wait_for_mark(3).await.unwrap();
      assert_eq!(watermark.done_until(), 3);
    })
    .await;
  }

  #[tokio::test]
  async fn test_last_index() {
    init_and_close::<_, _>(|watermark| async move {
      watermark
        .begin_many([1, 2, 3].into_iter().collect())
        .unwrap();
      watermark.done_many([2, 3].into_iter().collect()).unwrap();

      assert_eq!(watermark.last_index(), 3);
    })
    .await;
  }

  #[tokio::test]
  async fn test_multiple_singles() {
    let closer = TokioCloser::default();
    closer.signal();
    closer.signal();
    closer.signal_and_wait().await;

    let closer = TokioCloser::new(1);
    closer.done();
    closer.signal_and_wait().await;
    closer.signal_and_wait().await;
    closer.signal();
  }

  #[tokio::test]
  async fn test_closer() {
    let closer = TokioCloser::new(1);
    let tc = closer.clone();
    tokio::spawn(async move {
      if let Err(err) = tc.has_been_closed().await {
        eprintln!("err: {}", err);
      }
      tc.done();
    });
    closer.signal_and_wait().await;
  }

  #[tokio::test]
  async fn test_closer_() {
    use async_channel::unbounded;
    use core::time::Duration;

    let (tx, rx) = unbounded();

    let c = TokioCloser::default();

    for _ in 0..10 {
      let c = c.clone();
      let tx = tx.clone();
      tokio::spawn(async move {
        assert!(c.has_been_closed().await.is_err());
        tx.send(()).await.unwrap();
      });
    }
    c.signal();
    for _ in 0..10 {
      tokio::time::timeout(Duration::from_millis(1000), rx.recv())
        .await
        .unwrap()
        .unwrap();
    }
  }
}

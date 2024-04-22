use crossbeam_channel::{bounded, select, Receiver, Sender};
use crossbeam_utils::CachePadded;
use smallvec_wrapper::MediumVec;
use std::{
  borrow::Cow,
  cell::RefCell,
  cmp::Reverse,
  collections::{BinaryHeap, HashMap},
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
};

use crate::{Closer, WaterMarkError};

type Result<T> = std::result::Result<T, WaterMarkError>;

#[derive(Debug)]
enum MarkIndex {
  Single(u64),
  Multiple(MediumVec<u64>),
}

#[derive(Debug)]
struct Mark {
  index: MarkIndex,
  waiter: Option<Sender<()>>,
  done: bool,
}

#[derive(Debug)]
struct Inner {
  done_until: CachePadded<AtomicU64>,
  last_index: CachePadded<AtomicU64>,
  name: Cow<'static, str>,
  mark_tx: Sender<Mark>,
  mark_rx: Receiver<Mark>,
}

impl Inner {
  fn process(&self, closer: Closer) {
    scopeguard::defer!(closer.done(););

    let mut indices: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
    // pending maps raft proposal index to the number of pending mutations for this proposal.
    let pending: RefCell<HashMap<u64, i64>> = RefCell::new(HashMap::new());
    let waiters: RefCell<HashMap<u64, MediumVec<Sender<()>>>> = RefCell::new(HashMap::new());

    let mut process_one = |idx: u64, done: bool| {
      // If not already done, then set. Otherwise, don't undo a done entry.
      let mut pending = pending.borrow_mut();
      let mut waiters = waiters.borrow_mut();

      if !pending.contains_key(&idx) {
        indices.push(Reverse(idx));
      }

      let mut delta = 1;
      if done {
        delta = -1;
      }
      pending
        .entry(idx)
        .and_modify(|v| *v += delta)
        .or_insert(delta);

      // Update mark by going through all indices in order; and checking if they have
      // been done. Stop at the first index, which isn't done.
      let done_until = self.done_until.load(Ordering::SeqCst);
      assert!(
        done_until <= idx,
        "name: {}, done_until: {}, idx: {}",
        self.name,
        done_until,
        idx
      );

      let mut until = done_until;

      while !indices.is_empty() {
        let min = indices.peek().unwrap().0;
        if let Some(done) = pending.get(&min) {
          if done.gt(&0) {
            break; // len(indices) will be > 0.
          }
        }
        // Even if done is called multiple times causing it to become
        // negative, we should still pop the index.
        indices.pop();
        pending.remove(&min);
        until = min;
      }

      if until != done_until {
        assert_eq!(
          self
            .done_until
            .compare_exchange(done_until, until, Ordering::SeqCst, Ordering::Acquire),
          Ok(done_until)
        );
      }

      if until - done_until <= waiters.len() as u64 {
        // Close channel and remove from waiters.
        (done_until + 1..=until).for_each(|idx| {
          let _ = waiters.remove(&idx);
        });
      } else {
        // Close and drop idx <= util channels.
        waiters.retain(|idx, _| *idx > until);
      }
    };

    let closer = closer.has_been_closed();
    loop {
      select! {
        recv(closer) -> _ => return,
        recv(self.mark_rx) -> mark => match mark {
          Ok(mark) => {
            if let Some(wait_tx) = mark.waiter {
              if let MarkIndex::Single(index) = mark.index {
                let done_until = self.done_until.load(Ordering::SeqCst);
                if done_until >= index {
                  let _ = wait_tx; // Close channel.
                } else {
                  waiters.borrow_mut().entry(index).or_default().push(wait_tx);
                }
              }
            } else {
              match mark.index {
                MarkIndex::Single(idx) => process_one(idx, mark.done),
                MarkIndex::Multiple(indices) => indices.into_iter().for_each(|idx| process_one(idx, mark.done)),
              }
            }
          },
          Err(_) => {
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

/// WaterMark is used to keep track of the minimum un-finished index. Typically, an index k becomes
/// finished or "done" according to a WaterMark once `done(k)` has been called
///  1. as many times as `begin(k)` has, AND
///  2. a positive number of times.
///
/// An index may also become "done" by calling `set_done_until` at a time such that it is not
/// inter-mingled with `begin/done` calls.
///
/// Since `done_until` and `last_index` addresses are passed to sync/atomic packages, we ensure that they
/// are 64-bit aligned by putting them at the beginning of the structure.
#[derive(Debug)]
pub struct WaterMark {
  inner: Arc<Inner>,
  initialized: bool,
}

impl WaterMark {
  /// Create a new WaterMark with the given name.
  ///
  /// **Note**: Before using the watermark, you must call `init` to start the background thread.
  #[inline]
  pub fn new(name: Cow<'static, str>) -> Self {
    let (mark_tx, mark_rx) = bounded(100);
    Self {
      inner: Arc::new(Inner {
        done_until: CachePadded::new(AtomicU64::new(0)),
        last_index: CachePadded::new(AtomicU64::new(0)),
        name,
        mark_tx,
        mark_rx,
      }),
      initialized: false,
    }
  }

  /// Returns the name of the watermark.
  #[inline(always)]
  pub fn name(&self) -> &str {
    self.inner.name.as_ref()
  }

  /// Initializes a WaterMark struct. MUST be called before using it.
  #[inline]
  pub fn init(&mut self, closer: Closer) {
    if self.initialized {
      return;
    }

    let inner = self.inner.clone();
    std::thread::spawn(move || {
      inner.process(closer);
    });
  }

  /// Sets the last index to the given value.
  #[inline]
  pub fn begin(&self, index: u64) -> Result<()> {
    self.check().map(|_| {
      self.inner.last_index.store(index, Ordering::SeqCst);
      self
        .inner
        .mark_tx
        .send(Mark {
          index: MarkIndex::Single(index),
          waiter: None,
          done: false,
        })
        .unwrap()
    })
  }

  /// Works like [`begin`] but accepts multiple indices.
  #[inline]
  pub fn begin_many(&self, indices: MediumVec<u64>) -> Result<()> {
    if indices.is_empty() {
      return Ok(());
    }

    self.check().map(|_| {
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
        .unwrap()
    })
  }

  /// Sets a single index as done.
  #[inline]
  pub fn done(&self, index: u64) -> Result<()> {
    self.check().map(|_| {
      self
        .inner
        .mark_tx
        .send(Mark {
          index: MarkIndex::Single(index),
          waiter: None,
          done: true,
        })
        .unwrap() // unwrap is safe because self also holds a receiver
    })
  }

  /// Sets multiple indices as done.
  #[inline]
  pub fn done_many(&self, indices: MediumVec<u64>) -> Result<()> {
    self.check().map(|_| {
      self
        .inner
        .mark_tx
        .send(Mark {
          index: MarkIndex::Multiple(indices),
          waiter: None,
          done: true,
        })
        .unwrap() // unwrap is safe because self also holds a receiver
    })
  }

  /// Returns the maximum index that has the property that all indices
  /// less than or equal to it are done.
  #[inline]
  pub fn done_until(&self) -> Result<u64> {
    self
      .check()
      .map(|_| self.inner.done_until.load(Ordering::SeqCst))
  }

  /// Sets the maximum index that has the property that all indices
  /// less than or equal to it are done.
  #[inline]
  pub fn set_done_util(&self, val: u64) -> Result<()> {
    self
      .check()
      .map(|_| self.inner.done_until.store(val, Ordering::SeqCst))
  }

  /// Returns the last index for which `begin` has been called.
  #[inline]
  pub fn last_index(&self) -> Result<u64> {
    self
      .check()
      .map(|_| self.inner.last_index.load(Ordering::SeqCst))
  }

  /// Waits until the given index is marked as done.
  #[inline]
  pub fn wait_for_mark(&self, index: u64) -> Result<()> {
    self.check().map(|_| {
      if self.inner.done_until.load(Ordering::SeqCst) >= index {
        return;
      }

      let (wait_tx, wait_rx) = bounded(1);
      self
        .inner
        .mark_tx
        .send(Mark {
          index: MarkIndex::Single(index),
          waiter: Some(wait_tx),
          done: false,
        })
        .unwrap(); // unwrap is safe because self also holds a receiver

      let _ = wait_rx.recv();
    })
  }

  #[inline(always)]
  fn check(&self) -> Result<()> {
    if !self.initialized {
      return Err(WaterMarkError::Uninitialized);
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn init_and_close<F>(f: F)
  where
    F: FnOnce(&WaterMark),
  {
    let closer = Closer::new(1);

    let mut watermark = WaterMark::new("watermark".into());
    watermark.init(closer.clone());

    f(&watermark);

    closer.signal_and_wait();
  }

  #[test]
  fn test_basic() {
    init_and_close(|_| {});
  }

  #[test]
  fn test_begin_done() {
    init_and_close(|watermark| {
      watermark.begin(1).unwrap();
      watermark.begin_many([2, 3].into_iter().collect()).unwrap();

      watermark.done(1).unwrap();
      watermark.done_many([2, 3].into_iter().collect()).unwrap();
    });
  }

  #[test]
  fn test_wait_for_mark() {
    init_and_close(|watermark| {
      watermark
        .begin_many([1, 2, 3].into_iter().collect())
        .unwrap();
      watermark.done_many([2, 3].into_iter().collect()).unwrap();

      assert_eq!(watermark.done_until().unwrap(), 0);

      watermark.done(1).unwrap();
      watermark.wait_for_mark(1).unwrap();
      watermark.wait_for_mark(3).unwrap();
      assert_eq!(watermark.done_until().unwrap(), 3);
    });
  }

  #[test]
  fn test_last_index() {
    init_and_close(|watermark| {
      watermark
        .begin_many([1, 2, 3].into_iter().collect())
        .unwrap();
      watermark.done_many([2, 3].into_iter().collect()).unwrap();

      assert_eq!(watermark.last_index().unwrap(), 3);
    });
  }

  #[test]
  fn test_multiple_singles() {
    let closer = Closer::default();
    closer.signal();
    closer.signal();
    closer.signal_and_wait();

    let closer = Closer::new(1);
    closer.done();
    closer.signal_and_wait();
    closer.signal_and_wait();
    closer.signal();
  }

  #[test]
  fn test_closer() {
    let closer = Closer::new(1);
    let tc = closer.clone();
    std::thread::spawn(move || {
      if let Err(err) = tc.has_been_closed().recv() {
        eprintln!("err: {}", err);
      }
      tc.done();
    });
    closer.signal_and_wait();
  }

  #[test]
  fn test_closer_() {
    use core::time::Duration;
    use crossbeam_channel::unbounded;

    let (tx, rx) = unbounded();

    let c = Closer::default();

    for _ in 0..10 {
      let c = c.clone();
      let tx = tx.clone();
      std::thread::spawn(move || {
        assert!(c.has_been_closed().recv().is_err());
        tx.send(()).unwrap();
      });
    }
    c.signal();
    for _ in 0..10 {
      rx.recv_timeout(Duration::from_millis(1000)).unwrap();
    }
  }
}

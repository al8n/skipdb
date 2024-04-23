use std::sync::{
  atomic::{AtomicPtr, Ordering},
  Arc,
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use wg::WaitGroup;

#[derive(Debug)]
struct Canceler {
  tx: AtomicPtr<()>,
}

impl Canceler {
  #[inline]
  fn cancel(&self) {
    // Safely take the sender out of the AtomicPtr.
    let tx_ptr = self.tx.swap(std::ptr::null_mut(), Ordering::AcqRel);

    // Check if the pointer is not null (indicating it hasn't been taken already).
    if !tx_ptr.is_null() {
      // Safe because we ensure that this is the only place that takes ownership of the pointer,
      // and it is done only once.
      unsafe {
        // Convert the pointer back to a Box to take ownership and drop the sender.
        let _ = Box::from_raw(tx_ptr as *mut Sender<()>);
        // Sender is dropped here when `_tx_boxed` goes out of scope.
      }
    }
  }
}

impl Drop for Canceler {
  fn drop(&mut self) {
    self.cancel();
  }
}

#[derive(Debug)]
#[repr(transparent)]
struct CancelContext {
  rx: Receiver<()>,
}

impl CancelContext {
  fn new() -> (Self, Canceler) {
    let (tx, rx) = unbounded();
    (
      Self { rx },
      Canceler {
        tx: AtomicPtr::new(Box::into_raw(Box::new(tx)) as _),
      },
    )
  }

  #[inline]
  fn done(&self) -> Receiver<()> {
    self.rx.clone()
  }
}

/// Closer holds the two things we need to close a thread and wait for it to
/// finish: a chan to tell the thread to shut down, and a WaitGroup with
/// which to wait for it to finish shutting down.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Closer {
  inner: Arc<CloserInner>,
}

#[derive(Debug)]
struct CloserInner {
  wg: WaitGroup,
  ctx: CancelContext,
  cancel: Canceler,
}

impl CloserInner {
  #[inline]
  fn new() -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      wg: WaitGroup::new(),
      ctx,
      cancel,
    }
  }

  #[inline]
  fn with(initial: usize) -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      wg: WaitGroup::from(initial),
      ctx,
      cancel,
    }
  }
}

impl Default for Closer {
  fn default() -> Self {
    Self {
      inner: Arc::new(CloserInner::new()),
    }
  }
}

impl Closer {
  /// Constructs a new [`Closer`], with an initial count on the [`WaitGroup`].
  #[inline]
  pub fn new(initial: usize) -> Self {
    Self {
      inner: Arc::new(CloserInner::with(initial)),
    }
  }

  /// Adds delta to the [`WaitGroup`].
  #[inline]
  pub fn add_running(&self, running: usize) {
    self.inner.wg.add(running);
  }

  /// Calls [`WaitGroup::done`] on the [`WaitGroup`].
  #[inline]
  pub fn done(&self) {
    self.inner.wg.done();
  }

  /// Signals the [`Closer::has_been_closed`] signal.
  #[inline]
  pub fn signal(&self) {
    self.inner.cancel.cancel();
  }

  /// Waits on the [`WaitGroup`]. (It waits for the Closer's initial value, [`Closer::add_running`], and [`Closer::done`]
  /// calls to balance out.)
  #[inline]
  pub fn wait(&self) {
    self.inner.wg.wait();
  }

  /// Calls [`Closer::signal`], then [`Closer::wait`].
  #[inline]
  pub fn signal_and_wait(&self) {
    self.signal();
    self.wait();
  }

  /// Listens for the [`Closer::signal`] signal.
  pub fn listen(&self) -> Receiver<()> {
    self.inner.ctx.done()
  }
}

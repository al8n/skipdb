use core::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "std")]
use std::sync::Arc;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, sync::Arc};

use tokio::sync::Notify;
use wg::tokio::AsyncWaitGroup as WaitGroup;

/// Canceled is a type that is used to signal that a thread has been canceled.
#[derive(thiserror::Error, Debug)]
#[error("closer has been canceled")]
pub struct Canceled;

#[derive(Debug)]
struct Canceler {
  ntf: Arc<Notify>,
  notified: Arc<AtomicBool>,
}

impl Canceler {
  #[inline]
  fn cancel(&self) {
    if !self.notified.swap(true, Ordering::AcqRel) {
      self.ntf.notify_waiters();
    }
  }
}

impl Drop for Canceler {
  fn drop(&mut self) {
    self.cancel();
  }
}

#[derive(Debug)]
struct CancelContext {
  ntf: Arc<Notify>,
  canceled: Arc<AtomicBool>,
}

impl CancelContext {
  fn new() -> (Self, Canceler) {
    let ntf = Arc::new(Notify::new());
    let canceled = Arc::new(AtomicBool::new(false));
    (
      Self {
        ntf: ntf.clone(),
        canceled: canceled.clone(),
      },
      Canceler {
        ntf,
        notified: canceled,
      },
    )
  }

  #[inline]
  async fn done(&self) -> Result<(), Canceled> {
    if self.canceled.load(Ordering::Acquire) {
      return Err(Canceled);
    }
    self.ntf.notified().await;
    Ok(())
  }
}

/// TokioCloser holds the two things we need to close a thread and wait for it to
/// finish: a chan to tell the thread to shut down, and a WaitGroup with
/// which to wait for it to finish shutting down.
#[derive(Debug, Clone)]
pub struct TokioCloser {
  inner: Arc<TokioCloserInner>,
}

#[derive(Debug)]
struct TokioCloserInner {
  wg: WaitGroup,
  ctx: CancelContext,
  cancel: Canceler,
}

impl TokioCloserInner {
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
  fn new_with_initial(initial: usize) -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      wg: WaitGroup::from(initial),
      ctx,
      cancel,
    }
  }
}

impl Default for TokioCloser {
  fn default() -> Self {
    Self {
      inner: Arc::new(TokioCloserInner::new()),
    }
  }
}

impl TokioCloser {
  /// Constructs a new [`TokioCloser`], with an initial count on the [`WaitGroup`].
  #[inline]
  pub fn new(initial: usize) -> Self {
    Self {
      inner: Arc::new(TokioCloserInner::new_with_initial(initial)),
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

  /// Signals the [`TokioCloser::has_been_closed`] signal.
  #[inline]
  pub fn signal(&self) {
    self.inner.cancel.cancel();
  }

  /// Gets signaled when [`TokioCloser::signal`] is called.
  #[inline]
  pub async fn has_been_closed(&self) -> Result<(), Canceled> {
    self.inner.ctx.done().await
  }

  /// Waits on the [`WaitGroup`]. (It waits for the TokioCloser's initial value, [`TokioCloser::add_running`], and [`TokioCloser::done`]
  /// calls to balance out.)
  #[inline]
  pub async fn wait(&self) {
    self.inner.wg.wait().await;
  }

  /// Calls [`TokioCloser::signal`], then [`TokioCloser::wait`].
  #[inline]
  pub async fn signal_and_wait(&self) {
    self.signal();
    self.wait().await;
  }
}

impl TokioCloser {
  /// Calls [`TokioCloser::signal`], then [`TokioCloser::wait`].
  #[inline]
  pub fn signal_and_wait_blocking(&self) {
    self.signal();
    self.inner.wg.block_wait();
  }
}

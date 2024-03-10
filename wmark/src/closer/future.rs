#[cfg(feature = "std")]
use std::sync::Arc;

#[cfg(not(feature = "std"))]
use alloc::sync::Arc;

use async_channel::{unbounded, Receiver, Sender};
use wg::future::AsyncWaitGroup as WaitGroup;

#[derive(Debug)]
struct Canceler {
  tx: Sender<()>,
}

impl Canceler {
  #[inline]
  fn cancel(&self) {
    self.tx.close();
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
    (Self { rx }, Canceler { tx })
  }

  #[inline]
  fn done(&self) -> Receiver<()> {
    self.rx.clone()
  }
}

/// AsyncCloser holds the two things we need to close a thread and wait for it to
/// finish: a chan to tell the thread to shut down, and a WaitGroup with
/// which to wait for it to finish shutting down.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct AsyncCloser {
  inner: Arc<AsyncCloserInner>,
}

#[derive(Debug)]
struct AsyncCloserInner {
  wg: WaitGroup,
  ctx: CancelContext,
  cancel: Canceler,
}

impl AsyncCloserInner {
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

impl Default for AsyncCloser {
  fn default() -> Self {
    Self {
      inner: Arc::new(AsyncCloserInner::new()),
    }
  }
}

impl AsyncCloser {
  /// Constructs a new [`AsyncCloser`], with an initial count on the [`WaitGroup`].
  #[inline]
  pub fn new(initial: usize) -> Self {
    Self {
      inner: Arc::new(AsyncCloserInner::new_with_initial(initial)),
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

  /// Signals the [`AsyncCloser::has_been_closed`] signal.
  #[inline]
  pub fn signal(&self) {
    self.inner.cancel.cancel();
  }

  /// Gets signaled when [`AsyncCloser::signal`] is called.
  #[inline]
  pub fn has_been_closed(&self) -> Receiver<()> {
    self.inner.ctx.done()
  }

  /// Waits on the [`WaitGroup`]. (It waits for the AsyncCloser's initial value, [`AsyncCloser::add_running`], and [`AsyncCloser::done`]
  /// calls to balance out.)
  #[inline]
  pub async fn wait(&self) {
    self.inner.wg.wait().await;
  }

  /// Calls [`AsyncCloser::signal`], then [`AsyncCloser::wait`].
  #[inline]
  pub async fn signal_and_wait(&self) {
    self.signal();
    self.wait().await;
  }
}

use core::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "std")]
use std::sync::Arc;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, sync::Arc};

use async_channel::{unbounded, Receiver, Sender};
use event_listener::{Event, Listener};

use crate::AsyncSpawner;

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
#[derive(Debug)]
pub struct AsyncCloser<S> {
  inner: Arc<AsyncCloserInner>,
  _spawner: core::marker::PhantomData<S>,
}

impl<S> Clone for AsyncCloser<S> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      _spawner: core::marker::PhantomData,
    }
  }
}

#[derive(Debug)]
struct AsyncCloserInner {
  waitings: AtomicUsize,
  event: Event,
  ctx: CancelContext,
  cancel: Canceler,
}

impl AsyncCloserInner {
  #[inline]
  fn new() -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      waitings: AtomicUsize::new(0),
      event: Event::new(),
      ctx,
      cancel,
    }
  }

  #[inline]
  fn new_with_initial(initial: usize) -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      waitings: AtomicUsize::new(initial),
      event: Event::new(),
      ctx,
      cancel,
    }
  }
}

impl<S> Default for AsyncCloser<S> {
  fn default() -> Self {
    Self {
      inner: Arc::new(AsyncCloserInner::new()),
      _spawner: core::marker::PhantomData,
    }
  }
}

impl<S> AsyncCloser<S> {
  /// Constructs a new [`AsyncCloser`], with an initial count on the [`WaitGroup`].
  #[inline]
  pub fn new(initial: usize) -> Self {
    Self {
      inner: Arc::new(AsyncCloserInner::new_with_initial(initial)),
      _spawner: core::marker::PhantomData,
    }
  }

  /// Adds delta to the [`WaitGroup`].
  #[inline]
  pub fn add_running(&self, running: usize) {
    self.inner.waitings.fetch_add(running, Ordering::SeqCst);
  }

  /// Calls [`WaitGroup::done`] on the [`WaitGroup`].
  #[inline]
  pub fn done(&self) {
    if self
      .inner
      .waitings
      .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
        if v != 0 {
          Some(v - 1)
        } else {
          None
        }
      })
      .is_ok()
    {
      self.inner.event.notify(usize::MAX);
    }
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
    while self.inner.waitings.load(Ordering::SeqCst) != 0 {
      let ln = self.inner.event.listen();
      // Check the flag again after creating the listener.
      if self.inner.waitings.load(Ordering::SeqCst) == 0 {
        return;
      }
      ln.await;
    }
  }

  /// Calls [`AsyncCloser::signal`], then [`AsyncCloser::wait`].
  #[inline]
  pub async fn signal_and_wait(&self) {
    self.signal();
    self.wait().await;
  }
}

impl<S: AsyncSpawner> AsyncCloser<S> {
  /// Waits on the [`WaitGroup`]. (It waits for the AsyncCloser's initial value, [`AsyncCloser::add_running`], and [`AsyncCloser::done`]
  /// calls to balance out.)
  #[inline]
  pub fn blocking_wait(&self) {
    while self.inner.waitings.load(Ordering::SeqCst) != 0 {
      let ln = self.inner.event.listen();
      // Check the flag again after creating the listener.
      if self.inner.waitings.load(Ordering::SeqCst) == 0 {
        return;
      }
      ln.wait();
    }
  }

  /// Like [`AsyncCloser::signal_and_wait`], but spawns and detach the waiting in a new task.
  #[inline]
  pub fn signal_and_wait_detach(&self) {
    self.signal();
    let wg = self.clone();
    S::spawn_detach(async move {
      wg.wait().await;
    })
  }
}

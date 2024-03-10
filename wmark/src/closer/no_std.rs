use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crossbeam_queue::SegQueue;

use alloc::sync::Arc;

#[derive(Debug)]
struct Canceler {
  signal: Arc<Channel<()>>,
}

impl Canceler {
  #[inline]
  fn cancel(&self) {
    if !self.signal.signal.swap(true, Ordering::Release) {
      while self.signal.queue.pop().is_some() {}
    }
  }
}

impl Drop for Canceler {
  fn drop(&mut self) {
    self.cancel();
  }
}

#[derive(Debug)]
struct Channel<T> {
  signal: AtomicBool,
  queue: SegQueue<T>,
}

#[derive(Debug)]
pub struct RecvError;

impl core::fmt::Display for RecvError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "recv on closed channel")
  }
}

/// A receiver
#[derive(Debug)]
pub struct Receiver<T>(Arc<Channel<T>>);

impl<T> Clone for Receiver<T> {
  #[inline]
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<T> Receiver<T> {
  #[inline]
  pub fn recv(&self) -> Result<T, RecvError> {
    if self.0.signal.load(Ordering::Acquire) {
      return Err(RecvError);
    }

    loop {
      match self.0.queue.pop() {
        Some(v) => return Ok(v),
        None => {
          if self.0.signal.load(Ordering::Acquire) {
            return Err(RecvError);
          }
        }
      }
    }
  }

  #[inline]
  pub fn try_recv(&self) -> Option<T> {
    self.0.queue.pop()
  }
}

#[derive(Debug)]
#[repr(transparent)]
struct CancelContext {
  rx: Receiver<()>,
}

impl CancelContext {
  fn new() -> (Self, Canceler) {
    let channel = Arc::new(Channel {
      signal: AtomicBool::new(false),
      queue: SegQueue::new(),
    });
    (
      Self {
        rx: Receiver(channel.clone()),
      },
      Canceler { signal: channel },
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
  wg: AtomicUsize,
  ctx: CancelContext,
  cancel: Canceler,
}

impl CloserInner {
  #[inline]
  fn new() -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      wg: AtomicUsize::new(0),
      ctx,
      cancel,
    }
  }

  #[inline]
  fn new_with_initial(initial: usize) -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      wg: AtomicUsize::new(initial),
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
      inner: Arc::new(CloserInner::new_with_initial(initial)),
    }
  }

  /// Adds delta to the [`WaitGroup`].
  #[inline]
  pub fn add_running(&self, running: usize) {
    self.inner.wg.fetch_add(running, Ordering::AcqRel);
  }

  /// Calls [`WaitGroup::done`] on the [`WaitGroup`].
  #[inline]
  pub fn done(&self) {
    self.inner.wg.fetch_sub(1, Ordering::AcqRel);
  }

  /// Signals the [`Closer::has_been_closed`] signal.
  #[inline]
  pub fn signal(&self) {
    self.inner.cancel.cancel();
  }

  /// Gets signaled when [`Closer::signal`] is called.
  #[inline]
  pub fn has_been_closed(&self) -> Receiver<()> {
    self.inner.ctx.done()
  }

  /// Waits on the [`WaitGroup`]. (It waits for the Closer's initial value, [`Closer::add_running`], and [`Closer::done`]
  /// calls to balance out.)
  #[inline]
  pub fn wait(&self) {
    while self.inner.wg.load(Ordering::Acquire) != 0 {
      core::hint::spin_loop();
    }
  }

  /// Calls [`Closer::signal`], then [`Closer::wait`].
  #[inline]
  pub fn signal_and_wait(&self) {
    self.signal();
    self.wait();
  }
}

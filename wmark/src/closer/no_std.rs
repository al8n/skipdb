use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crossbeam_queue::SegQueue;
use event_listener::{Event, Listener};

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

/// Receive error
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
  /// Receives a message from the channel.
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

  /// Tries to receive a message from the channel.
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
  event: Event,
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
      event: Event::new(),
    }
  }

  #[inline]
  fn with(initial: usize) -> Self {
    let (ctx, cancel) = CancelContext::new();
    Self {
      wg: AtomicUsize::new(initial),
      ctx,
      cancel,
      event: Event::new(),
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
    self.inner.wg.fetch_add(running, Ordering::AcqRel);
  }

  /// Calls [`WaitGroup::done`] on the [`WaitGroup`].
  #[inline]
  pub fn done(&self) {
    if self
      .inner
      .wg
      .fetch_update(Ordering::Release, Ordering::Acquire, |v| {
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

  /// Signals the [`Closer::listen`] signal.
  #[inline]
  pub fn signal(&self) {
    self.inner.cancel.cancel();
  }

  /// Gets signaled when [`Closer::signal`] is called.
  #[inline]
  pub fn listen(&self) -> Receiver<()> {
    self.inner.ctx.done()
  }

  /// Waits on the [`WaitGroup`]. (It waits for the Closer's initial value, [`Closer::add_running`], and [`Closer::done`]
  /// calls to balance out.)
  #[inline]
  pub fn wait(&self) {
    while self.inner.wg.load(Ordering::SeqCst) != 0 {
      let ln = self.inner.event.listen();
      // Check the flag again after creating the listener.
      if self.inner.wg.load(Ordering::SeqCst) == 0 {
        break;
      }
      ln.wait();
    }
  }

  /// Calls [`Closer::signal`], then [`Closer::wait`].
  #[inline]
  pub fn signal_and_wait(&self) {
    self.signal();
    self.wait();
  }
}

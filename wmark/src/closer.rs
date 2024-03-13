#[cfg(feature = "std")]
mod std_;
#[cfg(feature = "std")]
pub use std_::*;

#[cfg(feature = "future")]
mod future;
#[cfg(feature = "future")]
pub use future::*;

#[cfg(feature = "tokio")]
mod tokio_;
#[cfg(feature = "tokio")]
pub use tokio_::*;

/// Closer implementations for no_std environments.
#[cfg(feature = "core")]
#[cfg_attr(docsrs, doc(cfg(feature = "core")))]
pub(crate) mod no_std;

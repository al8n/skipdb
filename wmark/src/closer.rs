#[cfg(feature = "std")]
mod std_;
#[cfg(feature = "std")]
pub use std_::*;

#[cfg(feature = "future")]
mod future;
#[cfg(feature = "future")]
pub use future::*;

/// Closer implementations for no_std environments.
#[cfg(feature = "core")]
#[cfg_attr(docsrs, doc(cfg(feature = "core")))]
pub(crate) mod no_std;

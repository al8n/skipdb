#[cfg(feature = "std")]
mod std_;
#[cfg(feature = "std")]
pub use std_::*;

#[cfg(feature = "future")]
mod future;
#[cfg(feature = "future")]
pub use future::*;

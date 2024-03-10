#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "core")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod closer;

#[cfg(feature = "std")]
pub use closer::Closer;

#[cfg(feature = "future")]
pub use closer::AsyncCloser;

mod watermark;

#[cfg(feature = "std")]
pub use watermark::{WaterMark, WaterMarkError};

#[cfg(feature = "future")]
pub use watermark::AsyncWaterMark;

/// For use in no_std environments.
#[cfg(feature = "core")]
#[cfg_attr(docsrs, doc(cfg(feature = "core")))]
pub mod no_std {
  // pub use super::watermark::no_std::*;
  pub use super::closer::no_std::*;
}

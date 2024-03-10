#[cfg(feature = "std")]
mod std_;
#[cfg(feature = "std")]
pub use std_::*;

#[cfg(feature = "future")]
mod future;
#[cfg(feature = "future")]
pub use future::*;

/// Watermark implementations for no_std environments.
#[cfg(feature = "core")]
#[cfg_attr(docsrs, doc(cfg(feature = "core")))]
pub(crate) mod no_std;

/// Error type for watermark.
pub enum WaterMarkError {
  /// The watermark is uninitialized, please call init first before using any other functions
  Uninitialized,
}

impl core::fmt::Debug for WaterMarkError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Uninitialized => write!(
        f,
        "watermark: uninitialized, please call init first before using any other functions"
      ),
    }
  }
}

impl core::fmt::Display for WaterMarkError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Uninitialized => write!(
        f,
        "watermark: uninitialized, please call init first before using any other functions"
      ),
    }
  }
}

#[cfg(feature = "std")]
impl std::error::Error for WaterMarkError {}

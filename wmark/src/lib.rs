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

#[cfg(feature = "tokio")]
pub use closer::TokioCloser;

mod watermark;

#[cfg(feature = "std")]
pub use watermark::{WaterMark, WaterMarkError};

#[cfg(feature = "future")]
#[cfg_attr(docsrs, doc(cfg(feature = "future")))]
pub use watermark::future::AsyncWaterMark;

#[cfg(feature = "future")]
#[cfg_attr(docsrs, doc(cfg(feature = "future")))]
pub use wg::future::AsyncSpawner;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use watermark::tokio::TokioWaterMark;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use wg::future::TokioSpawner;

#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub use wg::future::AsyncStdSpawner;

#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub use wg::future::SmolSpawner;

/// For use in no_std environments.
#[cfg(feature = "core")]
#[cfg_attr(docsrs, doc(cfg(feature = "core")))]
pub mod no_std {
  // pub use super::watermark::no_std::*;
  pub use super::closer::no_std::*;
}

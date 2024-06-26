//! `Watermark` and `Closer` implementation for implementing transaction.
#![allow(clippy::type_complexity)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
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
#[cfg_attr(docsrs, doc(cfg(feature = "future")))]
pub use watermark::future::AsyncWaterMark;

#[cfg(feature = "future")]
#[cfg_attr(docsrs, doc(cfg(feature = "future")))]
pub use agnostic_lite::{AsyncSpawner, Detach};

#[cfg(all(feature = "tokio", feature = "future"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tokio", feature = "future"))))]
pub use agnostic_lite::tokio::TokioSpawner;

#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub use agnostic_lite::async_std::AsyncStdSpawner;

#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub use agnostic_lite::smol::SmolSpawner;

#[cfg(feature = "wasm")]
#[cfg_attr(docsrs, doc(cfg(feature = "wasm")))]
pub use agnostic_lite::wasm::WasmSpawner;

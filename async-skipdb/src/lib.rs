//! Blazing fast ACID and MVCC in memory database.
//!
//! `skipdb` uses the same SSI (Serializable Snapshot Isolation) transaction model used in [`badger`](https://github.com/dgraph-io/badger).
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs, warnings)]
#![forbid(unsafe_code)]
#![allow(clippy::type_complexity)]

use std::{borrow::Borrow, hash::BuildHasher, ops::RangeBounds, sync::Arc};

use async_txn::{error::TransactionError, AsyncRtm, AsyncTm, AsyncWtm, HashCm, HashCmOptions, Pwm};

/// `EquivalentDB` implementation, which requires `K` implements both [`Hash`](core::hash::Hash) and [`Ord`].
/// If your `K` does not implement [`Hash`](core::hash::Hash), you can use [`ComparableDB`] instead.
pub mod equivalent;

/// `ComparableDB` implementation, which requires `K` implements [`Ord`] and [`CheapClone`](cheap_clone::CheapClone). If your `K` implements both [`Hash`](core::hash::Hash) and [`Ord`], you are recommended to use [`EquivalentDB`](crate::equivalent::EquivalentDB) instead.
pub mod comparable;

pub use skipdb_core::{
  iter::*,
  range::*,
  rev_iter::*,
  types::{Ref, ValueRef},
  Options,
};

use skipdb_core::{AsSkipCore, Database, PendingMap, SkipCore};

mod read;
pub use read::*;

pub use async_txn::{AsyncSpawner, Detach};

#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub use async_txn::SmolSpawner;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use async_txn::TokioSpawner;

#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub use async_txn::AsyncStdSpawner;

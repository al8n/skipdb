// #![allow(clippy::type_complexity)]

// use std::{cmp::Reverse, collections::BTreeMap, hash::BuildHasher, mem};

// use either::Either;

// use async_mwmr::{tests::*, *};

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// struct TestKey {
//   key: u64,
//   version: u64,
// }

// impl TestKey {
//   fn new(key: u64, version: u64) -> Self {
//     Self { key, version }
//   }
// }

// impl PartialOrd for TestKey {
//   fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//     Some(self.cmp(other))
//   }
// }

// impl Ord for TestKey {
//   fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//     self
//       .version
//       .cmp(&other.version)
//       .then(self.key.cmp(&other.key))
//   }
// }

// struct TestBTreeDbIter {
//   pendings: BTreeMap<Reverse<TestKey>, Option<u64>>,
//   data: Either<BTreeMap<Reverse<u64>, (u64, Option<u64>)>, BTreeMap<Reverse<TestKey>, Option<u64>>>,
//   reverse: bool,
// }

// impl IntoIterator for TestBTreeDbIter {
//   type Item = (u64, u64, Option<u64>);
//   type IntoIter = std::vec::IntoIter<Self::Item>;

//   fn into_iter(self) -> Self::IntoIter {
//     if self.reverse {
//       let mut data = match self.data {
//         Either::Left(data) => IntoIterator::into_iter(self.pendings)
//           .rev()
//           .map(|(k, v)| (k.0.version, k.0.key, v))
//           .chain(
//             IntoIterator::into_iter(data)
//               .rev()
//               .map(|(k, (version, val))| (version, k.0, val)),
//           )
//           .collect::<Vec<_>>(),
//         Either::Right(data) => IntoIterator::into_iter(self.pendings)
//           .rev()
//           .chain(IntoIterator::into_iter(data).rev())
//           .map(|(k, v)| (k.0.version, k.0.key, v))
//           .collect::<Vec<_>>(),
//       };
//       data.sort_by(|a, b| a.1.cmp(&b.1));
//       data.into_iter()
//     } else {
//       let mut data = match self.data {
//         Either::Left(data) => IntoIterator::into_iter(self.pendings)
//           .map(|(k, v)| (k.0.version, k.0.key, v))
//           .chain(IntoIterator::into_iter(data).map(|(k, (version, val))| (version, k.0, val)))
//           .collect::<Vec<_>>(),
//         Either::Right(data) => IntoIterator::into_iter(self.pendings)
//           .chain(data)
//           .map(|(k, v)| (k.0.version, k.0.key, v))
//           .collect::<Vec<_>>(),
//       };

//       data.sort_by(|a, b| b.1.cmp(&a.1));
//       data.into_iter()
//     }
//   }
// }

// struct TestBTreeDbKeys {}

// #[derive(Debug)]
// struct TestBTreeDb {
//   max_batch_size: u64,
//   max_batch_entries: u64,
//   store: futures::lock::Mutex<BTreeMap<TestKey, Option<u64>>>,
//   hash: std::hash::RandomState,
// }

// impl TestBTreeDb {
//   async fn iter_all_versions<'a, 'b: 'a>(
//     &'a self,
//     pending: impl Iterator<Item = EntryRef<'b, u64, u64>> + 'b,
//     max_version: u64,
//     since_version: u64,
//     reverse: bool,
//   ) -> TestBTreeDbIter {
//     let pendings = pending
//       .map(|ent| match ent.data() {
//         EntryDataRef::Insert { key, value, .. } => {
//           (Reverse(TestKey::new(**key, ent.version())), Some(**value))
//         }
//         EntryDataRef::Remove(k) => (Reverse(TestKey::new(**k, ent.version())), None),
//       })
//       .collect::<BTreeMap<_, _>>();

//     let data = self
//       .store
//       .lock()
//       .await
//       .iter()
//       .filter_map(|(k, v)| {
//         if k.version > max_version || k.version <= since_version {
//           None
//         } else {
//           match v {
//             Some(v) => Some((Reverse(*k), Some(*v))),
//             None => Some((Reverse(*k), None)),
//           }
//         }
//       })
//       .collect::<BTreeMap<_, _>>();

//     TestBTreeDbIter {
//       pendings,
//       data: Either::Right(data),
//       reverse,
//     }
//   }

//   async fn iter<'a, 'b: 'a>(
//     &'a self,
//     pending: impl Iterator<Item = EntryRef<'b, u64, u64>> + 'b,
//     max_version: u64,
//     since_version: u64,
//     reverse: bool,
//   ) -> TestBTreeDbIter {
//     let pendings = pending
//       .map(|ent| match ent.data() {
//         EntryDataRef::Insert { key, value, .. } => (**key, Some(**value)),
//         EntryDataRef::Remove(k) => (**k, None),
//       })
//       .collect::<BTreeMap<_, _>>();

//     let mut data = BTreeMap::new();
//     for (k, v) in self.store.lock().await.iter() {
//       if pendings.contains_key(&k.key) {
//         continue;
//       }

//       if k.version > max_version || k.version <= since_version {
//         continue;
//       }

//       data
//         .entry(Reverse(k.key))
//         .and_modify(|(old_version, old_val)| {
//           if *old_version < k.version {
//             *old_version = k.version;
//             *old_val = *v;
//           }
//         })
//         .or_insert((k.version, *v));
//     }

//     TestBTreeDbIter {
//       pendings: IntoIterator::into_iter(pendings)
//         .map(|(k, v)| (Reverse(TestKey::new(k, max_version)), v))
//         .collect(),
//       data: Either::Left(data),
//       reverse,
//     }
//   }
// }

// impl AsyncDatabase for TestBTreeDb {
//   type Error = std::convert::Infallible;

//   type Options = ();

//   type Key = u64;

//   type Value = u64;

//   // version and value
//   type Item = (u64, u64);
//   // version and value
//   type ItemRef<'a> = (u64, u64);

//   type Iterator<'a> = TestBTreeDbIter;

//   type Keys<'a> = TestBTreeDbKeys;

//   fn max_batch_size(&self) -> u64 {
//     self.max_batch_size
//   }

//   fn max_batch_entries(&self) -> u64 {
//     self.max_batch_entries
//   }

//   fn estimate_size(&self, _entry: &Entry<Self::Key, Self::Value>) -> u64 {
//     (mem::size_of::<TestKey>() + mem::size_of::<u64>()) as u64
//   }

//   fn validate_entry(&self, _entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error> {
//     Ok(())
//   }

//   fn maximum_version(&self) -> u64 {
//     0
//   }

//   fn options(&self) -> &Self::Options {
//     &()
//   }

//   async fn open(_opts: Self::Options) -> Result<Self, Self::Error> {
//     Ok(Self {
//       max_batch_size: 1024,
//       max_batch_entries: 1024,
//       store: futures::lock::Mutex::new(BTreeMap::new()),
//       hash: std::hash::RandomState::new(),
//     })
//   }

//   fn fingerprint(&self, k: &Self::Key) -> u64 {
//     self.hash.hash_one(k)
//   }

//   async fn apply(
//     &self,
//     entries: OneOrMore<Entry<Self::Key, Self::Value>>,
//   ) -> Result<(), Self::Error> {
//     let mut store = self.store.lock().await;
//     store.extend(entries.into_iter().map(|ent| match ent.data() {
//       EntryData::Insert { key, value, .. } => (
//         TestKey {
//           key: *key,
//           version: ent.version(),
//         },
//         Some(*value),
//       ),
//       EntryData::Remove(key) => (
//         TestKey {
//           key: *key,
//           version: ent.version(),
//         },
//         None,
//       ),
//     }));
//     Ok(())
//   }

//   async fn get(
//     &self,
//     k: &Self::Key,
//     version: u64,
//   ) -> Result<Option<Either<Self::ItemRef<'_>, Self::Item>>, Self::Error> {
//     let mut h = None;
//     let mut val = None;
//     self.store.lock().await.iter().for_each(|(key, v)| {
//       if key.key == *k && v.is_some() && key.version <= version && key.version > h.unwrap_or(0) {
//         h = Some(key.version);
//         val = v.map(|v| Either::Right((key.version, v)));
//       }
//     });
//     Ok(val)
//   }

//   async fn iter<'a, 'b: 'a>(
//     &'a self,
//     pending: impl Iterator<Item = EntryRef<'b, Self::Key, Self::Value>> + 'b,
//     transaction_version: u64,
//     opts: IteratorOptions,
//   ) -> Self::Iterator<'a> {
//     if opts.all_versions {
//       self
//         .iter_all_versions(
//           pending,
//           transaction_version,
//           opts.since_version,
//           opts.reverse,
//         )
//         .await
//     } else {
//       self
//         .iter(
//           pending,
//           transaction_version,
//           opts.since_version,
//           opts.reverse,
//         )
//         .await
//     }
//   }

//   async fn keys<'a, 'b: 'a>(
//     &'a self,
//     _pending: impl Iterator<Item = KeyRef<'b, Self::Key>> + 'b,
//     _transaction_version: u64,
//     _opts: KeysOptions,
//   ) -> Self::Keys<'a> {
//     // let pendings = pending.map(|k| *k.key).collect::<BTreeSet<_>>();
//     // let data = self
//     //   .store
//     //   .lock()
//     //   .iter()
//     //   .filter_map(|(k, v)| {
//     //     if pendings.contains(&k.key) {
//     //       None
//     //     } else {
//     //       v.as_ref().map(|_| k.key)
//     //     }
//     //   })
//     //   .collect::<BTreeSet<_>>();

//     // TestBTreeDbKeys { pendings, data }
//     todo!()
//   }
// }

// macro_rules! unittests {
//   (
//     $run:ident($spawner:ty)
//   ) => {
//     #[test]
//     fn test_txn_simple() {
//       $run(txn_simple::<
//         TestBTreeDb,
//         BTreeMap<u64, EntryValue<u64>>,
//         $spawner,
//       >(
//         (),
//         BTreeMap::default,
//         |i| i as u64,
//         |i| i as u64,
//         |item| match item {
//           Either::Left(l) => Either::Right(l.1),
//           Either::Right(r) => Either::Right(r.1),
//         },
//       ))
//     }

//     #[test]
//     fn test_txn_read_after_write() {
//       $run(txn_read_after_write::<TestBTreeDb, $spawner>(
//         (),
//         |i| i as u64,
//         |i| i as u64,
//         |item| match item {
//           Either::Left(l) => Either::Right(l.1),
//           Either::Right(r) => Either::Right(r.1),
//         },
//       ))
//     }

//     #[test]
//     fn test_txn_commit_with_callback() {
//       $run(txn_commit_with_callback::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         $spawner,
//       >((), BTreeMap::default, |i| i as u64, |i| i as u64))
//     }

//     #[test]
//     fn test_txn_versions() {
//       $run(txn_versions::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         _,
//         _,
//         $spawner,
//       >(
//         (),
//         BTreeMap::default,
//         |i| i as u64,
//         |i| i as u64,
//         |item| match item {
//           Either::Left(l) => Either::Right(l.1),
//           Either::Right(r) => Either::Right(r.1),
//         },
//         |itr| itr.into_iter().map(|(vs, k, v)| (vs, k, v.unwrap())),
//         |itr| itr.into_iter().map(|(vs, k, v)| (vs, k, v.unwrap())),
//       ))
//     }

//     #[test]
//     fn test_txn_write_skew() {
//       $run(txn_write_skew::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         $spawner,
//       >(
//         (),
//         BTreeMap::default,
//         |i| i as u64,
//         |i| i as u64,
//         |v: &u64| *v as usize,
//         |item| match item {
//           Either::Left(l) => Either::Right(l.1),
//           Either::Right(r) => Either::Right(r.1),
//         },
//       ))
//     }

//     #[test]
//     fn test_txn_iteration_edge_case() {
//       $run(txn_iteration_edge_case::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         _,
//         _,
//         $spawner,
//       >(
//         (),
//         BTreeMap::default,
//         |i| i as u64,
//         |i| i as u64,
//         |iter| {
//           iter
//             .into_iter()
//             .rev()
//             .filter_map(|(vs, k, v)| v.map(|v| (vs, k, v)))
//         },
//         |iter| {
//           iter
//             .into_iter()
//             .rev()
//             .filter_map(|(vs, k, v)| v.map(|v| (vs, k, v)))
//         },
//       ))
//     }

//     #[test]
//     fn test_txn_iteration_edge_case2() {
//       $run(txn_iteration_edge_case2::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         _,
//         _,
//         $spawner,
//       >(
//         (),
//         BTreeMap::default,
//         |i| i as u64,
//         |i| i as u64,
//         |iter| {
//           iter
//             .into_iter()
//             .rev()
//             .filter_map(|(vs, k, v)| v.map(|v| (vs, k, v)))
//         },
//         |iter| {
//           iter
//             .into_iter()
//             .rev()
//             .filter_map(|(vs, k, v)| v.map(|v| (vs, k, v)))
//         },
//       ))
//     }

//     #[test]
//     fn test_txn_all_versions_with_removed() {
//       $run(txn_all_versions_with_removed::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         _,
//         $spawner,
//       >(
//         (),
//         BTreeMap::default,
//         |i| i as u64,
//         |i| i as u64,
//         |v| match v {
//           Either::Left(l) => l.0,
//           Either::Right(r) => r.0,
//         },
//         |iter| iter.into_iter().rev(),
//       ))
//     }

//     #[test]
//     fn test_txn_all_versions_with_removed2() {
//       $run(txn_all_versions_with_removed2::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         _,
//         $spawner,
//       >(
//         (),
//         BTreeMap::default,
//         |i| i as u64,
//         |i| i as u64,
//         |iter| iter.into_iter().rev(),
//       ))
//     }

//     #[test]
//     fn test_txn_conflict_get() {
//       $run(txn_conflict_get::<
//         TestBTreeDb,
//         AsyncBTreeMapManager<u64, u64>,
//         $spawner,
//       >(|| (), BTreeMap::default, |i| i as u64, |i| i as u64))
//     }

//     // #[$rt::test]
//     // async fn test_txn_conflict_iter() {
//     //   txn_conflict_iter::<TestBTreeDb, AsyncBTreeMapManager<u64, u64>, _, $spawner>(
//     //     || (),
//     //     BTreeMap::default,
//     //     |i| i as u64,
//     //     |i| i as u64,
//     //     |itr| itr.into_iter().map(|(vs, k, v)| (vs, k, v.unwrap())),
//     //   ).await
//     // }
//   };
// }

// #[cfg(feature = "async-std")]
// mod _async_std {
//   use super::*;
//   use async_std::task::block_on;
//   use wmark::AsyncStdSpawner;

//   unittests!(block_on(AsyncStdSpawner));
// }

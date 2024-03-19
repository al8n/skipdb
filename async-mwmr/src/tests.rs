// use std::{
//   sync::atomic::{AtomicU32, Ordering},
//   time::Duration,
// };

// use either::Either;
// use futures::{select, FutureExt};
// use rand::{rngs::OsRng, Rng};
// use wmark::AsyncCloser;

// use crate::error::{Error, TransactionError};

// use super::*;

// /// Unit test for txn simple functionality
// pub async fn txn_simple<'a, D, W, S>(
//   opts: D::Options,
//   backend: impl Fn() -> W,
//   mut get_key: impl FnMut(usize) -> D::Key,
//   mut get_value: impl FnMut(usize) -> D::Value,
//   item_value: impl FnOnce(Either<D::ItemRef<'_>, D::Item>) -> Either<&'a D::Value, D::Value>,
// ) where
//   D: AsyncDatabase + Send + Sync + 'static,
//   D::Key: Send + Sync + 'static,
//   D::Value: PartialEq + Send + Sync + 'static,
//   W: AsyncPwm<Key = D::Key, Value = D::Value> + Send + Sync + 'static,
//   S: AsyncSpawner,
// {
//   let db = match Tm::<D, S>::new(Default::default(), opts).await {
//     Ok(db) => db,
//     Err(e) => panic!("{e}"),
//   };

//   {
//     let k = get_key(8);
//     let v = get_value(8);
//     let mut txn = db.write_by(backend()).await;
//     for i in 0..10 {
//       if let Err(e) = txn.insert(get_key(i), get_value(i)).await {
//         panic!("{e}");
//       }
//     }

//     let item = txn.get(&k).await.unwrap().unwrap();
//     match item {
//       Item::Pending(ent) => assert_eq!(ent.value().unwrap(), &v),
//       _ => panic!("unexpected item"),
//     }
//     drop(item);

//     txn.commit().await.unwrap();
//   }

//   let k = get_key(8);
//   let v = get_value(8);
//   let txn = db.read().await;
//   let item = txn.get(&k).await.unwrap().unwrap();
//   match item_value(item) {
//     Either::Left(l) => assert_eq!(l, &v),
//     Either::Right(r) => assert_eq!(r, v),
//   }
// }

// /// Unit test for txn read after write functionality
// pub async fn txn_read_after_write<'a, D, S>(
//   opts: D::Options,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   mut get_value: impl FnMut(usize) -> D::Value + Send + Sync + Copy + 'static,
//   item_value: impl FnOnce(Either<D::ItemRef<'_>, D::Item>) -> Either<&'a D::Value, D::Value>
//     + Send
//     + Sync
//     + Copy
//     + 'static,
// ) where
//   D: AsyncDatabase + Send + Sync + 'static,
//   D::Error: Send + Sync + 'static,
//   D::Key: core::hash::Hash + Eq,
//   D::Value: PartialEq,
//   S: AsyncSpawner,
// {
//   const N: usize = 100;

//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();

//   let handles = (0..N).map(|i| {
//     let db = db.clone();
//     S::spawn(async move {
//       let k = get_key(i);
//       let v = get_value(i);

//       let mut txn = db.write().await;
//       txn.insert(k, v).await.unwrap();
//       txn.commit().await.unwrap();

//       let txn = db.read().await;
//       let k = get_key(i);
//       let v = get_value(i);
//       let item = txn.get(&k).await.unwrap().unwrap();
//       match item_value(item) {
//         Either::Left(l) => assert_eq!(l, &v),
//         Either::Right(r) => assert_eq!(r, v),
//       }
//     })
//   });

//   for handle in handles {
//     handle.await;
//   }
// }

// /// Unit test for commit with callback functionality
// pub async fn txn_commit_with_callback<D, W, S>(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + 'static + Copy,
//   mut get_value: impl FnMut(usize) -> D::Value + Send + Sync + 'static + Copy,
// ) where
//   D: AsyncDatabase,
//   D::Value: PartialEq,
//   W: AsyncPwm<Key = D::Key, Value = D::Value> + Send + Sync + 'static,
//   S: AsyncSpawner,
// {
//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();
//   let mut txn = db.write_by(backend()).await;
//   for i in 0..40 {
//     let k = get_key(i);
//     let v = get_value(100);
//     txn.insert(k, v).await.unwrap();
//   }
//   txn.commit().await.unwrap();
//   txn.discard().await;

//   let closer = AsyncCloser::<S>::new(1);

//   let db1 = db.clone();
//   let closer1 = closer.clone();
//   S::spawn_detach(async move {
//     scopeguard::defer!(closer.done(););
//     let closer = closer.has_been_closed();
//     loop {
//       select! {
//         _ = closer.recv().fuse() => return,
//         default => {
//           // Keep checking balance variant
//           let txn = db1.read().await;
//           let mut total_balance = 0;

//           for i in 0..40 {
//             let k = get_key(i);
//             let _item = txn.get(&k).await.unwrap().unwrap();
//             total_balance += 100;
//           }
//           assert_eq!(total_balance, 4000);
//         }
//       }
//     }
//   });

//   let handles = (0..100).map(|_| {
//     let db1 = db.clone();
//     S::spawn(async move {
//       let mut txn = db1.write_by(backend()).await;
//       for i in 0..20 {
//         let mut rng = OsRng;
//         let r = rng.gen_range(0..100);
//         let k = get_key(i);
//         let v = get_value(100 - r);
//         txn.insert(k, v).await.unwrap();
//       }

//       for i in 20..40 {
//         let mut rng = OsRng;
//         let r = rng.gen_range(0..100);
//         let k = get_key(i);
//         let v = get_value(100 + r);
//         txn.insert(k, v).await.unwrap();
//       }

//       // We are only doing writes, so there won't be any conflicts.
//       let _ = txn.commit_with_task(|_| ()).await.unwrap().await;
//       txn.discard().await;
//     })
//   });

//   for handle in handles {
//     handle.await;
//   }

//   closer1.signal_and_wait().await;
//   std::thread::sleep(Duration::from_millis(10));
// }

// /// Unit test for txn versions functionality
// #[allow(clippy::too_many_arguments)]
// pub async fn txn_versions<
//   'b,
//   D: AsyncDatabase + Send + Sync,
//   W: AsyncPwm<Key = D::Key, Value = D::Value>,
//   I,
//   RI,
//   S,
// >(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
//   item_value: impl FnOnce(Either<D::ItemRef<'_>, D::Item>) -> Either<&'b D::Value, D::Value>
//     + Send
//     + Sync
//     + Copy
//     + 'static,
//   iter: impl Fn(D::Iterator<'_>) -> I,
//   rev_iter: impl Fn(D::Iterator<'_>) -> RI,
// ) where
//   D::Key: PartialEq,
//   D::Value: PartialEq,
//   I: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator + Send + Sync,
//   RI: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator + Send + Sync,
//   S: AsyncSpawner,
// {
//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();

//   for i in 1..10 {
//     let mut txn = db.write_by(backend()).await;
//     let k = get_key(0);
//     let v = get_value(i);
//     txn.insert(k, v).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(i as u64, db.inner.orc.read_ts().await);
//   }

//   let k0 = get_key(0);
//   let check_iter = |itr: I, i: usize| {
//     let mut count = 0;
//     for (_, k, v) in itr {
//       assert_eq!(k, k0);
//       assert_eq!(v, get_value(i), "{i} {:?}", v);
//       count += 1;
//     }
//     assert_eq!(1, count) // should only loop once.
//   };

//   let check_rev_iter = |itr: RI, i: usize| {
//     let mut count = 0;
//     for (_, k, v) in itr {
//       assert_eq!(k, k0);
//       assert_eq!(v, get_value(i));
//       count += 1;
//     }
//     assert_eq!(1, count) // should only loop once.
//   };

//   let check_all_versions = |itr: I, i: usize| {
//     let mut version = i as u64;

//     let mut count = 0;
//     for (vs, k, v) in itr {
//       assert_eq!(k, k0);
//       assert_eq!(vs, version);
//       assert_eq!(v, get_value(version as usize));

//       count += 1;
//       version -= 1;
//     }

//     assert_eq!(i, count); // Should loop as many times as i.
//   };

//   let check_rev_all_versions = |itr: RI, i: usize| {
//     let mut version = 1;

//     let mut count = 0;
//     for (vs, k, v) in itr {
//       assert_eq!(k, k0);
//       assert_eq!(vs, version);
//       assert_eq!(v, get_value(version as usize));

//       count += 1;
//       version += 1;
//     }

//     assert_eq!(i, count); // Should loop as many times as i.
//   };

//   for i in 1..10 {
//     let mut txn = db.write_by(backend()).await;
//     txn.read_ts = i as u64; // Read version at i.

//     let v = get_value(i);
//     let item = txn.get(&k0).await.unwrap().unwrap();
//     match item_value(item.unwrap_committed()) {
//       Either::Left(val) => assert_eq!(&v, val),
//       Either::Right(val) => assert_eq!(v, val),
//     }

//     // Try retrieving the latest version forward and reverse.
//     let itr = iter(txn.iter(Default::default()).await.unwrap());
//     check_iter(itr, i);

//     let itr = rev_iter(
//       txn
//         .iter(IteratorOptions::new().with_reverse(true))
//         .await
//         .unwrap(),
//     );
//     check_rev_iter(itr, i);

//     // Now try retrieving all versions forward and reverse.
//     let itr = iter(
//       txn
//         .iter(IteratorOptions::new().with_all_versions(true))
//         .await
//         .unwrap(),
//     );
//     check_all_versions(itr, i);

//     let itr = rev_iter(
//       txn
//         .iter(
//           IteratorOptions::new()
//             .with_all_versions(true)
//             .with_reverse(true),
//         )
//         .await
//         .unwrap(),
//     );
//     check_rev_all_versions(itr, i);
//   }

//   let mut txn = db.write_by(backend()).await;
//   let item = txn.get(&k0).await.unwrap().unwrap();
//   match item_value(item.unwrap_committed()) {
//     Either::Left(val) => assert_eq!(&get_value(9), val),
//     Either::Right(val) => assert_eq!(get_value(9), val),
//   }
// }

// /// Unit test for txn write skew functionality
// pub async fn txn_write_skew<'a, D: AsyncDatabase, W: AsyncPwm<Key = D::Key, Value = D::Value>, S>(
//   opts: D::Options,
//   backend: impl Fn() -> W,
//   get_key: impl Fn(usize) -> D::Key,
//   get_value: impl Fn(usize) -> D::Value,
//   value_to_usize: impl Fn(&D::Value) -> usize + Copy,
//   item_value: impl Fn(Either<D::ItemRef<'_>, D::Item>) -> Either<&'a D::Value, D::Value> + Copy,
// ) where
//   D::Value: PartialEq,
//   S: AsyncSpawner,
// {
//   // accounts
//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();

//   // Set balance to $100 in each account.
//   let mut txn = db.write_by(backend()).await;
//   txn.insert(a999, get_value(100)).await.unwrap();
//   txn.insert(a888, get_value(100)).await.unwrap();
//   txn.commit().await.unwrap();
//   assert_eq!(1, db.inner.orc.read_ts().await);

//   async fn get_bal<'a, D, W, S>(
//     txn: &mut Wtm<D, W, S>,
//     k: &D::Key,
//     value_to_usize: impl Fn(&D::Value) -> usize,
//     item_value: impl Fn(Either<D::ItemRef<'_>, D::Item>) -> Either<&'a D::Value, D::Value>,
//   ) -> usize
//   where
//     D: AsyncDatabase,
//     W: AsyncPwm<Key = D::Key, Value = D::Value>,
//     S: AsyncSpawner,
//   {
//     let item = txn.get(k).await.unwrap().unwrap();
//     match item {
//       Item::Pending(ent) => value_to_usize(ent.value().unwrap()),
//       Item::Owned(ent) => value_to_usize(item_value(Either::Right(ent)).as_ref().right().unwrap()),
//       Item::Borrowed(ent) => value_to_usize(item_value(Either::Left(ent)).left().unwrap()),
//     }
//   }

//   // Start two transactions, each would read both accounts and deduct from one account.
//   let mut txn1 = db.write_by(backend()).await;

//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn1, &a999, value_to_usize, item_value).await;
//   sum += get_bal(&mut txn1, &a888, value_to_usize, item_value).await;
//   assert_eq!(200, sum);
//   txn1.insert(a999, get_value(0)).await.unwrap(); // Deduct 100 from a999

//   // Let's read this back.
//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn1, &a999, value_to_usize, item_value).await;
//   assert_eq!(0, sum);
//   sum += get_bal(&mut txn1, &a888, value_to_usize, item_value).await;
//   assert_eq!(100, sum);
//   // Don't commit yet.

//   let mut txn2 = db.write_by(backend()).await;

//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn2, &a999, value_to_usize, item_value).await;
//   sum += get_bal(&mut txn2, &a888, value_to_usize, item_value).await;
//   assert_eq!(200, sum);
//   txn2.insert(a888, get_value(0)).await.unwrap(); // Deduct 100 from a888

//   // Let's read this back.
//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn2, &a999, value_to_usize, item_value).await;
//   assert_eq!(100, sum);
//   sum += get_bal(&mut txn2, &a888, value_to_usize, item_value).await;
//   assert_eq!(100, sum);

//   // Commit both now.
//   txn1.commit().await.unwrap();
//   txn2.commit().await.unwrap_err(); // This should fail

//   assert_eq!(2, db.orc().read_ts().await);
// }

// /// Unit test for txn conflict get functionality
// pub async fn txn_conflict_get<D, W, S>(
//   opts: impl Fn() -> D::Options,
//   backend: impl Fn() -> W + Send + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + 'static + Copy,
//   mut get_value: impl FnMut(usize) -> D::Value + Send + 'static + Copy,
// ) where
//   D: AsyncDatabase,
//   D::Value: PartialEq,
//   W: AsyncPwm<Key = D::Key, Value = D::Value>,
//   W::Error: Send,
//   S: AsyncSpawner,
// {
//   let set_count = Arc::new(AtomicU32::new(0));

//   for _ in 0..10 {
//     let db = Tm::<D, S>::new(Default::default(), opts()).await.unwrap();
//     set_count.store(0, Ordering::SeqCst);
//     let handles = (0..16).map(|_| {
//       let db1 = db.clone();
//       let set_count1 = set_count.clone();
//       S::spawn(async move {
//         let mut txn = db1.write_by(backend()).await;
//         if txn.get(&get_key(100)).await.unwrap().is_none() {
//           txn.insert(get_key(100), get_value(999)).await.unwrap();
//           if let Err(e) = txn
//             .commit_with_task(move |e| {
//               match e {
//                 Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
//                 Err(e) => panic!("{e}"),
//               };
//             })
//             .await
//           {
//             assert!(matches!(e, Error::Transaction(TransactionError::Conflict)));
//           }
//         }
//       })
//     });

//     for handle in handles {
//       handle.await;
//     }

//     assert_eq!(1, set_count.load(Ordering::SeqCst));
//   }
// }

// // /// Unit test for txn conflict iter functionality
// // pub async fn txn_conflict_iter<D, W, I, S>(
// //   opts: impl Fn() -> D::Options,
// //   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
// //   get_key: impl Fn(usize) -> D::Key + Send + Sync + Copy + 'static,
// //   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
// //   iter: impl Fn(D::Iterator<'_>) -> I + Send + Sync + Copy + 'static,
// // ) where
// //   D: AsyncDatabase,
// //   D::Key: PartialEq,
// //   D::Value: PartialEq,
// //   W: AsyncPwm<Key = D::Key, Value = D::Value>,
// //   I: Iterator<Item = (u64, D::Key, D::Value)> + Send + Sync + 'static,
// //   S: AsyncSpawner,
// // {
// //   let set_count = Arc::new(AtomicU32::new(0));

// //   for _ in 0..10 {
// //     let db = Tm::<D, S>::new(Default::default(), opts()).await.unwrap();
// //     set_count.store(0, Ordering::SeqCst);
// //     let wg = Arc::new(());
// //     (0..16).map(|_| {
// //       let db1 = db.clone();
// //       let set_count1 = set_count.clone();
// //       let wg = wg.clone();
// //       S::spawn(async {
// //         let mut txn = db1.write_by(backend()).await;

// //         let itr = txn.iter(Default::default()).await.unwrap();
// //         let mut found = false;
// //         for (_, k, _) in iter(itr) {
// //           if k == get_key(100) {
// //             found = true;
// //             break;
// //           }
// //         }

// //         if !found {
// //           txn.insert(get_key(100), get_value(999)).await.unwrap();
// //           if let Err(e) = txn.commit_with_task(move |e| {
// //             match e {
// //               Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
// //               Err(e) => panic!("{e}"),
// //             };
// //           }).await {
// //             assert!(matches!(e, Error::Transaction(TransactionError::Conflict)));
// //           }
// //         }
// //         drop(wg);
// //       })
// //     });

// //     while Arc::strong_count(&wg) > 1 {
// //       std::hint::spin_loop();
// //     }

// //     assert_eq!(1, set_count.load(Ordering::SeqCst));
// //   }
// // }

// /// Unit test for txn all versions with removed functionality
// pub async fn txn_all_versions_with_removed<
//   D: AsyncDatabase + Send + Sync,
//   W: AsyncPwm<Key = D::Key, Value = D::Value>,
//   I,
//   S,
// >(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
//   item_version: impl FnOnce(Either<D::ItemRef<'_>, D::Item>) -> u64 + Send + Copy + 'static,
//   iter: impl Fn(D::Iterator<'_>) -> I,
// ) where
//   D::Key: PartialEq,
//   D::Value: PartialEq,
//   I: Iterator<Item = (u64, D::Key, Option<D::Value>)> + DoubleEndedIterator,
//   S: AsyncSpawner,
// {
//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();
//   // write two keys
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.insert(get_key(1), get_value(42)).await.unwrap();
//     txn.insert(get_key(2), get_value(43)).await.unwrap();
//     txn.commit().await.unwrap();
//   }

//   // Delete the specific key version from underlying db directly
//   {
//     let txn = db.read().await;
//     let item = txn.get(&get_key(1)).await.unwrap().unwrap();
//     txn
//       .db
//       .database()
//       .apply(OneOrMore::from(Entry {
//         version: item_version(item),
//         data: EntryData::Remove(get_key(1)),
//       }))
//       .await
//       .unwrap();
//   }

//   // Verify that deleted shows up when AllVersions is set.
//   {
//     let txn = db.read().await;
//     let itr = iter(
//       txn
//         .iter(IteratorOptions::new().with_all_versions(true))
//         .await,
//     );

//     let mut count = 0;
//     for (_, k, v) in itr {
//       count += 1;
//       if count == 1 {
//         assert_eq!(k, get_key(1));
//         assert!(v.is_none());
//       } else {
//         assert_eq!(k, get_key(2));
//       }
//     }
//     assert_eq!(2, count);
//   }
// }

// /// Unit test for txn all versions with removed functionality 2
// pub async fn txn_all_versions_with_removed2<
//   D: AsyncDatabase + Send + Sync,
//   W: AsyncPwm<Key = D::Key, Value = D::Value>,
//   I,
//   S,
// >(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
//   iter: impl Fn(D::Iterator<'_>) -> I,
// ) where
//   D::Key: PartialEq,
//   D::Value: PartialEq,
//   I: Iterator<Item = (u64, D::Key, Option<D::Value>)> + DoubleEndedIterator,
//   S: AsyncSpawner,
// {
//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();
//   // Set and delete alternatively
//   {
//     for i in 0..4 {
//       let mut txn = db.write_by(backend()).await;
//       if i % 2 == 0 {
//         txn.insert(get_key(0), get_value(99)).await.unwrap();
//       } else {
//         txn.remove(get_key(0)).await.unwrap();
//       }
//       txn.commit().await.unwrap();
//     }
//   }

//   // Verify that deleted shows up when AllVersions is set.
//   {
//     let txn = db.read().await;
//     let itr = iter(
//       txn
//         .iter(IteratorOptions::new().with_all_versions(true))
//         .await,
//     );

//     let mut count = 0;
//     for (_, _, v) in itr {
//       if count % 2 == 0 {
//         assert_eq!(v, Some(get_value(99)));
//       } else {
//         assert!(v.is_none());
//       }
//       count += 1
//     }
//     assert_eq!(4, count);
//   }
// }

// /// Unit test for txn iteration edge case
// ///
// /// a3, a2, b4 (del), b3, c2, c1
// /// Read at ts=4 -> a3, c2
// /// Read at ts=4(Uncommitted) -> a3, b4
// /// Read at ts=3 -> a3, b3, c2
// /// Read at ts=2 -> a2, c2
// /// Read at ts=1 -> c1
// pub async fn txn_iteration_edge_case<
//   D: AsyncDatabase + Send + Sync,
//   W: AsyncPwm<Key = D::Key, Value = D::Value>,
//   I,
//   RI,
//   S,
// >(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
//   iter: impl Fn(D::Iterator<'_>) -> I,
//   rev_iter: impl Fn(D::Iterator<'_>) -> RI,
// ) where
//   D::Key: PartialEq,
//   D::Value: PartialEq,
//   I: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator,
//   RI: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator,
//   S: AsyncSpawner,
// {
//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();

//   // c1
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.insert(get_key(3), get_value(31)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(1, db.inner.orc.read_ts().await);
//   }

//   // a2, c2
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.insert(get_key(1), get_value(12)).await.unwrap();
//     txn.insert(get_key(3), get_value(32)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(2, db.inner.orc.read_ts().await);
//   }

//   // b3
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.insert(get_key(1), get_value(13)).await.unwrap();
//     txn.insert(get_key(2), get_value(23)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(3, db.inner.orc.read_ts().await);
//   }

//   // b4, c4(remove) (uncommitted)
//   let mut txn4 = db.write_by(backend()).await;
//   txn4.insert(get_key(2), get_value(24)).await.unwrap();
//   txn4.remove(get_key(3)).await.unwrap();
//   assert_eq!(3, db.inner.orc.read_ts().await);

//   // b4 (remove)
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.remove(get_key(2)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(4, db.inner.orc.read_ts().await);
//   }

//   let check_iter = |itr: I, expected: &[D::Value]| {
//     let mut i = 0;
//     for (vs, _, v) in itr {
//       assert_eq!(&expected[i], &v, "read_vs={}", vs);
//       i += 1;
//     }
//     assert_eq!(expected.len(), i);
//   };

//   let check_rev_iter = |itr: RI, expected: &[D::Value]| {
//     let mut i = 0;
//     for (vs, _, v) in itr {
//       assert_eq!(&expected[i], &v, "read_vs={}", vs);
//       i += 1;
//     }
//     assert_eq!(expected.len(), i);
//   };

//   let mut txn = db.write_by(backend()).await;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   let itr5 = txn4.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(32)]);
//   check_iter(iter(itr5), &[get_value(13), get_value(24)]);

//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   let itr5 = txn4
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(13)]);
//   check_rev_iter(rev_iter(itr5), &[get_value(24), get_value(13)]);

//   txn.read_ts = 3;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(23), get_value(32)]);
//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(
//     rev_iter(itr),
//     &[get_value(32), get_value(23), get_value(13)],
//   );

//   txn.read_ts = 2;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(12), get_value(32)]);
//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(12)]);

//   txn.read_ts = 1;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(31)]);
//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(31)]);
// }

// /// Unit test for txn iteration edge case 2
// ///
// /// a2, a3, b4 (del), b3, c2, c1
// /// Read at ts=4 -> a3, c2
// /// Read at ts=3 -> a3, b3, c2
// /// Read at ts=2 -> a2, c2
// /// Read at ts=1 -> c1
// pub async fn txn_iteration_edge_case2<
//   D: AsyncDatabase + Send + Sync,
//   W: AsyncPwm<Key = D::Key, Value = D::Value>,
//   I,
//   RI,
//   S,
// >(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
//   iter: impl Fn(D::Iterator<'_>) -> I,
//   rev_iter: impl Fn(D::Iterator<'_>) -> RI,
// ) where
//   D::Key: PartialEq,
//   D::Value: PartialEq,
//   I: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator,
//   RI: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator,
//   S: AsyncSpawner,
// {
//   let db = Tm::<D, S>::new(Default::default(), opts).await.unwrap();

//   // c1
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.insert(get_key(3), get_value(31)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(1, db.inner.orc.read_ts().await);
//   }

//   // a2, c2
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.insert(get_key(1), get_value(12)).await.unwrap();
//     txn.insert(get_key(3), get_value(32)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(2, db.inner.orc.read_ts().await);
//   }

//   // b3
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.insert(get_key(1), get_value(13)).await.unwrap();
//     txn.insert(get_key(2), get_value(23)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(3, db.inner.orc.read_ts().await);
//   }

//   // b4 (remove)
//   {
//     let mut txn = db.write_by(backend()).await;
//     txn.remove(get_key(2)).await.unwrap();
//     txn.commit().await.unwrap();
//     assert_eq!(4, db.inner.orc.read_ts().await);
//   }

//   let check_iter = |itr: I, expected: &[D::Value]| {
//     let mut i = 0;
//     for (_, _, v) in itr {
//       assert_eq!(&expected[i], &v);
//       i += 1;
//     }
//     assert_eq!(expected.len(), i);
//   };

//   let check_rev_iter = |itr: RI, expected: &[D::Value]| {
//     let mut i = 0;
//     for (_, _, v) in itr {
//       assert_eq!(&expected[i], &v);
//       i += 1;
//     }
//     assert_eq!(expected.len(), i);
//   };

//   let mut txn = db.write_by(backend()).await;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(32)]);
//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(13)]);

//   txn.read_ts = 5;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   let itr = iter(itr);
//   let mut count = 2;
//   for (_, k, _) in itr {
//     if k == get_key(1) {
//       count -= 1;
//     }

//     if k == get_key(3) {
//       count -= 1;
//     }
//   }
//   assert_eq!(0, count);

//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   let itr = iter(itr);
//   let mut count = 2;
//   for (_, k, _) in itr {
//     if k == get_key(1) {
//       count -= 1;
//     }

//     if k == get_key(3) {
//       count -= 1;
//     }
//   }
//   assert_eq!(0, count);

//   txn.read_ts = 3;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(23), get_value(32)]);

//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(
//     rev_iter(itr),
//     &[get_value(32), get_value(23), get_value(13)],
//   );

//   txn.read_ts = 2;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(12), get_value(32)]);

//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(12)]);

//   txn.read_ts = 1;
//   let itr = txn.iter(Default::default()).await.unwrap();
//   check_iter(iter(itr), &[get_value(31)]);
//   let itr = txn
//     .iter(IteratorOptions::new().with_reverse(true))
//     .await
//     .unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(31)]);
// }

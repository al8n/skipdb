// use std::{
//   sync::atomic::{AtomicU32, Ordering},
//   time::Duration,
// };

// use crossbeam_channel::select;
// use rand::Rng;
// use wmark::Closer;

// use super::*;

// /// Unit test for txn simple functionality
// pub fn txn_simple<'a, D: Database, W: Pwm<Key = D::Key, Value = D::Value>>(
//   opts: D::Options,
//   backend: impl Fn() -> W,
//   mut get_key: impl FnMut(usize) -> D::Key,
//   mut get_value: impl FnMut(usize) -> D::Value,
//   item_value: impl FnOnce(Either<D::ItemRef<'_>, D::Item>) -> Either<&'a D::Value, D::Value>,
// ) where
//   D::Value: PartialEq,
// {
//   let db = match Tm::<D>::new(Default::default(), opts) {
//     Ok(db) => db,
//     Err(e) => panic!("{e}"),
//   };

//   {
//     let k = get_key(8);
//     let v = get_value(8);
//     let mut txn = db.write_by(backend());
//     for i in 0..10 {
//       if let Err(e) = txn.insert(get_key(i), get_value(i)) {
//         panic!("{e}");
//       }
//     }

//     let item = txn.get(&k).unwrap().unwrap();
//     match item {
//       Item::Pending(ent) => assert_eq!(ent.value().unwrap(), &v),
//       _ => panic!("unexpected item"),
//     }
//     drop(item);

//     txn.commit().unwrap();
//   }

//   let k = get_key(8);
//   let v = get_value(8);
//   let txn = db.read();
//   let item = txn.get(&k).unwrap().unwrap();
//   match item_value(item) {
//     Either::Left(l) => assert_eq!(l, &v),
//     Either::Right(r) => assert_eq!(r, v),
//   }
// }

// /// Unit test for txn read after write functionality
// pub fn txn_read_after_write<'a, D: Database + Send + Sync>(
//   opts: D::Options,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   mut get_value: impl FnMut(usize) -> D::Value + Send + Sync + Copy + 'static,
//   item_value: impl FnOnce(Either<D::ItemRef<'_>, D::Item>) -> Either<&'a D::Value, D::Value>
//     + Send
//     + Copy
//     + 'static,
// ) where
//   D::Key: core::hash::Hash + Eq + Send + Sync,
//   D::Value: PartialEq,
// {
//   const N: usize = 100;

//   let db = Tm::<D>::new(Default::default(), opts).unwrap();

//   let handles = (0..N)
//     .map(|i| {
//       let db = db.clone();
//       std::thread::spawn(move || {
//         let k = get_key(i);
//         let v = get_value(i);

//         let mut txn = db.write();
//         txn.insert(k, v).unwrap();
//         txn.commit().unwrap();

//         let txn = db.read();
//         let k = get_key(i);
//         let v = get_value(i);
//         let item = txn.get(&k).unwrap().unwrap();
//         match item_value(item) {
//           Either::Left(l) => assert_eq!(l, &v),
//           Either::Right(r) => assert_eq!(r, v),
//         }
//       })
//     })
//     .collect::<Vec<_>>();

//   handles.into_iter().for_each(|h| {
//     h.join().unwrap();
//   });
// }

// /// Unit test for commit with callback functionality
// pub fn txn_commit_with_callback<
//   D: Database + Send + Sync,
//   W: Pwm<Key = D::Key, Value = D::Value> + Send,
// >(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + 'static + Copy,
//   mut get_value: impl FnMut(usize) -> D::Value + Send + Sync + 'static + Copy,
// ) where
//   D::Value: PartialEq + Send,
//   D::Key: Send,
//   D::Error: Send,
// {
//   use rand::thread_rng;

//   let db = Tm::<D>::new(Default::default(), opts).unwrap();
//   let mut txn = db.write_by(backend());
//   for i in 0..40 {
//     let k = get_key(i);
//     let v = get_value(100);
//     txn.insert(k, v).unwrap();
//   }
//   txn.commit().unwrap();
//   txn.discard();

//   let closer = Closer::new(1);

//   let db1 = db.clone();
//   let closer1 = closer.clone();
//   std::thread::spawn(move || {
//     scopeguard::defer!(closer.done(););

//     loop {
//       select! {
//         recv(closer.has_been_closed()) -> _ => return,
//         default => {
//           // Keep checking balance variant
//           let txn = db1.read();
//           let mut total_balance = 0;

//           for i in 0..40 {
//             let k = get_key(i);
//             let _item = txn.get(&k).unwrap().unwrap();
//             total_balance += 100;
//           }
//           assert_eq!(total_balance, 4000);
//         }
//       }
//     }
//   });

//   let handles = (0..100)
//     .map(|_| {
//       let db1 = db.clone();
//       std::thread::spawn(move || {
//         let mut txn = db1.write_by(backend());
//         for i in 0..20 {
//           let mut rng = thread_rng();
//           let r = rng.gen_range(0..100);
//           let k = get_key(i);
//           let v = get_value(100 - r);
//           txn.insert(k, v).unwrap();
//         }

//         for i in 20..40 {
//           let mut rng = thread_rng();
//           let r = rng.gen_range(0..100);
//           let k = get_key(i);
//           let v = get_value(100 + r);
//           txn.insert(k, v).unwrap();
//         }

//         // We are only doing writes, so there won't be any conflicts.
//         let _ = txn.commit_with_callback(|_| {}).unwrap();
//         txn.discard();
//       })
//     })
//     .collect::<Vec<_>>();

//   for h in handles {
//     h.join().unwrap();
//   }

//   closer1.signal_and_wait();
//   std::thread::sleep(Duration::from_millis(10));
// }

// /// Unit test for txn versions functionality
// pub fn txn_versions<'b, D: Database + Send + Sync, W: Pwm<Key = D::Key, Value = D::Value>, I, RI>(
//   opts: D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
//   item_value: impl FnOnce(Either<D::ItemRef<'_>, D::Item>) -> Either<&'b D::Value, D::Value>
//     + Send
//     + Copy
//     + 'static,
//   iter: impl Fn(D::Iterator<'_>) -> I,
//   rev_iter: impl Fn(D::Iterator<'_>) -> RI,
// ) where
//   D::Key: PartialEq,
//   D::Value: PartialEq,
//   I: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator,
//   RI: Iterator<Item = (u64, D::Key, D::Value)> + DoubleEndedIterator,
// {
//   let db = Tm::<D>::new(Default::default(), opts).unwrap();

//   for i in 1..10 {
//     let mut txn = db.write_by(backend());
//     let k = get_key(0);
//     let v = get_value(i);
//     txn.insert(k, v).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(i as u64, db.inner.orc.read_ts());
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
//     let mut txn = db.write_by(backend());
//     txn.read_ts = i as u64; // Read version at i.

//     let v = get_value(i);
//     let item = txn.get(&k0).unwrap().unwrap();
//     match item_value(item.unwrap_committed()) {
//       Either::Left(val) => assert_eq!(&v, val),
//       Either::Right(val) => assert_eq!(v, val),
//     }

//     // Try retrieving the latest version forward and reverse.
//     let itr = iter(txn.iter(Default::default()).unwrap());
//     check_iter(itr, i);

//     let itr = rev_iter(txn.iter(IteratorOptions::new().with_reverse(true)).unwrap());
//     check_rev_iter(itr, i);

//     // Now try retrieving all versions forward and reverse.
//     let itr = iter(
//       txn
//         .iter(IteratorOptions::new().with_all_versions(true))
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
//         .unwrap(),
//     );
//     check_rev_all_versions(itr, i);
//   }

//   let mut txn = db.write_by(backend());
//   let item = txn.get(&k0).unwrap().unwrap();
//   match item_value(item.unwrap_committed()) {
//     Either::Left(val) => assert_eq!(&get_value(9), val),
//     Either::Right(val) => assert_eq!(get_value(9), val),
//   }
// }

// /// Unit test for txn write skew functionality
// pub fn txn_write_skew<'a, D: Database, W: Pwm<Key = D::Key, Value = D::Value>>(
//   opts: D::Options,
//   backend: impl Fn() -> W,
//   mut get_key: impl FnMut(usize) -> D::Key,
//   mut get_value: impl FnMut(usize) -> D::Value,
//   mut value_to_usize: impl FnMut(&D::Value) -> usize,
//   mut item_value: impl FnMut(Either<D::ItemRef<'_>, D::Item>) -> Either<&'a D::Value, D::Value>,
// ) where
//   D::Value: PartialEq,
// {
//   // accounts
//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let db = Tm::<D>::new(Default::default(), opts).unwrap();

//   // Set balance to $100 in each account.
//   let mut txn = db.write_by(backend());
//   txn.insert(a999, get_value(100)).unwrap();
//   txn.insert(a888, get_value(100)).unwrap();
//   txn.commit().unwrap();
//   assert_eq!(1, db.inner.orc.read_ts());

//   let mut get_bal = |txn: &mut WriteTransaction<D, W>, k: &D::Key| -> usize {
//     let item = txn.get(k).unwrap().unwrap();
//     match item {
//       Item::Pending(ent) => value_to_usize(ent.value().unwrap()),
//       Item::Owned(ent) => value_to_usize(item_value(Either::Right(ent)).as_ref().right().unwrap()),
//       Item::Borrowed(ent) => value_to_usize(item_value(Either::Left(ent)).left().unwrap()),
//     }
//   };

//   // Start two transactions, each would read both accounts and deduct from one account.
//   let mut txn1 = db.write_by(backend());

//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn1, &a999);
//   sum += get_bal(&mut txn1, &a888);
//   assert_eq!(200, sum);
//   txn1.insert(a999, get_value(0)).unwrap(); // Deduct 100 from a999

//   // Let's read this back.
//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn1, &a999);
//   assert_eq!(0, sum);
//   sum += get_bal(&mut txn1, &a888);
//   assert_eq!(100, sum);
//   // Don't commit yet.

//   let mut txn2 = db.write_by(backend());

//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn2, &a999);
//   sum += get_bal(&mut txn2, &a888);
//   assert_eq!(200, sum);
//   txn2.insert(a888, get_value(0)).unwrap(); // Deduct 100 from a888

//   // Let's read this back.
//   let a999 = get_key(999);
//   let a888 = get_key(888);
//   let mut sum = get_bal(&mut txn2, &a999);
//   assert_eq!(100, sum);
//   sum += get_bal(&mut txn2, &a888);
//   assert_eq!(100, sum);

//   // Commit both now.
//   txn1.commit().unwrap();
//   txn2.commit().unwrap_err(); // This should fail

//   assert_eq!(2, db.orc().read_ts());
// }

// /// Unit test for txn conflict get functionality
// pub fn txn_conflict_get<D, W>(
//   opts: impl Fn() -> D::Options,
//   backend: impl Fn() -> W + Send + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + 'static + Copy,
//   mut get_value: impl FnMut(usize) -> D::Value + Send + 'static + Copy,
// ) where
//   D: Database + Send + Sync + 'static,
//   D::Key: Send,
//   D::Value: Send + PartialEq,
//   D::Error: Send + 'static,
//   W: Pwm<Key = D::Key, Value = D::Value> + Send + 'static,
//   W::Error: Send,
// {
//   let set_count = Arc::new(AtomicU32::new(0));

//   for _ in 0..10 {
//     let db = Tm::<D>::new(Default::default(), opts()).unwrap();
//     set_count.store(0, Ordering::SeqCst);
//     let handles = (0..16).map(|_| {
//       let db1 = db.clone();
//       let set_count1 = set_count.clone();
//       std::thread::spawn(move || {
//         let mut txn = db1.write_by(backend());
//         if txn.get(&get_key(100)).unwrap().is_none() {
//           txn.insert(get_key(100), get_value(999)).unwrap();
//           if let Err(e) = txn.commit_with_callback(move |e| {
//             match e {
//               Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
//               Err(e) => panic!("{e}"),
//             };
//           }) {
//             assert!(matches!(e, Error::Transaction(TransactionError::Conflict)));
//           }
//         }
//       })
//     });

//     for h in handles {
//       h.join().unwrap();
//     }

//     assert_eq!(1, set_count.load(Ordering::SeqCst));
//   }
// }

// /// Unit test for txn conflict iter functionality
// pub fn txn_conflict_iter<D, W, I>(
//   opts: impl Fn() -> D::Options,
//   backend: impl Fn() -> W + Send + Sync + Copy + 'static,
//   mut get_key: impl FnMut(usize) -> D::Key + Send + Sync + Copy + 'static,
//   get_value: impl Fn(usize) -> D::Value + Send + Sync + Copy + 'static,
//   iter: impl Fn(D::Iterator<'_>) -> I + Send + Copy + 'static,
// ) where
//   D: Database + Send + Sync + 'static,
//   D::Key: PartialEq + Send,
//   D::Value: Send + PartialEq,
//   D::Error: Send + 'static,
//   W: Pwm<Key = D::Key, Value = D::Value> + Send + 'static,
//   W::Error: Send,
//   I: Iterator<Item = (u64, D::Key, D::Value)> + Send + Sync + 'static,
// {
//   let set_count = Arc::new(AtomicU32::new(0));

//   for _ in 0..10 {
//     let db = Tm::<D>::new(Default::default(), opts()).unwrap();
//     set_count.store(0, Ordering::SeqCst);
//     let handles = (0..16).map(|_| {
//       let db1 = db.clone();
//       let set_count1 = set_count.clone();
//       std::thread::spawn(move || {
//         let mut txn = db1.write_by(backend());

//         let itr = txn.iter(Default::default()).unwrap();
//         let mut found = false;
//         for (_, k, _) in iter(itr) {
//           if k == get_key(100) {
//             found = true;
//             break;
//           }
//         }

//         if !found {
//           txn.insert(get_key(100), get_value(999)).unwrap();
//           if let Err(e) = txn.commit_with_callback(move |e| {
//             match e {
//               Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
//               Err(e) => panic!("{e}"),
//             };
//           }) {
//             assert!(matches!(e, Error::Transaction(TransactionError::Conflict)));
//           }
//         }
//       })
//     });

//     for h in handles {
//       h.join().unwrap();
//     }

//     assert_eq!(1, set_count.load(Ordering::SeqCst));
//   }
// }

// /// Unit test for txn all versions with removed functionality
// pub fn txn_all_versions_with_removed<
//   D: Database + Send + Sync,
//   W: Pwm<Key = D::Key, Value = D::Value>,
//   I,
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
// {
//   let db = Tm::<D>::new(Default::default(), opts).unwrap();
//   // write two keys
//   {
//     let mut txn = db.write_by(backend());
//     txn.insert(get_key(1), get_value(42)).unwrap();
//     txn.insert(get_key(2), get_value(43)).unwrap();
//     txn.commit().unwrap();
//   }

//   // Delete the specific key version from underlying db directly
//   {
//     let txn = db.read();
//     let k = get_key(1);
//     let item = txn.get(&k).unwrap().unwrap();
//     txn
//       .db
//       .database()
//       .apply(OneOrMore::from(Entry {
//         version: item_version(item),
//         data: EntryData::Remove(get_key(1)),
//       }))
//       .unwrap();
//   }

//   // Verify that deleted shows up when AllVersions is set.
//   {
//     let txn = db.read();
//     let itr = iter(txn.iter(IteratorOptions::new().with_all_versions(true)));

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
// pub fn txn_all_versions_with_removed2<
//   D: Database + Send + Sync,
//   W: Pwm<Key = D::Key, Value = D::Value>,
//   I,
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
// {
//   let db = Tm::<D>::new(Default::default(), opts).unwrap();
//   // Set and delete alternatively
//   {
//     for i in 0..4 {
//       let mut txn = db.write_by(backend());
//       if i % 2 == 0 {
//         txn.insert(get_key(0), get_value(99)).unwrap();
//       } else {
//         txn.remove(get_key(0)).unwrap();
//       }
//       txn.commit().unwrap();
//     }
//   }

//   // Verify that deleted shows up when AllVersions is set.
//   {
//     let txn = db.read();
//     let itr = iter(txn.iter(IteratorOptions::new().with_all_versions(true)));

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
// pub fn txn_iteration_edge_case<
//   D: Database + Send + Sync,
//   W: Pwm<Key = D::Key, Value = D::Value>,
//   I,
//   RI,
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
// {
//   let db = Tm::<D>::new(Default::default(), opts).unwrap();

//   // c1
//   {
//     let mut txn = db.write_by(backend());
//     txn.insert(get_key(3), get_value(31)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(1, db.inner.orc.read_ts());
//   }

//   // a2, c2
//   {
//     let mut txn = db.write_by(backend());
//     txn.insert(get_key(1), get_value(12)).unwrap();
//     txn.insert(get_key(3), get_value(32)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(2, db.inner.orc.read_ts());
//   }

//   // b3
//   {
//     let mut txn = db.write_by(backend());
//     txn.insert(get_key(1), get_value(13)).unwrap();
//     txn.insert(get_key(2), get_value(23)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(3, db.inner.orc.read_ts());
//   }

//   // b4, c4(remove) (uncommitted)
//   let mut txn4 = db.write_by(backend());
//   txn4.insert(get_key(2), get_value(24)).unwrap();
//   txn4.remove(get_key(3)).unwrap();
//   assert_eq!(3, db.inner.orc.read_ts());

//   // b4 (remove)
//   {
//     let mut txn = db.write_by(backend());
//     txn.remove(get_key(2)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(4, db.inner.orc.read_ts());
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

//   let mut txn = db.write_by(backend());
//   let itr = txn.iter(Default::default()).unwrap();
//   let itr5 = txn4.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(32)]);
//   check_iter(iter(itr5), &[get_value(13), get_value(24)]);

//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   let itr5 = txn4
//     .iter(IteratorOptions::new().with_reverse(true))
//     .unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(13)]);
//   check_rev_iter(rev_iter(itr5), &[get_value(24), get_value(13)]);

//   txn.read_ts = 3;
//   let itr = txn.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(23), get_value(32)]);
//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   check_rev_iter(
//     rev_iter(itr),
//     &[get_value(32), get_value(23), get_value(13)],
//   );

//   txn.read_ts = 2;
//   let itr = txn.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(12), get_value(32)]);
//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(12)]);

//   txn.read_ts = 1;
//   let itr = txn.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(31)]);
//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(31)]);
// }

// /// Unit test for txn iteration edge case 2
// ///
// /// a2, a3, b4 (del), b3, c2, c1
// /// Read at ts=4 -> a3, c2
// /// Read at ts=3 -> a3, b3, c2
// /// Read at ts=2 -> a2, c2
// /// Read at ts=1 -> c1
// pub fn txn_iteration_edge_case2<
//   D: Database + Send + Sync,
//   W: Pwm<Key = D::Key, Value = D::Value>,
//   I,
//   RI,
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
// {
//   let db = Tm::<D>::new(Default::default(), opts).unwrap();

//   // c1
//   {
//     let mut txn = db.write_by(backend());
//     txn.insert(get_key(3), get_value(31)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(1, db.inner.orc.read_ts());
//   }

//   // a2, c2
//   {
//     let mut txn = db.write_by(backend());
//     txn.insert(get_key(1), get_value(12)).unwrap();
//     txn.insert(get_key(3), get_value(32)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(2, db.inner.orc.read_ts());
//   }

//   // b3
//   {
//     let mut txn = db.write_by(backend());
//     txn.insert(get_key(1), get_value(13)).unwrap();
//     txn.insert(get_key(2), get_value(23)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(3, db.inner.orc.read_ts());
//   }

//   // b4 (remove)
//   {
//     let mut txn = db.write_by(backend());
//     txn.remove(get_key(2)).unwrap();
//     txn.commit().unwrap();
//     assert_eq!(4, db.inner.orc.read_ts());
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

//   let mut txn = db.write_by(backend());
//   let itr = txn.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(32)]);
//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(13)]);

//   txn.read_ts = 5;
//   let itr = txn.iter(Default::default()).unwrap();
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

//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
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
//   let itr = txn.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(13), get_value(23), get_value(32)]);

//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   check_rev_iter(
//     rev_iter(itr),
//     &[get_value(32), get_value(23), get_value(13)],
//   );

//   txn.read_ts = 2;
//   let itr = txn.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(12), get_value(32)]);

//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(32), get_value(12)]);

//   txn.read_ts = 1;
//   let itr = txn.iter(Default::default()).unwrap();
//   check_iter(iter(itr), &[get_value(31)]);
//   let itr = txn.iter(IteratorOptions::new().with_reverse(true)).unwrap();
//   check_rev_iter(rev_iter(itr), &[get_value(31)]);
// }

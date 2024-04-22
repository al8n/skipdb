#![allow(clippy::blocks_in_conditions)]

use std::{
  sync::atomic::{AtomicU32, Ordering},
  // time::Duration,
};

use async_mwmr::error::WtmError;
use futures::{
  stream::FuturesUnordered,
  // Future, FutureExt,
  StreamExt,
};
// use rand::{rngs::OsRng, Rng};
// use wmark::AsyncCloser;

use super::*;

async fn begin_tx_readable_in<S: AsyncSpawner>() {
  let db: ComparableDB<&'static str, Vec<u8>, S> = ComparableDB::new().await;
  let tx = db.read().await;
  assert_eq!(tx.version(), 0);
}

#[tokio::test]
async fn begin_tx_readable_tokio() {
  begin_tx_readable_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn begin_tx_readable_async_std() {
  begin_tx_readable_in::<AsyncStdSpawner>().await;
}

#[test]
fn begin_tx_readable_smol() {
  smol::block_on(begin_tx_readable_in::<SmolSpawner>());
}

async fn begin_tx_writeable_in<S: AsyncSpawner>() {
  let db: ComparableDB<&'static str, Vec<u8>, S> = ComparableDB::new().await;
  let tx = db.write().await;
  assert_eq!(tx.version(), 0);
}

#[tokio::test]
async fn begin_tx_writeable_tokio() {
  begin_tx_writeable_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn begin_tx_writeable_async_std() {
  begin_tx_writeable_in::<AsyncStdSpawner>().await;
}

#[test]
fn begin_tx_writeable_smol() {
  smol::block_on(begin_tx_writeable_in::<SmolSpawner>());
}

async fn writeable_tx_in<S: AsyncSpawner>() {
  let db: ComparableDB<&'static str, &'static str, S> = ComparableDB::new().await;
  {
    let mut tx = db.write().await;
    assert_eq!(tx.version(), 0);

    tx.insert("foo", "foo1").unwrap();
    assert_eq!(*tx.get(&"foo").unwrap().unwrap().value(), "foo1");
    tx.commit().await.unwrap();
  }

  {
    let tx = db.read().await;
    assert_eq!(tx.version(), 1);
    assert_eq!(*tx.get("foo").unwrap().value(), "foo1");
  }
}

#[tokio::test]
async fn writeable_tx_tokio() {
  writeable_tx_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn writeable_tx_async_std() {
  writeable_tx_in::<AsyncStdSpawner>().await;
}

#[test]
fn writeable_tx_smol() {
  smol::block_on(writeable_tx_in::<SmolSpawner>());
}

async fn txn_simple_in<S: AsyncSpawner>() {
  let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;

  {
    let mut txn = db.write().await;
    for i in 0..10 {
      if let Err(e) = txn.insert(i, i) {
        panic!("{e}");
      }
    }

    let item = txn.get(&8).unwrap().unwrap();
    assert!(!item.is_committed());
    assert_eq!(*item.value(), 8);
    drop(item);

    txn.commit().await.unwrap();
  }

  let k = 8;
  let v = 8;
  let txn = db.read().await;
  let item = txn.get(&k).unwrap();
  assert_eq!(*item.value(), v);
  drop(item);
}

#[tokio::test]
async fn txn_simple_tokio() {
  txn_simple_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_simple_async_std() {
  txn_simple_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_simple_smol() {
  smol::block_on(txn_simple_in::<SmolSpawner>());
}

async fn txn_read_after_write_in<S: AsyncSpawner>() {
  const N: u64 = 100;

  let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;

  let mut handles = (0..N)
    .map(|i| {
      let db = db.clone();
      S::spawn(async move {
        let k = i;
        let v = i;

        let mut txn = db.write().await;
        txn.insert(k, v).unwrap();
        txn.commit().await.unwrap();

        let txn = db.read().await;
        let k = i;
        let v = i;
        let item = txn.get(&k).unwrap();
        assert_eq!(*item.value(), v);
        drop(item);
      })
    })
    .collect::<FuturesUnordered<_>>();

  while handles.next().await.is_some() {}
}

#[tokio::test]
async fn txn_read_after_write_tokio() {
  txn_read_after_write_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_read_after_write_async_std() {
  txn_read_after_write_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_read_after_write_smol() {
  smol::block_on(txn_read_after_write_in::<SmolSpawner>());
}

// async fn txn_commit_with_callback_in<S: AsyncSpawner, R, Y>(sleep: impl Future<Output = R>, yielder: impl Fn() -> Y + Send + Copy + 'static)
// where
//   Y: Future<Output = ()> + Send,
// {
//   let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;
//   let mut txn = db.write().await;
//   for i in 0..40 {
//     txn.insert(i, 100).unwrap();
//   }
//   txn.commit().await.unwrap();

//   let closer = AsyncCloser::<S>::new(1);

//   let db1 = db.clone();
//   let closer1 = closer.clone();
//   S::spawn(async move {
//     scopeguard::defer!(closer.done(););
//     let rx = closer.has_been_closed();
//     loop {
//       futures::select! {
//         _ = rx.recv().fuse() => {
//           return
//         },
//         default => {
//           // Keep checking balance variant
//           let txn = db1.read().await;
//           let mut total_balance = 0;

//           for i in 0..40 {
//             let _item = txn.get(&i).unwrap();
//             total_balance += 100;
//           }
//           assert_eq!(total_balance, 4000);

//         }
//       }
//     }
//   })
//   .detach();

//   let mut handles = (0..100)
//     .map(|_| {
//       let db1 = db.clone();
//       S::spawn(async move {
//         yielder().await;

//         let mut txn = db1.write().await;
//         for i in 0..20 {
//           let mut rng = OsRng;
//           let r = rng.gen_range(0..100);
//           let v = 100 - r;
//           txn.insert(i, v).unwrap();
//         }

//         for i in 20..40 {
//           let mut rng = OsRng;
//           let r = rng.gen_range(0..100);
//           let v = 100 + r;
//           txn.insert(i, v).unwrap();
//         }

//         // We are only doing writes, so there won't be any conflicts.
//         txn
//           .commit_with_task::<std::convert::Infallible, ()>(|_| {})
//           .await
//           .unwrap()
//           .await;
//       })
//     })
//     .collect::<FuturesUnordered<_>>();

//   while handles.next().await.is_some() {}

//   closer1.signal_and_wait().await;
//   sleep.await;
// }

// #[tokio::test]
// async fn txn_commit_with_callback_tokio() {
//   txn_commit_with_callback_in::<TokioSpawner, _, _>(tokio::time::sleep(Duration::from_millis(10)), tokio::task::yield_now)
//     .await;
// }

// #[async_std::test]
// async fn txn_commit_with_callback_async_std() {
//   txn_commit_with_callback_in::<AsyncStdSpawner, _, _>(async_std::task::sleep(Duration::from_millis(
//     10,
//   )), async_std::task::yield_now)
//   .await;
// }

// #[test]
// fn txn_commit_with_callback_smol() {
//   smol::block_on(txn_commit_with_callback_in::<SmolSpawner, _, _>(
//     smol::Timer::after(Duration::from_millis(10)), smol::future::yield_now),
//   );
// }

async fn txn_write_skew_in<S: AsyncSpawner>() {
  // accounts
  let a999 = 999;
  let a888 = 888;
  let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;

  // Set balance to $100 in each account.
  let mut txn = db.write().await;
  txn.insert(a999, 100).unwrap();
  txn.insert(a888, 100).unwrap();
  txn.commit().await.unwrap();
  assert_eq!(1, db.version().await);

  async fn get_bal<'a, S: AsyncSpawner>(
    txn: &'a mut WriteTransaction<u64, u64, S>,
    k: &'a u64,
  ) -> u64 {
    let item = txn.get(k).unwrap().unwrap();
    let val = *item.value();
    val
  }

  // Start two transactions, each would read both accounts and deduct from one account.
  let mut txn1 = db.write().await;

  let mut sum = get_bal(&mut txn1, &a999).await;
  sum += get_bal(&mut txn1, &a888).await;
  assert_eq!(200, sum);
  txn1.insert(a999, 0).unwrap(); // Deduct 100 from a999

  // Let's read this back.
  let mut sum = get_bal(&mut txn1, &a999).await;
  assert_eq!(0, sum);
  sum += get_bal(&mut txn1, &a888).await;
  assert_eq!(100, sum);
  // Don't commit yet.

  let mut txn2 = db.write().await;

  let mut sum = get_bal(&mut txn2, &a999).await;
  sum += get_bal(&mut txn2, &a888).await;
  assert_eq!(200, sum);
  txn2.insert(a888, 0).unwrap(); // Deduct 100 from a888

  // Let's read this back.
  let mut sum = get_bal(&mut txn2, &a999).await;
  assert_eq!(100, sum);
  sum += get_bal(&mut txn2, &a888).await;
  assert_eq!(100, sum);

  // Commit both now.
  txn1.commit().await.unwrap();
  txn2.commit().await.unwrap_err(); // This should fail

  assert_eq!(2, db.version().await);
}

#[tokio::test]
async fn txn_write_skew_tokio() {
  txn_write_skew_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_write_skew_async_std() {
  txn_write_skew_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_write_skew_smol() {
  smol::block_on(txn_write_skew_in::<SmolSpawner>());
}

async fn txn_conflict_get_in<S: AsyncSpawner>() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;
    set_count.store(0, Ordering::SeqCst);
    let handles = (0..16).map(|_| {
      let db1 = db.clone();
      let set_count1 = set_count.clone();
      S::spawn(async move {
        let mut txn = db1.write().await;
        if txn.get(&100).unwrap().is_none() {
          txn.insert(100, 999).unwrap();
          #[allow(clippy::blocks_in_conditions)]
          match txn
            .commit_with_task::<_, std::convert::Infallible, _>(|e| async move {
              match e {
                Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
                Err(e) => panic!("{e}"),
              };
            })
            .await
          {
            Ok(handle) => {
              let _ = handle.await;
            }
            Err(e) => assert!(matches!(
              e,
              WtmError::Transaction(TransactionError::Conflict)
            )),
          }
        }
      })
    });

    for h in handles {
      let _ = h.await;
    }

    assert_eq!(1, set_count.load(Ordering::SeqCst));
  }
}

#[tokio::test]
async fn txn_conflict_get_tokio() {
  txn_conflict_get_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_conflict_get_async_std() {
  txn_conflict_get_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_conflict_get_smol() {
  smol::block_on(txn_conflict_get_in::<SmolSpawner>());
}

async fn txn_versions_in<S: AsyncSpawner>() {
  let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;

  let k0 = 0;
  for i in 1..10 {
    let mut txn = db.write().await;
    txn.insert(k0, i).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(i, db.version().await);
  }

  let check_iter = |itr: WriteTransactionIter<'_, u64, u64, BTreeCm<u64>>, i: u64| {
    let mut count = 0;
    for ent in itr {
      assert_eq!(ent.key(), &k0);
      assert_eq!(ent.value(), i, "{i} {:?}", ent.value());
      count += 1;
    }
    assert_eq!(1, count) // should only loop once.
  };

  let check_rev_iter = |itr: WriteTransactionRevIter<'_, u64, u64, BTreeCm<u64>>, i: u64| {
    let mut count = 0;
    for ent in itr {
      assert_eq!(ent.key(), &k0);
      assert_eq!(ent.value(), i, "{i} {:?}", ent.value());
      count += 1;
    }
    assert_eq!(1, count) // should only loop once.
  };

  for i in 1..10 {
    let mut txn = db.write().await;
    txn.wtm.__set_read_version(i); // Read version at i.

    let v = i;
    {
      let item = txn.get(&k0).unwrap().unwrap();
      assert_eq!(v, *item.value());
    }

    // Try retrieving the latest version forward and reverse.
    let itr = txn.iter().unwrap();
    check_iter(itr, i);

    let itr = txn.iter_rev().unwrap();
    check_rev_iter(itr, i);
  }

  let mut txn = db.write().await;
  let item = txn.get(&k0).unwrap().unwrap();
  let val = *item.value();
  assert_eq!(9, val)
}

#[tokio::test]
async fn txn_versions_tokio() {
  txn_versions_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_versions_async_std() {
  txn_versions_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_versions_smol() {
  smol::block_on(txn_versions_in::<SmolSpawner>());
}

async fn txn_conflict_iter_in<S: AsyncSpawner>() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;
    set_count.store(0, Ordering::SeqCst);
    let handles = (0..16).map(|_| {
      let db1 = db.clone();
      let set_count1 = set_count.clone();
      S::spawn(async move {
        let mut txn = db1.write().await;

        let itr = txn.iter().unwrap();
        let mut found = false;
        for ent in itr {
          if *ent.key() == 100 {
            found = true;
            break;
          }
        }

        if !found {
          txn.insert(100, 999).unwrap();

          match txn
            .commit_with_task::<_, std::convert::Infallible, ()>(|e| async move {
              match e {
                Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
                Err(e) => panic!("{e}"),
              };
            })
            .await
          {
            Ok(handle) => {
              let _ = handle.await;
            }
            Err(e) => assert!(matches!(
              e,
              WtmError::Transaction(TransactionError::Conflict)
            )),
          }
        }
      })
    });

    for h in handles {
      let _ = h.await;
    }

    assert_eq!(1, set_count.load(Ordering::SeqCst));
  }
}

#[tokio::test]
async fn txn_conflict_iter_tokio() {
  txn_conflict_iter_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_conflict_iter_async_std() {
  txn_conflict_iter_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_conflict_iter_smol() {
  smol::block_on(txn_conflict_iter_in::<SmolSpawner>());
}

/// a3, a2, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=4(Uncommitted) -> a3, b4
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
async fn txn_iteration_edge_case_in<S: AsyncSpawner>() {
  let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;

  // c1
  {
    let mut txn = db.write().await;
    txn.insert(3, 31).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(1, db.version().await);
  }

  // a2, c2
  {
    let mut txn = db.write().await;
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(2, db.version().await);
  }

  // b3
  {
    let mut txn = db.write().await;
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(3, db.version().await);
  }

  // b4, c4(remove) (uncommitted)
  let mut txn4 = db.write().await;
  txn4.insert(2, 24).unwrap();
  txn4.remove(3).unwrap();
  assert_eq!(3, db.version().await);

  // b4 (remove)
  {
    let mut txn = db.write().await;
    txn.remove(2).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(4, db.version().await);
  }

  let check_iter = |itr: WriteTransactionIter<'_, u64, u64, BTreeCm<u64>>, expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value(), "read_vs={}", ent.version());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let check_rev_iter = |itr: WriteTransactionRevIter<'_, u64, u64, BTreeCm<u64>>,
                        expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value(), "read_vs={}", ent.version());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let mut txn = db.write().await;
  let itr = txn.iter().unwrap();
  let itr5 = txn4.iter().unwrap();
  check_iter(itr, &[13, 32]);
  check_iter(itr5, &[13, 24]);

  let itr = txn.iter_rev().unwrap();
  let itr5 = txn4.iter_rev().unwrap();
  check_rev_iter(itr, &[32, 13]);
  check_rev_iter(itr5, &[24, 13]);

  txn.wtm.__set_read_version(3);
  let itr = txn.iter().unwrap();
  check_iter(itr, &[13, 23, 32]);
  let itr = txn.iter_rev().unwrap();
  check_rev_iter(itr, &[32, 23, 13]);

  txn.wtm.__set_read_version(2);
  let itr = txn.iter().unwrap();
  check_iter(itr, &[12, 32]);
  let itr = txn.iter_rev().unwrap();
  check_rev_iter(itr, &[32, 12]);

  txn.wtm.__set_read_version(1);
  let itr = txn.iter().unwrap();
  check_iter(itr, &[31]);
  let itr = txn.iter_rev().unwrap();
  check_rev_iter(itr, &[31]);
}

#[tokio::test]
async fn txn_iteration_edge_case_tokio() {
  txn_iteration_edge_case_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_iteration_edge_case_async_std() {
  txn_iteration_edge_case_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_iteration_edge_case_smol() {
  smol::block_on(txn_iteration_edge_case_in::<SmolSpawner>());
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
async fn txn_iteration_edge_case2_in<S: AsyncSpawner>() {
  let db: ComparableDB<u64, u64, S> = ComparableDB::new().await;

  // c1
  {
    let mut txn = db.write().await;
    txn.insert(3, 31).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(1, db.version().await);
  }

  // a2, c2
  {
    let mut txn = db.write().await;
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(2, db.version().await);
  }

  // b3
  {
    let mut txn = db.write().await;
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(3, db.version().await);
  }

  // b4 (remove)
  {
    let mut txn = db.write().await;
    txn.remove(2).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(4, db.version().await);
  }

  let check_iter = |itr: WriteTransactionIter<'_, u64, u64, BTreeCm<u64>>, expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let check_rev_iter = |itr: WriteTransactionRevIter<'_, u64, u64, BTreeCm<u64>>,
                        expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let mut txn = db.write().await;
  let itr = txn.iter().unwrap();
  check_iter(itr, &[13, 32]);
  let itr = txn.iter_rev().unwrap();
  check_rev_iter(itr, &[32, 13]);

  txn.wtm.__set_read_version(5);
  let itr = txn.iter().unwrap();
  let mut count = 2;
  for ent in itr {
    if *ent.key() == 1 {
      count -= 1;
    }

    if *ent.key() == 3 {
      count -= 1;
    }
  }
  assert_eq!(0, count);

  let itr = txn.iter().unwrap();
  let mut count = 2;
  for ent in itr {
    if *ent.key() == 1 {
      count -= 1;
    }

    if *ent.key() == 3 {
      count -= 1;
    }
  }
  assert_eq!(0, count);

  txn.wtm.__set_read_version(3);
  let itr = txn.iter().unwrap();
  check_iter(itr, &[13, 23, 32]);

  let itr = txn.iter_rev().unwrap();
  check_rev_iter(itr, &[32, 23, 13]);

  txn.wtm.__set_read_version(2);
  let itr = txn.iter().unwrap();
  check_iter(itr, &[12, 32]);

  let itr = txn.iter_rev().unwrap();
  check_rev_iter(itr, &[32, 12]);

  txn.wtm.__set_read_version(1);
  let itr = txn.iter().unwrap();
  check_iter(itr, &[31]);
  let itr = txn.iter_rev().unwrap();
  check_rev_iter(itr, &[31]);
}

#[tokio::test]
async fn txn_iteration_edge_case2_tokio() {
  txn_iteration_edge_case2_in::<TokioSpawner>().await;
}

#[async_std::test]
async fn txn_iteration_edge_case2_async_std() {
  txn_iteration_edge_case2_in::<AsyncStdSpawner>().await;
}

#[test]
fn txn_iteration_edge_case2_smol() {
  smol::block_on(txn_iteration_edge_case2_in::<SmolSpawner>());
}

#![allow(clippy::blocks_in_conditions)]

use std::{
  future::Future,
  sync::atomic::{AtomicU32, Ordering},
  time::Duration,
};

use async_txn::error::WtmError;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use rand::{rngs::OsRng, Rng};
use skipdb_core::rev_range::WriteTransactionRevRange;
use wmark::AsyncCloser;

use super::*;

async fn begin_tx_readable_in<S: AsyncSpawner>() {
  let db: OptimisticDb<&'static str, Vec<u8>, S> = OptimisticDb::new().await;
  let tx = db.read().await;
  assert_eq!(tx.version(), 0);
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn begin_tx_readable_tokio() {
  begin_tx_readable_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn begin_tx_readable_async_std() {
  begin_tx_readable_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn begin_tx_readable_smol() {
  smol::block_on(begin_tx_readable_in::<SmolSpawner>());
}

async fn begin_tx_writeable_in<S: AsyncSpawner>() {
  let db: OptimisticDb<&'static str, Vec<u8>, S> = OptimisticDb::new().await;
  let tx = db.write().await;
  assert_eq!(tx.version(), 0);
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn begin_tx_writeable_tokio() {
  begin_tx_writeable_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn begin_tx_writeable_async_std() {
  begin_tx_writeable_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn begin_tx_writeable_smol() {
  smol::block_on(begin_tx_writeable_in::<SmolSpawner>());
}

async fn writeable_tx_in<S: AsyncSpawner>() {
  let db: OptimisticDb<&'static str, &'static str, S> = OptimisticDb::new().await;
  {
    let mut tx = db.write().await;
    assert_eq!(tx.version(), 0);

    tx.insert("foo", "foo1").unwrap();
    assert_eq!(*tx.get("foo").unwrap().unwrap().value(), "foo1");
    assert!(tx.contains_key("foo").unwrap());
    tx.commit().await.unwrap();
  }

  {
    let tx = db.read().await;
    assert_eq!(tx.version(), 1);
    assert_eq!(*tx.get("foo").unwrap().value(), "foo1");
    assert!(tx.contains_key("foo"));
  }
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn writeable_tx_tokio() {
  writeable_tx_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn writeable_tx_async_std() {
  writeable_tx_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn writeable_tx_smol() {
  smol::block_on(writeable_tx_in::<SmolSpawner>());
}

async fn txn_simple_in<S: AsyncSpawner>() {
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;

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
#[cfg(feature = "tokio")]
async fn txn_simple_tokio() {
  txn_simple_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_simple_async_std() {
  txn_simple_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_simple_smol() {
  smol::block_on(txn_simple_in::<SmolSpawner>());
}

async fn txn_read_after_write_in<S: AsyncSpawner>() {
  const N: u64 = 100;

  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;

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
#[cfg(feature = "tokio")]
async fn txn_read_after_write_tokio() {
  txn_read_after_write_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_read_after_write_async_std() {
  txn_read_after_write_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_read_after_write_smol() {
  smol::block_on(txn_read_after_write_in::<SmolSpawner>());
}

async fn txn_commit_with_callback_in<S: AsyncSpawner, Y>(
  yielder: impl Fn() -> Y + Send + Sync + 'static,
) where
  Y: Future<Output = ()> + Send + Sync + 'static,
{
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;
  let mut txn = db.write().await;
  for i in 0..40 {
    txn.insert(i, 100).unwrap();
  }
  txn.commit().await.unwrap();

  let closer = AsyncCloser::<S>::new(1);

  let db1 = db.clone();
  let closer1 = closer.clone();
  S::spawn(async move {
    scopeguard::defer!(closer.done(););
    let rx = closer.listen();
    loop {
      futures::select! {
        _ = rx.wait().fuse() => return,
        default => {
          // Keep checking balance variant
          let txn = db1.read().await;
          let mut total_balance = 0;

          for i in 0..40 {
            let _item = txn.get(&i).unwrap();
            total_balance += 100;
          }
          assert_eq!(total_balance, 4000);
          yielder().await;
        }
      }
    }
  })
  .detach();

  let mut handles = (0..100)
    .map(|_| {
      let db1 = db.clone();
      S::spawn(async move {
        let mut txn = db1.write().await;
        for i in 0..20 {
          let mut rng = OsRng;
          let r = rng.gen_range(0..100);
          let v = 100 - r;
          txn.insert(i, v).unwrap();
        }

        for i in 20..40 {
          let mut rng = OsRng;
          let r = rng.gen_range(0..100);
          let v = 100 + r;
          txn.insert(i, v).unwrap();
        }

        // We are only doing writes, so there won't be any conflicts.
        let _a = txn
          .commit_with_task::<_, std::convert::Infallible, ()>(|_| async {})
          .await
          .unwrap()
          .await;
      })
    })
    .collect::<FuturesUnordered<_>>();
  while handles.next().await.is_some() {}
  closer1.signal_and_wait().await;
  std::thread::sleep(Duration::from_millis(10));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
#[cfg(feature = "tokio")]
async fn txn_commit_with_callback_tokio() {
  txn_commit_with_callback_in::<TokioSpawner, _>(tokio::task::yield_now).await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_commit_with_callback_async_std() {
  txn_commit_with_callback_in::<AsyncStdSpawner, _>(async_std::task::yield_now).await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_commit_with_callback_smol() {
  smol::block_on(txn_commit_with_callback_in::<SmolSpawner, _>(
    smol::future::yield_now,
  ));
}

async fn txn_write_skew_in<S: AsyncSpawner>() {
  // accounts
  let a999 = 999;
  let a888 = 888;
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;

  // Set balance to $100 in each account.
  let mut txn = db.write().await;
  txn.insert(a999, 100).unwrap();
  txn.insert(a888, 100).unwrap();
  txn.commit().await.unwrap();
  assert_eq!(1, db.version().await);

  async fn get_bal<'a, S: AsyncSpawner>(
    txn: &'a mut OptimisticTransaction<u64, u64, S>,
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
#[cfg(feature = "tokio")]
async fn txn_write_skew_tokio() {
  txn_write_skew_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_write_skew_async_std() {
  txn_write_skew_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_write_skew_smol() {
  smol::block_on(txn_write_skew_in::<SmolSpawner>());
}

// https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
async fn txn_write_skew2_in<S: AsyncSpawner>() {
  let db: OptimisticDb<&'static str, u64, S> = OptimisticDb::new().await;

  // Setup
  let mut txn = db.write().await;
  txn.insert("a1", 10).unwrap();
  txn.insert("a2", 20).unwrap();
  txn.insert("b1", 100).unwrap();
  txn.insert("b2", 200).unwrap();
  txn.commit().await.unwrap();
  assert_eq!(1, db.version().await);

  let mut txn1 = db.write().await;
  let val = txn1
    .iter()
    .unwrap()
    .filter_map(|ele| {
      if ele.key().starts_with('a') {
        Some(*ele.value())
      } else {
        None
      }
    })
    .sum::<u64>();
  txn1.insert("b3", 30).unwrap();
  assert_eq!(30, val);

  let mut txn2 = db.write().await;
  let val = txn2
    .iter()
    .unwrap()
    .filter_map(|ele| {
      if ele.key().starts_with('b') {
        Some(*ele.value())
      } else {
        None
      }
    })
    .sum::<u64>();
  txn2.insert("a3", 300).unwrap();
  assert_eq!(300, val);
  txn2.commit().await.unwrap();
  txn1.commit().await.unwrap_err();

  let mut txn3 = db.write().await;
  let val = txn3
    .iter()
    .unwrap()
    .filter_map(|ele| {
      if ele.key().starts_with('a') {
        Some(*ele.value())
      } else {
        None
      }
    })
    .sum::<u64>();
  assert_eq!(330, val);
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn txn_write_skew2_tokio() {
  txn_write_skew2_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_write_skew2_async_std() {
  txn_write_skew2_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_write_skew2_smol() {
  smol::block_on(txn_write_skew2_in::<SmolSpawner>());
}

async fn txn_conflict_get_in<S: AsyncSpawner>() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;
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
            Ok(h) => {
              let _ = h.await;
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
#[cfg(feature = "tokio")]
async fn txn_conflict_get_tokio() {
  txn_conflict_get_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_conflict_get_async_std() {
  txn_conflict_get_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_conflict_get_smol() {
  smol::block_on(txn_conflict_get_in::<SmolSpawner>());
}

async fn txn_versions_in<S: AsyncSpawner>() {
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;

  let k0 = 0;
  for i in 1..10 {
    let mut txn = db.write().await;
    txn.insert(k0, i).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(i, db.version().await);
  }

  let check_iter = |itr: TransactionIter<'_, u64, u64, HashCm<u64, RandomState>>, i: u64| {
    let mut count = 0;
    for ent in itr {
      assert_eq!(ent.key(), &k0);
      assert_eq!(ent.value(), i, "{i} {:?}", ent.value());
      count += 1;
    }
    assert_eq!(1, count) // should only loop once.
  };

  let check_rev_iter = |itr: WriteTransactionRevIter<'_, u64, u64, HashCm<u64, RandomState>>,
                        i: u64| {
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
#[cfg(feature = "tokio")]
async fn txn_versions_tokio() {
  txn_versions_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_versions_async_std() {
  txn_versions_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_versions_smol() {
  smol::block_on(txn_versions_in::<SmolSpawner>());
}

async fn txn_conflict_iter_in<S: AsyncSpawner>() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;
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
            Ok(h) => {
              let _ = h.await;
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
#[cfg(feature = "tokio")]
async fn txn_conflict_iter_tokio() {
  txn_conflict_iter_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_conflict_iter_async_std() {
  txn_conflict_iter_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
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
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;

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

  let check_iter = |itr: TransactionIter<'_, u64, u64, HashCm<u64, RandomState>>,
                    expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value(), "read_vs={}", ent.version());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let check_rev_iter = |itr: WriteTransactionRevIter<'_, u64, u64, HashCm<u64, RandomState>>,
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
#[cfg(feature = "tokio")]
async fn txn_iteration_edge_case_tokio() {
  txn_iteration_edge_case_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_iteration_edge_case_async_std() {
  txn_iteration_edge_case_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_iteration_edge_case_smol() {
  smol::block_on(txn_iteration_edge_case_in::<SmolSpawner>());
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
async fn txn_iteration_edge_case2_in<S: AsyncSpawner>() {
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;

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

  let check_iter = |itr: TransactionIter<'_, u64, u64, HashCm<u64, RandomState>>,
                    expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let check_rev_iter = |itr: WriteTransactionRevIter<'_, u64, u64, HashCm<u64, RandomState>>,
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
#[cfg(feature = "tokio")]
async fn txn_iteration_edge_case2_tokio() {
  txn_iteration_edge_case2_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_iteration_edge_case2_async_std() {
  txn_iteration_edge_case2_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_iteration_edge_case2_smol() {
  smol::block_on(txn_iteration_edge_case2_in::<SmolSpawner>());
}

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
async fn txn_range_edge_case2_in<S: AsyncSpawner>() {
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;

  // c1
  {
    let mut txn = db.write().await;

    txn.insert(0, 0).unwrap();
    txn.insert(u64::MAX, u64::MAX).unwrap();

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

  let check_iter = |itr: TransactionRange<'_, _, _, u64, u64, HashCm<u64, RandomState>>,
                    expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let check_rev_iter =
    |itr: WriteTransactionRevRange<'_, _, _, u64, u64, HashCm<u64, RandomState>>,
     expected: &[u64]| {
      let mut i = 0;
      for ent in itr {
        assert_eq!(expected[i], *ent.value());
        i += 1;
      }
      assert_eq!(expected.len(), i);
    };

  let mut txn = db.write().await;
  let itr = txn.range(1..10).unwrap();
  check_iter(itr, &[13, 32]);
  let itr = txn.range_rev(1..10).unwrap();
  check_rev_iter(itr, &[32, 13]);

  txn.wtm.__set_read_version(5);
  let itr = txn.range(1..10).unwrap();
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

  let itr = txn.range(1..10).unwrap();
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
  let itr = txn.range(1..10).unwrap();
  check_iter(itr, &[13, 23, 32]);

  let itr = txn.range_rev(1..10).unwrap();
  check_rev_iter(itr, &[32, 23, 13]);

  txn.wtm.__set_read_version(2);
  let itr = txn.range(1..10).unwrap();
  check_iter(itr, &[12, 32]);

  let itr = txn.range_rev(1..10).unwrap();
  check_rev_iter(itr, &[32, 12]);

  txn.wtm.__set_read_version(1);
  let itr = txn.range(1..10).unwrap();
  check_iter(itr, &[31]);
  let itr = txn.range_rev(1..10).unwrap();
  check_rev_iter(itr, &[31]);
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn txn_range_edge_case2_tokio() {
  txn_range_edge_case2_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn txn_range_edge_case2_async_std() {
  txn_range_edge_case2_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn txn_range_edge_case2_smol() {
  smol::block_on(txn_range_edge_case2_in::<SmolSpawner>());
}

async fn compact_in<S: AsyncSpawner, Y>(yielder: impl Fn() -> Y + Send + Sync + 'static)
where
  Y: Future<Output = ()> + Send + Sync + 'static,
{
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;
  let mut txn = db.write().await;
  let k = 88;
  for i in 0..40 {
    txn.insert(k, i).unwrap();
    txn.insert(i, 100).unwrap();
  }
  txn.commit().await.unwrap();

  let mut txn = db.write().await;
  txn.remove(k).unwrap();
  txn.commit().await.unwrap();

  let closer = AsyncCloser::<S>::new(1);

  let db1 = db.clone();
  let closer1 = closer.clone();
  S::spawn(async move {
    scopeguard::defer!(closer.done(););
    let rx = closer.listen();
    loop {
      futures::select! {
        _ = rx.wait().fuse() => return,
        default => {
          // Keep checking balance variant
          let txn = db1.read().await;
          let mut total_balance = 0;

          for i in 0..40 {
            let _item = txn.get(&i).unwrap();
            total_balance += 100;
          }
          assert_eq!(total_balance, 4000);
          yielder().await;
        }
      }
    }
  })
  .detach();

  let mut handles = (0..100)
    .map(|_| {
      let db1 = db.clone();
      S::spawn(async move {
        let mut txn = db1.write().await;
        for i in 0..20 {
          let mut rng = OsRng;
          let r = rng.gen_range(0..100);
          let v = 100 - r;
          txn.insert(i, v).unwrap();
        }

        for i in 20..40 {
          let mut rng = OsRng;
          let r = rng.gen_range(0..100);
          let v = 100 + r;
          txn.insert(i, v).unwrap();
        }

        // We are only doing writes, so there won't be any conflicts.
        let _a = txn
          .commit_with_task::<_, std::convert::Infallible, ()>(|_| async {})
          .await
          .unwrap()
          .await;
      })
    })
    .collect::<FuturesUnordered<_>>();

  while handles.next().await.is_some() {}

  closer1.signal_and_wait().await;
  std::thread::sleep(Duration::from_millis(1000));

  let map = db.as_inner().__by_ref();
  assert_eq!(map.len(), 41);

  for i in 0..40 {
    assert_eq!(map.get(&i).unwrap().value().len(), 101);
  }

  db.compact();
  assert_eq!(map.len(), 40);
  for i in 0..40 {
    assert!(map.get(&i).unwrap().value().len() < 101);
  }
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn compact_tokio() {
  compact_in::<TokioSpawner, _>(tokio::task::yield_now).await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn compact_async_std() {
  compact_in::<AsyncStdSpawner, _>(async_std::task::yield_now).await;
}

#[test]
#[cfg(feature = "smol")]
fn compact_smol() {
  smol::block_on(compact_in::<SmolSpawner, _>(smol::future::yield_now));
}

async fn rollback_in<S: AsyncSpawner>() {
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;
  let mut txn = db.write().await;
  txn.insert(1, 1).unwrap();
  txn.insert(2, 2).unwrap();
  txn.insert(3, 3).unwrap();
  txn.commit().await.unwrap();

  let mut txn = db.write().await;
  txn.insert(4, 4).unwrap();
  txn.insert(5, 5).unwrap();
  txn.insert(6, 6).unwrap();
  txn.rollback().unwrap();

  let txn = db.read().await;
  assert_eq!(txn.get(&1).unwrap().value(), &1);
  assert_eq!(txn.get(&2).unwrap().value(), &2);
  assert_eq!(txn.get(&3).unwrap().value(), &3);
  assert!(txn.get(&4).is_none());
  assert!(txn.get(&5).is_none());
  assert!(txn.get(&6).is_none());
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn rollback_tokio() {
  rollback_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn rollback_async_std() {
  rollback_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn rollback_smol() {
  smol::block_on(rollback_in::<SmolSpawner>());
}

async fn iter_in<S: AsyncSpawner>() {
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;
  let mut txn = db.write().await;
  txn.insert(1, 1).unwrap();
  txn.insert(2, 2).unwrap();
  txn.insert(3, 3).unwrap();
  txn.commit().await.unwrap();

  let txn = db.read().await;
  let iter = txn.iter();
  let mut count = 0;
  for ent in iter {
    count += 1;
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
  }
  assert_eq!(count, 3);

  let iter = txn.iter_rev();
  let mut count = 3;
  for ent in iter {
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    count -= 1;
  }
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn iter_tokio() {
  iter_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn iter_async_std() {
  iter_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn iter_smol() {
  smol::block_on(iter_in::<SmolSpawner>());
}

async fn range_in<S: AsyncSpawner>() {
  let db: OptimisticDb<u64, u64, S> = OptimisticDb::new().await;
  let mut txn = db.write().await;
  txn.insert(1, 1).unwrap();
  txn.insert(2, 2).unwrap();
  txn.insert(3, 3).unwrap();
  txn.commit().await.unwrap();

  let txn = db.read().await;
  let iter = txn.range(1..4);
  let mut count = 0;
  for ent in iter {
    count += 1;
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
  }
  assert_eq!(count, 3);

  let iter = txn.range_rev(1..4);
  let mut count = 3;
  for ent in iter {
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    count -= 1;
  }
}

#[tokio::test]
#[cfg(feature = "tokio")]
async fn range_tokio() {
  range_in::<TokioSpawner>().await;
}

#[async_std::test]
#[cfg(feature = "async-std")]
async fn range_async_std() {
  range_in::<AsyncStdSpawner>().await;
}

#[test]
#[cfg(feature = "smol")]
fn range_smol() {
  smol::block_on(range_in::<SmolSpawner>());
}

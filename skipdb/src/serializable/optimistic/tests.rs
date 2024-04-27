use std::{
  sync::atomic::{AtomicU32, Ordering},
  time::Duration,
};

use rand::Rng;
use wmark::Closer;

use super::*;

mod write_skew;

#[test]
fn begin_tx_readable() {
  let db: SerializableDb<&'static str, Vec<u8>> = SerializableDb::new();
  let tx = db.read();
  assert_eq!(tx.version(), 0);
}

#[test]
fn begin_tx_writeable() {
  let db: SerializableDb<&'static str, Vec<u8>> = SerializableDb::new();
  let tx = db.optimistic_write();
  assert_eq!(tx.version(), 0);
}

#[test]
fn writeable_tx() {
  let db: SerializableDb<&'static str, &'static str> = SerializableDb::new();
  {
    let mut tx = db.optimistic_write();
    assert_eq!(tx.version(), 0);

    tx.insert("foo", "foo1").unwrap();
    assert_eq!(*tx.get(&"foo").unwrap().unwrap().value(), "foo1");
    assert!(tx.contains_key(&"foo").unwrap());
    tx.commit().unwrap();
  }

  {
    let tx = db.read();
    assert_eq!(tx.version(), 1);
    assert_eq!(*tx.get("foo").unwrap().value(), "foo1");
    assert!(tx.contains_key("foo"));
  }
}

#[test]
fn txn_simple() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();

  {
    let mut txn = db.optimistic_write();
    for i in 0..10 {
      if let Err(e) = txn.insert(i, i) {
        panic!("{e}");
      }
    }

    let item = txn.get(&8).unwrap().unwrap();
    assert!(!item.is_committed());
    assert_eq!(*item.value(), 8);
    drop(item);

    txn.commit().unwrap();
  }

  let k = 8;
  let v = 8;
  let txn = db.read();
  let item = txn.get(&k).unwrap();
  assert_eq!(*item.value(), v);
}

#[test]
fn txn_read_after_write() {
  const N: u64 = 100;

  let db: SerializableDb<u64, u64> = SerializableDb::new();

  let handles = (0..N)
    .map(|i| {
      let db = db.clone();
      std::thread::spawn(move || {
        let k = i;
        let v = i;

        let mut txn = db.optimistic_write();
        txn.insert(k, v).unwrap();
        txn.commit().unwrap();

        let txn = db.read();
        let k = i;
        let v = i;
        let item = txn.get(&k).unwrap();
        assert_eq!(*item.value(), v);
      })
    })
    .collect::<Vec<_>>();

  handles.into_iter().for_each(|h| {
    h.join().unwrap();
  });
}

#[test]
fn txn_commit_with_callback() {
  use rand::thread_rng;

  let db: SerializableDb<u64, u64> = SerializableDb::new();
  let mut txn = db.optimistic_write();
  for i in 0..40 {
    txn.insert(i, 100).unwrap();
  }
  txn.commit().unwrap();

  let closer = Closer::new(1);

  let db1 = db.clone();
  let closer1 = closer.clone();
  std::thread::spawn(move || {
    scopeguard::defer!(closer.done(););

    loop {
      crossbeam_channel::select! {
        recv(closer.listen()) -> _ => return,
        default => {
          // Keep checking balance variant
          let txn = db1.read();
          let mut total_balance = 0;

          for i in 0..40 {
            let _item = txn.get(&i).unwrap();
            total_balance += 100;
          }
          assert_eq!(total_balance, 4000);
        }
      }
    }
  });

  let handles = (0..100)
    .map(|_| {
      let db1 = db.clone();
      std::thread::spawn(move || {
        let mut txn = db1.optimistic_write();
        for i in 0..20 {
          let mut rng = thread_rng();
          let r = rng.gen_range(0..100);
          let v = 100 - r;
          txn.insert(i, v).unwrap();
        }

        for i in 20..40 {
          let mut rng = thread_rng();
          let r = rng.gen_range(0..100);
          let v = 100 + r;
          txn.insert(i, v).unwrap();
        }

        // We are only doing writes, so there won't be any conflicts.
        let _ = txn
          .commit_with_callback::<std::convert::Infallible, ()>(|_| {})
          .unwrap();
      })
    })
    .collect::<Vec<_>>();

  for h in handles {
    h.join().unwrap();
  }

  closer1.signal_and_wait();
  std::thread::sleep(Duration::from_millis(10));
}

#[test]
fn txn_conflict_get() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: SerializableDb<u64, u64> = SerializableDb::new();
    set_count.store(0, Ordering::SeqCst);
    let handles = (0..16).map(|_| {
      let db1 = db.clone();
      let set_count1 = set_count.clone();
      std::thread::spawn(move || {
        let mut txn = db1.optimistic_write();
        if txn.get(&100).unwrap().is_none() {
          txn.insert(100, 999).unwrap();
          if let Err(e) = txn.commit_with_callback::<std::convert::Infallible, _>(move |e| {
            match e {
              Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
              Err(e) => panic!("{e}"),
            };
          }) {
            assert!(matches!(
              e,
              WtmError::Transaction(TransactionError::Conflict)
            ));
          }
        }
      })
    });

    for h in handles {
      h.join().unwrap();
    }

    assert_eq!(1, set_count.load(Ordering::SeqCst));
  }
}

#[test]
fn txn_versions() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();

  let k0 = 0;
  for i in 1..10 {
    let mut txn = db.optimistic_write();
    txn.insert(k0, i).unwrap();
    txn.commit().unwrap();
    assert_eq!(i, db.version());
  }

  let check_iter = |itr: TransactionIter<'_, u64, u64, BTreeCm<u64>>, i: u64| {
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
    let mut txn = db.optimistic_write();
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

  let mut txn = db.optimistic_write();
  let item = txn.get(&k0).unwrap().unwrap();
  let val = *item.value();
  assert_eq!(9, val)
}

#[test]
fn txn_conflict_iter() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: SerializableDb<u64, u64> = SerializableDb::new();
    set_count.store(0, Ordering::SeqCst);
    let handles = (0..16).map(|_| {
      let db1 = db.clone();
      let set_count1 = set_count.clone();
      std::thread::spawn(move || {
        let mut txn = db1.optimistic_write();

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
          if let Err(e) = txn.commit_with_callback::<std::convert::Infallible, ()>(move |e| {
            match e {
              Ok(_) => assert!(set_count1.fetch_add(1, Ordering::SeqCst) + 1 >= 1),
              Err(e) => panic!("{e}"),
            };
          }) {
            assert!(matches!(
              e,
              WtmError::Transaction(TransactionError::Conflict)
            ));
          }
        }
      })
    });

    for h in handles {
      h.join().unwrap();
    }

    assert_eq!(1, set_count.load(Ordering::SeqCst));
  }
}

/// a3, a2, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=4(Uncommitted) -> a3, b4
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn txn_iteration_edge_case() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();

  // c1
  {
    let mut txn = db.optimistic_write();
    txn.insert(3, 31).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, db.version());
  }

  // a2, c2
  {
    let mut txn = db.optimistic_write();
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().unwrap();
    assert_eq!(2, db.version());
  }

  // b3
  {
    let mut txn = db.optimistic_write();
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().unwrap();
    assert_eq!(3, db.version());
  }

  // b4, c4(remove) (uncommitted)
  let mut txn4 = db.optimistic_write();
  txn4.insert(2, 24).unwrap();
  txn4.remove(3).unwrap();
  assert_eq!(3, db.version());

  // b4 (remove)
  {
    let mut txn = db.optimistic_write();
    txn.remove(2).unwrap();
    txn.commit().unwrap();
    assert_eq!(4, db.version());
  }

  let check_iter = |itr: TransactionIter<'_, u64, u64, BTreeCm<u64>>, expected: &[u64]| {
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

  let mut txn = db.optimistic_write();
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

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn txn_iteration_edge_case2() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();

  // c1
  {
    let mut txn = db.optimistic_write();
    txn.insert(3, 31).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, db.version());
  }

  // a2, c2
  {
    let mut txn = db.optimistic_write();
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().unwrap();
    assert_eq!(2, db.version());
  }

  // b3
  {
    let mut txn = db.optimistic_write();
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().unwrap();
    assert_eq!(3, db.version());
  }

  // b4 (remove)
  {
    let mut txn = db.optimistic_write();
    txn.remove(2).unwrap();
    txn.commit().unwrap();
    assert_eq!(4, db.version());
  }

  let check_iter = |itr: TransactionIter<'_, u64, u64, BTreeCm<u64>>, expected: &[u64]| {
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

  let mut txn = db.optimistic_write();
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

/// a2, a3, b4 (del), b3, c2, c1
/// Read at ts=4 -> a3, c2
/// Read at ts=3 -> a3, b3, c2
/// Read at ts=2 -> a2, c2
/// Read at ts=1 -> c1
#[test]
fn txn_range_edge_case2() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();

  // c1
  {
    let mut txn = db.optimistic_write();

    txn.insert(0, 0).unwrap();
    txn.insert(u64::MAX, u64::MAX).unwrap();

    txn.insert(3, 31).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, db.version());
  }

  // a2, c2
  {
    let mut txn = db.optimistic_write();
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().unwrap();
    assert_eq!(2, db.version());
  }

  // b3
  {
    let mut txn = db.optimistic_write();
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().unwrap();
    assert_eq!(3, db.version());
  }

  // b4 (remove)
  {
    let mut txn = db.optimistic_write();
    txn.remove(2).unwrap();
    txn.commit().unwrap();
    assert_eq!(4, db.version());
  }

  let check_iter = |itr: TransactionRange<'_, _, _, u64, u64, BTreeCm<u64>>, expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let check_rev_iter = |itr: WriteTransactionRevRange<'_, _, _, u64, u64, BTreeCm<u64>>,
                        expected: &[u64]| {
    let mut i = 0;
    for ent in itr {
      assert_eq!(expected[i], *ent.value());
      i += 1;
    }
    assert_eq!(expected.len(), i);
  };

  let mut txn = db.optimistic_write();
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

#[test]
fn compact() {
  use rand::thread_rng;

  let db: SerializableDb<u64, u64> = SerializableDb::new();
  let mut txn = db.optimistic_write();
  let k = 88;
  for i in 0..40 {
    txn.insert(k, i).unwrap();
    txn.insert(i, 100).unwrap();
  }
  txn.commit().unwrap();

  let mut txn = db.optimistic_write();
  txn.remove(k).unwrap();
  txn.commit().unwrap();

  let closer = Closer::new(1);

  let db1 = db.clone();
  let closer1 = closer.clone();
  std::thread::spawn(move || {
    scopeguard::defer!(closer.done(););

    loop {
      crossbeam_channel::select! {
        recv(closer.listen()) -> _ => return,
        default => {
          // Keep checking balance variant
          let txn = db1.read();
          let mut total_balance = 0;

          for i in 0..40 {
            let _item = txn.get(&i).unwrap();
            total_balance += 100;
          }
          assert_eq!(total_balance, 4000);
          std::thread::yield_now();
        }
      }
    }
  });

  let handles = (0..10)
    .map(|_| {
      let db1 = db.clone();
      std::thread::spawn(move || {
        let mut txn = db1.optimistic_write();
        for i in 0..20 {
          let mut rng = thread_rng();
          let r = rng.gen_range(0..100);
          let v = 100 - r;
          txn.insert(i, v).unwrap();
        }

        for i in 20..40 {
          let mut rng = thread_rng();
          let r = rng.gen_range(0..100);
          let v = 100 + r;
          txn.insert(i, v).unwrap();
        }

        // We are only doing writes, so there won't be any conflicts.
        let _ = txn
          .commit_with_callback::<std::convert::Infallible, ()>(|_| {})
          .unwrap();
        std::thread::yield_now();
      })
    })
    .collect::<Vec<_>>();

  for h in handles {
    h.join().unwrap();
  }

  closer1.signal_and_wait();
  std::thread::sleep(Duration::from_millis(1000));

  let map = db.as_inner().__by_ref();
  assert_eq!(map.len(), 41);

  for i in 0..40 {
    assert_eq!(map.get(&i).unwrap().value().len(), 11);
  }

  db.compact();
  assert_eq!(map.len(), 40);
  for i in 0..40 {
    assert!(map.get(&i).unwrap().value().len() < 11);
  }
}

#[test]
fn rollback() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();
  let mut txn = db.optimistic_write();
  txn.insert(1, 1).unwrap();
  txn.rollback().unwrap();
  assert!(txn.get(&1).unwrap().is_none());
}

#[test]
fn iter() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();
  let mut txn = db.optimistic_write();
  txn.insert(1, 1).unwrap();
  txn.insert(2, 2).unwrap();
  txn.insert(3, 3).unwrap();
  txn.commit().unwrap();

  let txn = db.read();
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

#[test]
fn iter2() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();
  let mut txn = db.optimistic_write();
  txn.insert(1, 1).unwrap();
  txn.insert(2, 2).unwrap();
  txn.insert(3, 3).unwrap();

  let iter = txn.iter().unwrap();
  let mut count = 0;
  for ent in iter {
    count += 1;
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    assert_eq!(ent.version(), 0);
  }
  assert_eq!(count, 3);

  let iter = txn.iter_rev().unwrap();
  let mut count = 3;
  for ent in iter {
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    assert_eq!(ent.version(), 0);
    count -= 1;
  }

  txn.commit().unwrap();

  let mut txn = db.optimistic_write();
  txn.insert(4, 4).unwrap();
  txn.insert(5, 5).unwrap();
  txn.insert(6, 6).unwrap();

  let iter = txn.iter().unwrap();
  let mut count = 0;
  for ent in iter {
    count += 1;
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    assert_eq!(ent.version(), 1);
  }
  assert_eq!(count, 6);

  let iter = txn.iter_rev().unwrap();
  let mut count = 6;
  for ent in iter {
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    assert_eq!(ent.version(), 1);
    count -= 1;
  }
}

#[test]
fn range() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();
  let mut txn = db.optimistic_write();
  txn.insert(1, 1).unwrap();
  txn.insert(2, 2).unwrap();
  txn.insert(3, 3).unwrap();
  txn.commit().unwrap();

  let txn = db.read();
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

#[test]
fn range2() {
  let db: SerializableDb<u64, u64> = SerializableDb::new();
  let mut txn = db.optimistic_write();
  txn.insert(1, 1).unwrap();
  txn.insert(2, 2).unwrap();
  txn.insert(3, 3).unwrap();

  let iter = txn.range(1..4).unwrap();
  let mut count = 0;
  for ent in iter {
    count += 1;
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    assert_eq!(ent.version(), 0);
  }
  assert_eq!(count, 3);

  let iter = txn.range_rev(1..4).unwrap();
  let mut count = 3;
  for ent in iter {
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    assert_eq!(ent.version(), 0);
    count -= 1;
  }

  txn.commit().unwrap();

  let mut txn = db.optimistic_write();
  txn.insert(4, 4).unwrap();
  txn.insert(5, 5).unwrap();
  txn.insert(6, 6).unwrap();

  let iter = txn.range(1..5).unwrap();
  let mut count = 0;
  for ent in iter {
    count += 1;
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
  }
  assert_eq!(count, 4);

  let iter = txn.range_rev(1..5).unwrap();
  let mut count = 4;
  for ent in iter {
    assert_eq!(ent.key(), &count);
    assert_eq!(ent.value(), count);
    count -= 1;
  }
}

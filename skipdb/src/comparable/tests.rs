use std::{
  sync::atomic::{AtomicU32, Ordering},
  time::Duration,
};

use rand::Rng;
use skipdb_core::rev_range::WriteTransactionRevRange;
use txn::error::WtmError;
use wmark::Closer;

use super::*;

#[test]
fn begin_tx_readable() {
  let db: ComparableDb<&'static str, Vec<u8>> = ComparableDb::new();
  let tx = db.read();
  assert_eq!(tx.version(), 0);
}

#[test]
fn begin_tx_writeable() {
  let db: ComparableDb<&'static str, Vec<u8>> = ComparableDb::new();
  let tx = db.write();
  assert_eq!(tx.version(), 0);
}

#[test]
fn writeable_tx() {
  let db: ComparableDb<&'static str, &'static str> = ComparableDb::new();
  {
    let mut tx = db.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();

  {
    let mut txn = db.write();
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

  let db: ComparableDb<u64, u64> = ComparableDb::new();

  let handles = (0..N)
    .map(|i| {
      let db = db.clone();
      std::thread::spawn(move || {
        let k = i;
        let v = i;

        let mut txn = db.write();
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

  let db: ComparableDb<u64, u64> = ComparableDb::new();
  let mut txn = db.write();
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
        let mut txn = db1.write();
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
fn txn_write_skew() {
  // accounts
  let a999 = 999;
  let a888 = 888;
  let db: ComparableDb<u64, u64> = ComparableDb::new();

  // Set balance to $100 in each account.
  let mut txn = db.write();
  txn.insert(a999, 100).unwrap();
  txn.insert(a888, 100).unwrap();
  txn.commit().unwrap();
  assert_eq!(1, db.version());

  let get_bal = |txn: &mut WriteTransaction<u64, u64>, k: &u64| -> u64 {
    let item = txn.get(k).unwrap().unwrap();
    let val = *item.value();
    val
  };

  // Start two transactions, each would read both accounts and deduct from one account.
  let mut txn1 = db.write();

  let mut sum = get_bal(&mut txn1, &a999);
  sum += get_bal(&mut txn1, &a888);
  assert_eq!(200, sum);
  txn1.insert(a999, 0).unwrap(); // Deduct 100 from a999

  // Let's read this back.
  let mut sum = get_bal(&mut txn1, &a999);
  assert_eq!(0, sum);
  sum += get_bal(&mut txn1, &a888);
  assert_eq!(100, sum);
  // Don't commit yet.

  let mut txn2 = db.write();

  let mut sum = get_bal(&mut txn2, &a999);
  sum += get_bal(&mut txn2, &a888);
  assert_eq!(200, sum);
  txn2.insert(a888, 0).unwrap(); // Deduct 100 from a888

  // Let's read this back.
  let mut sum = get_bal(&mut txn2, &a999);
  assert_eq!(100, sum);
  sum += get_bal(&mut txn2, &a888);
  assert_eq!(100, sum);

  // Commit both now.
  txn1.commit().unwrap();
  txn2.commit().unwrap_err(); // This should fail

  assert_eq!(2, db.version());
}

// https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
#[test]
fn txn_write_skew2() {
  let db: ComparableDb<&'static str, u64> = ComparableDb::new();

  // Setup
  let mut txn = db.write();
  txn.insert("a1", 10).unwrap();
  txn.insert("a2", 20).unwrap();
  txn.insert("b1", 100).unwrap();
  txn.insert("b2", 200).unwrap();
  txn.commit().unwrap();
  assert_eq!(1, db.version());

  //
  let mut txn1 = db.write();
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

  let mut txn2 = db.write();
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
  txn2.commit().unwrap();
  txn1.commit().unwrap_err();

  let mut txn3 = db.write();
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

// https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
#[test]
fn txn_write_skew3() {
  let db: ComparableDb<&'static str, u64> = ComparableDb::new();

  // Setup
  let mut txn = db.write();
  txn.insert("a1", 10).unwrap();
  txn.insert("b1", 100).unwrap();
  txn.insert("b2", 200).unwrap();
  txn.commit().unwrap();
  assert_eq!(1, db.version());

  //
  let mut txn1 = db.write();
  let val = txn1
    .range("a".."b")
    .unwrap()
    .map(|ele| *ele.value())
    .sum::<u64>();
  txn1.insert("b3", 10).unwrap();
  assert_eq!(10, val);

  let mut txn2 = db.write();
  let val = txn2
    .range("b".."c")
    .unwrap()
    .map(|ele| *ele.value())
    .sum::<u64>();
  txn2.insert("a3", 300).unwrap();
  assert_eq!(300, val);
  txn2.commit().unwrap();
  txn1.commit().unwrap_err();

  let mut txn3 = db.write();
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
  assert_eq!(310, val);
}

// https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
#[test]
fn txn_write_skew4() {
  let db: ComparableDb<&'static str, u64> = ComparableDb::new();

  // Setup
  let mut txn = db.write();
  txn.insert("b1", 100).unwrap();
  txn.insert("b2", 200).unwrap();
  txn.commit().unwrap();
  assert_eq!(1, db.version());

  //
  let mut txn1 = db.write();
  let val = txn1
    .range("a".."b")
    .unwrap()
    .map(|ele| *ele.value())
    .sum::<u64>();
  txn1.insert("b3", 0).unwrap();
  assert_eq!(0, val);

  let mut txn2 = db.write();
  let val = txn2
    .range("b".."c")
    .unwrap()
    .map(|ele| *ele.value())
    .sum::<u64>();
  txn2.insert("a3", 300).unwrap();
  assert_eq!(300, val);
  txn2.commit().unwrap();
  txn1.commit().unwrap_err();

  let mut txn3 = db.write();
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
  assert_eq!(300, val);
}

#[test]
fn txn_conflict_get() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: ComparableDb<u64, u64> = ComparableDb::new();
    set_count.store(0, Ordering::SeqCst);
    let handles = (0..16).map(|_| {
      let db1 = db.clone();
      let set_count1 = set_count.clone();
      std::thread::spawn(move || {
        let mut txn = db1.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();

  let k0 = 0;
  for i in 1..10 {
    let mut txn = db.write();
    txn.insert(k0, i).unwrap();
    txn.commit().unwrap();
    assert_eq!(i, db.version());
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
    let mut txn = db.write();
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

  let mut txn = db.write();
  let item = txn.get(&k0).unwrap().unwrap();
  let val = *item.value();
  assert_eq!(9, val)
}

#[test]
fn txn_conflict_iter() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: ComparableDb<u64, u64> = ComparableDb::new();
    set_count.store(0, Ordering::SeqCst);
    let handles = (0..16).map(|_| {
      let db1 = db.clone();
      let set_count1 = set_count.clone();
      std::thread::spawn(move || {
        let mut txn = db1.write();

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
  let db: ComparableDb<u64, u64> = ComparableDb::new();

  // c1
  {
    let mut txn = db.write();
    txn.insert(3, 31).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, db.version());
  }

  // a2, c2
  {
    let mut txn = db.write();
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().unwrap();
    assert_eq!(2, db.version());
  }

  // b3
  {
    let mut txn = db.write();
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().unwrap();
    assert_eq!(3, db.version());
  }

  // b4, c4(remove) (uncommitted)
  let mut txn4 = db.write();
  txn4.insert(2, 24).unwrap();
  txn4.remove(3).unwrap();
  assert_eq!(3, db.version());

  // b4 (remove)
  {
    let mut txn = db.write();
    txn.remove(2).unwrap();
    txn.commit().unwrap();
    assert_eq!(4, db.version());
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

  let mut txn = db.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();

  // c1
  {
    let mut txn = db.write();
    txn.insert(3, 31).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, db.version());
  }

  // a2, c2
  {
    let mut txn = db.write();
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().unwrap();
    assert_eq!(2, db.version());
  }

  // b3
  {
    let mut txn = db.write();
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().unwrap();
    assert_eq!(3, db.version());
  }

  // b4 (remove)
  {
    let mut txn = db.write();
    txn.remove(2).unwrap();
    txn.commit().unwrap();
    assert_eq!(4, db.version());
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

  let mut txn = db.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();

  // c1
  {
    let mut txn = db.write();

    txn.insert(0, 0).unwrap();
    txn.insert(u64::MAX, u64::MAX).unwrap();

    txn.insert(3, 31).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, db.version());
  }

  // a2, c2
  {
    let mut txn = db.write();
    txn.insert(1, 12).unwrap();
    txn.insert(3, 32).unwrap();
    txn.commit().unwrap();
    assert_eq!(2, db.version());
  }

  // b3
  {
    let mut txn = db.write();
    txn.insert(1, 13).unwrap();
    txn.insert(2, 23).unwrap();
    txn.commit().unwrap();
    assert_eq!(3, db.version());
  }

  // b4 (remove)
  {
    let mut txn = db.write();
    txn.remove(2).unwrap();
    txn.commit().unwrap();
    assert_eq!(4, db.version());
  }

  let check_iter = |itr: WriteTransactionRange<'_, _, _, u64, u64, BTreeCm<u64>>,
                    expected: &[u64]| {
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

  let mut txn = db.write();
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

  let db: ComparableDb<u64, u64> = ComparableDb::new();
  let mut txn = db.write();
  let k = 88;
  for i in 0..40 {
    txn.insert(k, i).unwrap();
    txn.insert(i, 100).unwrap();
  }
  txn.commit().unwrap();

  let mut txn = db.write();
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
        let mut txn = db1.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();
  let mut txn = db.write();
  txn.insert(1, 1).unwrap();
  txn.rollback().unwrap();
  assert!(txn.get(&1).unwrap().is_none());
}

#[test]
fn iter() {
  let db: ComparableDb<u64, u64> = ComparableDb::new();
  let mut txn = db.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();
  let mut txn = db.write();
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

  let mut txn = db.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();
  let mut txn = db.write();
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
  let db: ComparableDb<u64, u64> = ComparableDb::new();
  let mut txn = db.write();
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

  let mut txn = db.write();
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

use std::{
  sync::atomic::{AtomicU32, Ordering},
  time::Duration,
};

use mwmr::error::WtmError;
use rand::Rng;
use wmark::Closer;

use super::*;

#[test]
fn begin_tx_readable() {
  let db: EquivalentDB<&'static str, Vec<u8>> = EquivalentDB::new();
  let tx = db.read();
  assert_eq!(tx.version(), 0);
}

#[test]
fn begin_tx_writeable() {
  let db: EquivalentDB<&'static str, Vec<u8>> = EquivalentDB::new();
  let tx = db.write();
  assert_eq!(tx.version(), 0);
}

#[test]
fn writeable_tx() {
  let db: EquivalentDB<&'static str, &'static str> = EquivalentDB::new();
  {
    let mut tx = db.write();
    assert_eq!(tx.version(), 0);

    tx.insert("foo", "foo1").unwrap();
    assert_eq!(*tx.get("foo").unwrap().unwrap().value(), "foo1");
    tx.commit().unwrap();
  }

  {
    let tx = db.read();
    assert_eq!(tx.version(), 1);
    assert_eq!(*tx.get("foo").unwrap().value(), "foo1");
  }
}

#[test]
fn txn_simple() {
  let db: EquivalentDB<u64, u64> = EquivalentDB::new();

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

  let db: EquivalentDB<u64, u64> = EquivalentDB::new();

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

  let db: EquivalentDB<u64, u64> = EquivalentDB::new();
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
        recv(closer.has_been_closed()) -> _ => return,
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
  let db: EquivalentDB<u64, u64> = EquivalentDB::new();

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

#[test]
fn txn_conflict_get() {
  let set_count = Arc::new(AtomicU32::new(0));

  for _ in 0..10 {
    let db: EquivalentDB<u64, u64> = EquivalentDB::new();
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
  let db: EquivalentDB<u64, u64> = EquivalentDB::new();

  let k0 = 0;
  for i in 1..10 {
    let mut txn = db.write();
    txn.insert(k0, i).unwrap();
    txn.commit().unwrap();
    assert_eq!(i, db.version());
  }

  let check_iter =
    |itr: WriteTransactionIter<'_, u64, u64, HashCm<u64, std::hash::RandomState>>, i: u64| {
      let mut count = 0;
      for ent in itr {
        assert_eq!(ent.key(), &k0);
        assert_eq!(ent.value(), i, "{i} {:?}", ent.value());
        count += 1;
      }
      assert_eq!(1, count) // should only loop once.
    };

  let check_rev_iter =
    |itr: WriteTransactionRevIter<'_, u64, u64, HashCm<u64, std::hash::RandomState>>, i: u64| {
      let mut count = 0;
      for ent in itr {
        assert_eq!(ent.key(), &k0);
        assert_eq!(ent.value(), i, "{i} {:?}", ent.value());
        count += 1;
      }
      assert_eq!(1, count) // should only loop once.
    };

  let check_all_versions = |itr: WriteTransactionAllVersionsIter<
    '_,
    u64,
    u64,
    HashCm<u64, std::hash::RandomState>,
    EquivalentDB<u64, u64>,
  >,
                            i: u64| {
    let mut version = i;

    let mut count = 0;
    for ents in itr {
      for ent in ents {
        assert_eq!(ent.key(), &k0);
        assert_eq!(ent.version(), version);
        assert_eq!(*ent.value().unwrap(), version);
        count += 1;
        version -= 1;
      }
    }

    assert_eq!(i, count); // Should loop as many times as i.
  };

  let check_rev_all_versions = |itr: WriteTransactionRevAllVersionsIter<
    '_,
    u64,
    u64,
    HashCm<u64, std::hash::RandomState>,
    EquivalentDB<u64, u64>,
  >,
                                i: u64| {
    let mut version = 1;

    let mut count = 0;
    for ents in itr {
      for ent in ents {
        assert_eq!(ent.key(), &k0);
        assert_eq!(ent.version(), version);
        assert_eq!(*ent.value().unwrap(), version);
        count += 1;
        version += 1;
      }
    }

    assert_eq!(i, count); // Should loop as many times as i.
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

    // Now try retrieving all versions forward and reverse.
    let itr = txn.iter_all_versions().unwrap();
    check_all_versions(itr, i);

    let itr = txn.iter_all_versions_rev().unwrap();
    check_rev_all_versions(itr, i);
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
    let db: EquivalentDB<u64, u64> = EquivalentDB::new();
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

#[test]
fn txn_all_versions_with_removed() {
  let db: EquivalentDB<u64, u64> = EquivalentDB::new();
  // write two keys
  {
    let mut txn = db.write();
    txn.insert(1, 42).unwrap();
    txn.insert(2, 43).unwrap();
    txn.commit().unwrap();
  }

  // Delete the specific key version from underlying db directly
  {
    let txn = db.read();
    let item = txn.get(&1).unwrap();
    txn
      .db
      .inner
      .map
      .by_ref()
      .get(&1)
      .unwrap()
      .value()
      .insert(item.version(), None);
  }

  // Verify that deleted shows up when AllVersions is set.
  {
    let txn = db.read();
    let itr = txn.iter_all_versions();

    let mut count = 0;
    for ents in itr {
      count += 1;
      if count == 1 {
        let mut num = 0;
        let k = *ents.key();
        for ent in ents {
          assert_eq!(k, 1);
          assert!(ent.value().is_none());
          num += 1;
        }
        assert_eq!(1, num);
      } else {
        let mut num = 0;
        let k = *ents.key();
        for ent in ents {
          assert_eq!(k, 2);
          assert_eq!(ent.value().as_deref(), Some(&43));
          num += 1;
        }
        assert_eq!(1, num);
      }
    }
    assert_eq!(2, count);
  }

  // Verify that deleted shows up when AllVersions is set.
  {
    let txn = db.read();
    let itr = txn.iter_all_versions_rev();

    let mut count = 0;
    for ents in itr {
      count += 1;
      if count != 1 {
        let mut num = 0;
        let k = *ents.key();
        for ent in ents {
          assert_eq!(k, 1);
          assert!(ent.value().is_none());
          num += 1;
        }
        assert_eq!(1, num);
      } else {
        let mut num = 0;
        let k = *ents.key();
        for ent in ents {
          assert_eq!(k, 2);
          assert_eq!(ent.value().as_deref(), Some(&43));
          num += 1;
        }
        assert_eq!(1, num);
      }
    }
    assert_eq!(2, count);
  }
}

#[test]
fn txn_all_versions_with_removed2() {
  let db: EquivalentDB<u64, u64> = EquivalentDB::new();
  // Set and delete alternatively
  {
    for i in 0..4 {
      let mut txn = db.write();
      if i % 2 == 0 {
        txn.insert(0, 99).unwrap();
      } else {
        txn.remove(0).unwrap();
      }
      txn.commit().unwrap();
    }
  }

  // Verify that deleted shows up when AllVersions is set.
  {
    let txn = db.read();
    let itr = txn.iter_all_versions_rev();

    let mut count = 0;
    for ents in itr {
      for ent in ents {
        if count % 2 == 0 {
          assert_eq!(ent.value().as_deref(), Some(&99));
        } else {
          assert!(ent.value().is_none());
        }
        count += 1
      }
    }
    assert_eq!(4, count);
  }

  // Verify that deleted shows up when AllVersions is set.
  {
    let txn = db.read();
    let itr = txn.iter_all_versions();

    let mut count = 0;
    for ents in itr {
      for ent in ents {
        if count % 2 != 0 {
          assert_eq!(ent.value().as_deref(), Some(&99));
        } else {
          assert!(ent.value().is_none());
        }
        count += 1
      }
    }
    assert_eq!(4, count);
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
  let db: EquivalentDB<u64, u64> = EquivalentDB::new();

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

  let check_iter =
    |itr: WriteTransactionIter<'_, u64, u64, HashCm<u64, std::hash::RandomState>>,
     expected: &[u64]| {
      let mut i = 0;
      for ent in itr {
        assert_eq!(expected[i], *ent.value(), "read_vs={}", ent.version());
        i += 1;
      }
      assert_eq!(expected.len(), i);
    };

  let check_rev_iter =
    |itr: WriteTransactionRevIter<'_, u64, u64, HashCm<u64, std::hash::RandomState>>,
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
  let db: EquivalentDB<u64, u64> = EquivalentDB::new();

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

  let check_iter =
    |itr: WriteTransactionIter<'_, u64, u64, HashCm<u64, std::hash::RandomState>>,
     expected: &[u64]| {
      let mut i = 0;
      for ent in itr {
        assert_eq!(expected[i], *ent.value());
        i += 1;
      }
      assert_eq!(expected.len(), i);
    };

  let check_rev_iter =
    |itr: WriteTransactionRevIter<'_, u64, u64, HashCm<u64, std::hash::RandomState>>,
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

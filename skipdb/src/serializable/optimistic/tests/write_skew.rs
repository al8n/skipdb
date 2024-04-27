use super::*;

#[test]
fn txn_write_skew() {
  // accounts
  let a999 = 999;
  let a888 = 888;
  let db: SerializableDb<u64, u64> = SerializableDb::new();

  // Set balance to $100 in each account.
  let mut txn = db.optimistic_write();
  txn.insert(a999, 100).unwrap();
  txn.insert(a888, 100).unwrap();
  txn.commit().unwrap();
  assert_eq!(1, db.version());

  let get_bal = |txn: &mut OptimisticTransaction<u64, u64>, k: &u64| -> u64 {
    let item = txn.get(k).unwrap().unwrap();
    let val = *item.value();
    val
  };

  // Start two transactions, each would read both accounts and deduct from one account.
  let mut txn1 = db.optimistic_write();

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

  let mut txn2 = db.optimistic_write();

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

// https://wiki.postgresql.org/wiki/SSI#Black_and_White
#[test]
fn txn_write_skew_black_white() {
  let db: SerializableDb<u64, &'static str> = SerializableDb::new();

  // Setup
  let mut txn = db.optimistic_write();
  for i in 1..=10 {
    if i % 2 == 1 {
      txn.insert(i, "black").unwrap();
    } else {
      txn.insert(i, "white").unwrap();
    }
  }
  txn.commit().unwrap();

  // txn1
  let mut txn1 = db.optimistic_write();
  let indices = txn1
    .iter()
    .unwrap()
    .filter_map(|e| {
      if e.value() == "black" {
        Some(*e.key())
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  for i in indices {
    txn1.insert(i, "white").unwrap();
  }

  // txn2
  let mut txn2 = db.optimistic_write();
  let indices = txn2
    .iter()
    .unwrap()
    .filter_map(|e| {
      if e.value() == "white" {
        Some(*e.key())
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  for i in indices {
    txn2.insert(i, "black").unwrap();
  }
  txn2.commit().unwrap();
  txn1.commit().unwrap_err();
}

// https://wiki.postgresql.org/wiki/SSI#Overdraft_Protection
#[test]
fn txn_write_skew_overdraft_protection() {
  let db: SerializableDb<&'static str, u64> = SerializableDb::new();

  // Setup
  let mut txn = db.optimistic_write();
  txn.insert("kevin", 1000).unwrap();
  txn.commit().unwrap();

  // txn1
  let mut txn1 = db.optimistic_write();
  let money = *txn1.get(&"kevin").unwrap().unwrap().value();
  txn1.insert("kevin", money - 100).unwrap();

  // txn2
  let mut txn2 = db.optimistic_write();
  let money = *txn2.get(&"kevin").unwrap().unwrap().value();
  txn2.insert("kevin", money - 100).unwrap();

  txn1.commit().unwrap();
  txn2.commit().unwrap_err();
}

// https://wiki.postgresql.org/wiki/SSI#Primary_Colors
#[test]
fn txn_write_skew_primary_colors() {
  let db: SerializableDb<u64, &'static str> = SerializableDb::new();

  // Setup
  let mut txn = db.optimistic_write();
  for i in 1..=9000 {
    if i % 3 == 1 {
      txn.insert(i, "red").unwrap();
    } else if i % 3 == 2 {
      txn.insert(i, "yellow").unwrap();
    } else {
      txn.insert(i, "blue").unwrap();
    }
  }
  txn.commit().unwrap();

  // txn1
  let mut txn1 = db.optimistic_write();
  let indices = txn1
    .iter()
    .unwrap()
    .filter_map(|e| {
      if e.value() == "yellow" {
        Some(*e.key())
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  for i in indices {
    txn1.insert(i, "red").unwrap();
  }

  // txn2
  let mut txn2 = db.optimistic_write();
  let indices = txn2
    .iter()
    .unwrap()
    .filter_map(|e| {
      if e.value() == "blue" {
        Some(*e.key())
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  for i in indices {
    txn2.insert(i, "yellow").unwrap();
  }

  // txn3
  let mut txn3 = db.optimistic_write();
  let indices = txn3
    .iter()
    .unwrap()
    .filter_map(|e| {
      if e.value() == "blue" {
        Some(*e.key())
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  for i in indices {
    txn3.insert(i, "red").unwrap();
  }

  txn1.commit().unwrap();
  txn3.commit().unwrap_err();
  txn2.commit().unwrap_err();
}

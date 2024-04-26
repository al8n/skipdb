use skipdb::optimistic::OptimisticDb;

#[derive(Debug)]
struct Person {
  hobby: String,
  age: u8,
}

fn main() {
  let db: OptimisticDb<String, Person> = OptimisticDb::new();

  {
    let alice = Person {
      hobby: "swim".to_string(),
      age: 20,
    };
    let bob = Person {
      hobby: "run".to_string(),
      age: 30,
    };

    let mut txn = db.write();
    txn.insert("Alice".to_string(), alice).unwrap();
    txn.insert("Bob".to_string(), bob).unwrap();

    {
      let alice = txn.get("Alice").unwrap().unwrap();
      assert_eq!(alice.value().age, 20);
      assert_eq!(alice.value().hobby, "swim");
    }

    {
      // output:
      // Alice:Person { hobby: "swim", age: 20 }
      // Bob:Person { hobby: "run", age: 30 }
      for ent in txn.iter().unwrap() {
        println!("{}:{:?}", ent.key(), ent.value());
      }

      // output:
      // Bob:Person { hobby: "run", age: 30 }
      // Alice:Person { hobby: "swim", age: 20 }
      for ent in txn.iter_rev().unwrap() {
        println!("{}:{:?}", ent.key(), ent.value());
      }

      // output:
      // Bob:Person { hobby: "run", age: 30 }
      for ent in txn.range("B".to_string()..).unwrap() {
        println!("{}:{:?}", ent.key(), ent.value());
      }

      // output:
      // Alice:Person { hobby: "swim", age: 20 }
      for ent in txn.range_rev(..="B".to_string()).unwrap() {
        println!("{}:{:?}", ent.key(), ent.value());
      }
    }

    txn.commit().unwrap();
  }

  {
    let txn = db.read();
    let alice = txn.get("Alice").unwrap();
    assert_eq!(alice.value().age, 20);
    assert_eq!(alice.value().hobby, "swim");

    let bob = txn.get("Bob").unwrap();
    assert_eq!(bob.value().age, 30);
    assert_eq!(bob.value().hobby, "run");

    // output:
    // Alice:Person { hobby: "swim", age: 20 }
    // Bob:Person { hobby: "run", age: 30 }
    for ent in txn.iter() {
      println!("{}:{:?}", ent.key(), ent.value());
    }

    // output:
    // Bob:Person { hobby: "run", age: 30 }
    // Alice:Person { hobby: "swim", age: 20 }
    for ent in txn.iter_rev() {
      println!("{}:{:?}", ent.key(), ent.value());
    }

    // output:
    // Bob:Person { hobby: "run", age: 30 }
    for ent in txn.range("B".to_string()..) {
      println!("{}:{:?}", ent.key(), ent.value());
    }

    // output:
    // Alice:Person { hobby: "swim", age: 20 }
    for ent in txn.range(..="B".to_string()) {
      println!("{}:{:?}", ent.key(), ent.value());
    }
  }
}

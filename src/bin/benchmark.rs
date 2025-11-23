use std::time::Instant;
use key_value_store::KvStore;

fn main() {
    let path = "benchmark.kv";
    // Clean up previous run
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path).unwrap();
    }

    let mut store = KvStore::open(path).expect("failed to open store");

    let start = Instant::now();
    let iterations = 100_000;
    
    for i in 0..iterations {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        store.put(&key, value.as_bytes()).expect("put failed");
    }

    let duration = start.elapsed();
    println!("Performed {} writes in {:?}", iterations, duration);
    println!("Writes per second: {:.2}", iterations as f64 / duration.as_secs_f64());

    // Verify a few keys
    let val = store.get("key-0").expect("get failed").expect("key not found");
    assert_eq!(val, b"value-0");
    
    let val = store.get("key-99999").expect("get failed").expect("key not found");
    assert_eq!(val, b"value-99999");

    println!("Verification successful!");
    
    drop(store);

    // Clean up
    std::fs::remove_file(path).unwrap();
}

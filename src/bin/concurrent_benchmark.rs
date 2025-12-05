use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Instant;
use key_value_store::KvStore;

fn main() {
    let path = "concurrent_benchmark.kv";
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path).unwrap();
    }

    // We use RwLock to allow multiple readers or one writer.
    // Since get() takes &self, we can take a read lock for reading.
    let store = Arc::new(RwLock::new(KvStore::open(path).expect("failed to open store")));

    // 1. Populate data (Single writer)
    {
        let mut w_guard = store.write().unwrap();
        for i in 0..10_000 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            w_guard.put(&key, value.as_bytes()).expect("put failed");
        }
    }
    println!("Populated 10,000 keys.");

    // 2. Concurrent Readers
    let start = Instant::now();
    let mut handles = vec![];
    let num_threads = 8;
    let reads_per_thread = 5000;

    for t_id in 0..num_threads {
        let store_clone = store.clone();
        handles.push(thread::spawn(move || {
            for i in 0..reads_per_thread {
                // Read random keys (modulo 10000)
                let key_id = (t_id * reads_per_thread + i) % 10000;
                let key = format!("key-{}", key_id);
                
                // Acquire READ lock
                let r_guard = store_clone.read().unwrap();
                let val = r_guard.get(&key).expect("get failed").expect("key not found");
                
                let expected = format!("value-{}", key_id);
                assert_eq!(val, expected.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let duration = start.elapsed();
    let total_reads = num_threads * reads_per_thread;
    println!("Performed {} reads in {:?} with {} threads", total_reads, duration, num_threads);
    println!("Reads per second: {:.2}", total_reads as f64 / duration.as_secs_f64());

    // Clean up
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path).unwrap();
    }
}

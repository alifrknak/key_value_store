

mod  kv_store;

fn main() -> std::io::Result<()>{

      let path = "kv_store.data";
    
    let mut store = kv_store::KvStore::open(path)?;

    // store.put("name", b"Furkan")?;
    // store.put("lang", b"Rust")?;
    // store.put("city", b"cargo")?;
 
        store.print_index();

        if let Some(value) = store.get("lang")? {
        println!("lang => {}", String::from_utf8_lossy(&value));
    }
    
    Ok(())
}

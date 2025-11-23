use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct KvStore {
    writer: BufWriter<File>,
    reader: File,
    index: HashMap<String, u64>, 
}

impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true) 
            .open(path)?;

        let reader = file.try_clone()?;
        let writer = BufWriter::new(file);

        let mut store = KvStore {
            writer,
            reader,
            index: HashMap::new(),
        };

        store.load_index()?;
        Ok(store)
    }

    pub fn put(&mut self, key: &str, value: &[u8]) -> io::Result<u64> {
        // Get current position from the writer (logical position)
        let offset = self.writer.stream_position()?;

        // header: [u32 key_len][u32 value_len] (little-endian)
        let klen = key.len() as u32;
        let vlen = value.len() as u32;

        self.writer.write_all(&klen.to_le_bytes())?;
        self.writer.write_all(&vlen.to_le_bytes())?;
        self.writer.write_all(key.as_bytes())?;
        self.writer.write_all(value)?;
        // No flush here!

        self.index.insert(key.to_string(), offset);

        Ok(offset)
    }

    /// Get value by key. Uses in-memory index to seek directly to the record.
    pub fn get(&mut self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let &offset = match self.index.get(key) {
            Some(off) => off,
            None => return Ok(None),
        };

        // Flush writer to ensure data is on disk before reading
        self.writer.flush()?;

        // Seek to the record header using the reader
        self.reader.seek(SeekFrom::Start(offset))?;

        // Read header
        let mut buf4 = [0u8; 4];
        self.reader.read_exact(&mut buf4)?;
        let klen = u32::from_le_bytes(buf4) as usize;

        self.reader.read_exact(&mut buf4)?;
        let vlen = u32::from_le_bytes(buf4) as usize;

        let mut key_buf = vec![0u8; klen];
        self.reader.read_exact(&mut key_buf)?;
        let key_read = String::from_utf8_lossy(&key_buf);

        if key_read != key {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("key at offset does not match index (expected `{}`, got `{}`)", key, key_read),
            ));
        }

        // Read value bytes
        let mut value_buf = vec![0u8; vlen];
        self.reader.read_exact(&mut value_buf)?;
        Ok(Some(value_buf))
    }

    fn load_index(&mut self) -> io::Result<()> {
        // Use a buffered reader on a clone of the reader handle
        let mut rdr = BufReader::new(self.reader.try_clone()?);
        let mut offset: u64 = 0;

        loop {
            // read header (8 bytes)
            let mut header = [0u8; 8];
            match rdr.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        // done scanning.
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }

            let klen = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let vlen = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

            // read key
            let mut key_buf = vec![0u8; klen];
            rdr.read_exact(&mut key_buf)?;

            let key = String::from_utf8_lossy(&key_buf).to_string();

            self.index.insert(key, offset);

            rdr.seek_relative(vlen as i64)?;

            offset += 8 + (klen as u64) + (vlen as u64);
        }

        Ok(())
    }

    pub fn print_index(&self) {
        println!("--- in-memory index (key -> offset) ---");
        for (k, &off) in &self.index {
            println!("{:<20} -> {}", k, off);
        }
        println!("---------------------------------------");
    }
}

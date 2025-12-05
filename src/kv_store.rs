use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::path::Path;
use std::sync::Mutex;

pub struct KvStore {
    writer: Mutex<BufWriter<File>>,
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
        let writer = Mutex::new(BufWriter::new(file));

        let mut store = KvStore {
            writer,
            reader,
            index: HashMap::new(),
        };

        store.load_index()?;
        Ok(store)
    }

    pub fn put(&mut self, key: &str, value: &[u8]) -> io::Result<u64> {
        let mut writer = self.writer.lock().unwrap();
        // Get current position from the writer (logical position)
        let offset = writer.stream_position()?;

        // header: [u32 key_len][u32 value_len] (little-endian)
        let klen = key.len() as u32;
        let vlen = value.len() as u32;

        writer.write_all(&klen.to_le_bytes())?;
        writer.write_all(&vlen.to_le_bytes())?;
        writer.write_all(key.as_bytes())?;
        writer.write_all(value)?;
        // No flush here!

        self.index.insert(key.to_string(), offset);

        Ok(offset)
    }

    /// Get value by key. Uses in-memory index to seek directly to the record.
    /// Takes &self to allow concurrent reads.
    pub fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let &offset = match self.index.get(key) {
            Some(off) => off,
            None => return Ok(None),
        };

        // Flush writer to ensure data is on disk before reading.
        // We lock specifically for this operation to ensure read-your-writes consistency.
        {
            let mut writer = self.writer.lock().unwrap();
            writer.flush()?;
        }

        // Read header without seeking (stateless read)
        let mut buf4 = [0u8; 4];
        let mut current_offset = offset;

        self.read_exact_at(&mut buf4, current_offset)?;
        let klen = u32::from_le_bytes(buf4) as usize;
        current_offset += 4;

        self.read_exact_at(&mut buf4, current_offset)?;
        let vlen = u32::from_le_bytes(buf4) as usize;
        current_offset += 4;

        let mut key_buf = vec![0u8; klen];
        self.read_exact_at(&mut key_buf, current_offset)?;
        let key_read = String::from_utf8_lossy(&key_buf);
        current_offset += klen as u64;

        if key_read != key {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("key at offset does not match index (expected `{}`, got `{}`)", key, key_read),
            ));
        }

        // Read value bytes
        let mut value_buf = vec![0u8; vlen];
        self.read_exact_at(&mut value_buf, current_offset)?;
        Ok(Some(value_buf))
    }

    // Helper to read exact bytes at an offset using FileExt
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.reader.seek_read(buf, offset) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"))
        } else {
            Ok(())
        }
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

/*
 * Chunk Store
 * I want this to store the version of a chunk, the binary data, and the checksum
 */

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

const SLOT_SIZE: usize = 8 * 1024 * 1024; // 8 MiB chunk sizes
const MAGIC: u32 = 0x_43_48_4B_01;
const CHUNK_HEADER_SIZE: usize = std::mem::size_of::<ChunkHeader>();
const SLOT_DATA_SIZE: usize = SLOT_SIZE - CHUNK_HEADER_SIZE;
const ALLOCATED_ARENA_SIZE: usize = 2048; // Number of chunks to allocate (default: 16 GiB at 8 MiB each)
pub const DEFAULT_FSYNC_EVERY_N_WRITES: u64 = 1; // Preserve current durability by default

#[repr(C)]
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkHeader {
    pub magic: u32,
    /// Slot id
    pub id: u64,
    pub checksum: u32,
    pub data_len: u32,
}

impl ChunkHeader {
    fn new(id: u64, checksum: u32, data_len: u32) -> Self {
        Self {
            magic: MAGIC,
            id,
            checksum,
            data_len,
        }
    }

    fn to_bytes(&self) -> [u8; CHUNK_HEADER_SIZE] {
        let mut bytes = [0u8; CHUNK_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&self.magic.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.data_len.to_le_bytes());
        bytes
    }

    fn from_bytes(buf: &[u8; CHUNK_HEADER_SIZE]) -> Self {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let id = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let checksum = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        Self {
            magic,
            id,
            checksum,
            data_len,
        }
    }
}

fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Write the full buffer starting at the given offset.
#[cfg(unix)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        let written = file.write_at(buf, offset)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write buffer",
            ));
        }
        offset += written as u64;
        buf = &buf[written..];
    }
    Ok(())
}

/// Read the full buffer starting at the given offset.
#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        let read = file.read_at(buf, offset)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill buffer",
            ));
        }
        offset += read as u64;
        let tmp = buf;
        buf = &mut tmp[read..];
    }
    Ok(())
}

/// Writes the ChunkHeader and bytes to disk using positional I/O.
fn write_chunk_at(
    file: &File,
    slot_index: u64,
    header: ChunkHeader,
    bytes: &[u8],
    fsync: bool,
) -> std::io::Result<()> {
    assert!(bytes.len() <= SLOT_DATA_SIZE);

    let header_bytes = header.to_bytes();
    let offset = slot_index * SLOT_SIZE as u64;

    #[cfg(unix)]
    {
        write_all_at(file, &header_bytes, offset)?;
        if !bytes.is_empty() {
            write_all_at(file, bytes, offset + CHUNK_HEADER_SIZE as u64)?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = offset;
        let _ = header_bytes;
        let _ = bytes;
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "positional I/O not supported on this platform",
        ));
    }

    if fsync {
        file.sync_data()?;
    }
    Ok(())
}

/// Reads only the ChunkHeader from a slot (for recovery/scanning).
fn read_chunk_header(file: &File, slot_index: u64) -> std::io::Result<ChunkHeader> {
    let mut header_bytes = [0u8; CHUNK_HEADER_SIZE];
    let offset = slot_index * SLOT_SIZE as u64;
    #[cfg(unix)]
    read_exact_at(file, &mut header_bytes, offset)?;
    #[cfg(not(unix))]
    {
        let _ = file;
        let _ = offset;
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "positional I/O not supported on this platform",
        ));
    }
    Ok(ChunkHeader::from_bytes(&header_bytes))
}

/// Reads a ChunkHeader and only the actual data bytes (not the full slot).
/// This is more efficient for small chunks as it avoids reading/allocating 8MB.
fn read_chunk_optimized(file: &File, slot_index: u64) -> std::io::Result<(ChunkHeader, Vec<u8>)> {
    // First read just the header to get data_len
    let header = read_chunk_header(file, slot_index)?;

    if header.magic != MAGIC {
        // Invalid/empty slot - return empty data
        return Ok((header, Vec::new()));
    }

    // Now read only the actual data bytes
    let data_len = header.data_len as usize;
    let mut data = vec![0u8; data_len];

    if data_len > 0 {
        let data_offset = slot_index * SLOT_SIZE as u64 + CHUNK_HEADER_SIZE as u64;
        #[cfg(unix)]
        read_exact_at(file, &mut data, data_offset)?;
        #[cfg(not(unix))]
        {
            let _ = file;
            let _ = data_offset;
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "positional I/O not supported on this platform",
            ));
        }
    }

    Ok((header, data))
}

#[derive(Debug, Clone)]
pub struct ChunkStoreOptions {
    pub fsync_every_n_writes: Option<u64>,
    pub fsync_on_delete: bool,
    pub fast_recover: bool,
}

impl Default for ChunkStoreOptions {
    fn default() -> Self {
        Self {
            fsync_every_n_writes: Some(DEFAULT_FSYNC_EVERY_N_WRITES),
            fsync_on_delete: true,
            fast_recover: false,
        }
    }
}

pub struct ChunkStore {
    file: Arc<File>,
    index: RwLock<HashMap<u64, u64>>,

    /// Stack of available slot indices. Pop to allocate, push to free.
    free_slots: Mutex<Vec<u64>>,
    total_slots: u64,

    fsync_every_n_writes: Option<u64>,
    fsync_on_delete: bool,
    write_counter: AtomicU64,
}

impl ChunkStore {
    /// Opens the file backing the chunk store.
    /// `allocated_slots` sets the number of chunks to store, defaulting to a set value when `None`
    /// is provided.
    pub fn open(
        path: &str,
        allocated_slots: Option<usize>,
        options: ChunkStoreOptions,
    ) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(path)?;
        let allocated_arena_size = allocated_slots.unwrap_or(ALLOCATED_ARENA_SIZE);
        let file_bytes = SLOT_SIZE * allocated_arena_size;
        file.set_len(file_bytes as u64)?;
        let free_slots: Vec<u64> = (0..allocated_arena_size as u64).collect();
        Ok(Self {
            file: Arc::new(file),
            index: RwLock::new(HashMap::new()),
            free_slots: Mutex::new(free_slots),
            total_slots: allocated_arena_size as u64,
            fsync_every_n_writes: options.fsync_every_n_writes,
            fsync_on_delete: options.fsync_on_delete,
            write_counter: AtomicU64::new(0),
        })
    }

    /// Find a free slot and 'allocate' it.
    /// Pops from the `free_slots` stack in O(1) time.
    #[tracing::instrument(skip(self), fields(slot_index, free_slots_remaining, error))]
    fn find_slot(&self) -> Option<u64> {
        let span = tracing::Span::current();
        let mut free_slots = self.free_slots.lock().unwrap();
        match free_slots.pop() {
            Some(slot_index) => {
                span.record("slot_index", slot_index);
                span.record("free_slots_remaining", free_slots.len() as u64);
                Some(slot_index)
            }
            None => {
                span.record("error", "no free slots available");
                None
            }
        }
    }

    /// 'deallocates' a slot by pushing it back onto the `free_slots` stack.
    fn clear_slot(&self, slot_index: u64) -> std::io::Result<()> {
        assert!(slot_index < self.total_slots);
        let mut free_slots = self.free_slots.lock().unwrap();
        free_slots.push(slot_index);
        Ok(())
    }

    /// Inserts the chunk, mapping the chunk `id` to the slot index.
    /// If all allocated slots are used, returns a `StorageFull` error.
    /// If the `id` is already in use, returns an `AlreadyExists` error.
    #[tracing::instrument(skip(self, data), fields(chunk_id = id, data_len = data.len(), error, total_slots, used_slots, checksum, slot_index))]
    pub fn put_chunk(&self, id: u64, data: &[u8]) -> std::io::Result<()> {
        let span = tracing::Span::current();
        let mut index = self.index.write().unwrap();
        if index.contains_key(&id) {
            span.record("error", "chunk id already exists in index");
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "id is already used",
            ));
        }

        let slot_idx = self.find_slot();
        if slot_idx.is_none() {
            span.record("error", "all chunks allocated");
            let free = self.free_slots.lock().unwrap();
            span.record("total_slots", free.len() + index.len());
            span.record("used_slots", index.len());
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "all chunks allocated",
            ));
        }
        let slot_idx = slot_idx.unwrap();

        let checksum = compute_checksum(data);
        span.record("checksum", checksum);
        span.record("slot_index", slot_idx);

        let header = ChunkHeader::new(id, checksum, data.len() as u32);
        let fsync = self.should_fsync_write();

        let result = write_chunk_at(&self.file, slot_idx, header, data, fsync);

        if let Err(err) = result {
            let _ = self.clear_slot(slot_idx);
            return Err(err);
        }
        index.insert(id, slot_idx);
        Ok(())
    }

    /// Returns the chunk data for the chunk `id`.
    /// Uses optimized read that only fetches the actual data bytes, not the full slot.
    #[tracing::instrument(skip(self), fields(chunk_id = id, error, slot_index, stored_checksum, computed_checksum, data_len))]
    pub fn get_chunk(&self, id: u64) -> std::io::Result<(Vec<u8>, u32)> {
        let span = tracing::Span::current();
        let index = self.index.read().unwrap();
        let &slot_idx = index.get(&id).ok_or_else(|| {
            span.record("error", "chunk not found in index");
            std::io::Error::new(std::io::ErrorKind::NotFound, "Chunk not found")
        })?;

        span.record("slot_index", slot_idx);
        // Use optimized read that only reads header + actual data bytes
        let (header, data) = read_chunk_optimized(&self.file, slot_idx)?;
        if header.magic != MAGIC {
            span.record("error", "invalid magic in chunk header (corrupt data)");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "invalid magic in chunk header for slot {}: expected {:#X}, got {:#X}",
                    slot_idx, MAGIC, header.magic
                ),
            ));
        }
        let computed_checksum = compute_checksum(&data);
        span.record("stored_checksum", header.checksum);
        span.record("computed_checksum", computed_checksum);
        span.record("data_len", header.data_len);
        if header.checksum != computed_checksum {
            span.record("error", "checksum mismatch (corrupt data)");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "checksum mismatch for chunk {}: stored {:#X}, computed {:#X}",
                    id, header.checksum, computed_checksum
                ),
            ));
        }
        Ok((data, header.checksum))
    }

    /// Deletes a chunk by removing it from the `index` map, zeroing the slot header on disk,
    /// and marking the slot as 'empty'.
    #[tracing::instrument(skip(self), fields(chunk_id = id, slot_index, warning))]
    pub fn delete_chunk(&self, id: u64) -> std::io::Result<()> {
        let span = tracing::Span::current();
        let mut index = self.index.write().unwrap();
        if let Some(slot_idx) = index.remove(&id) {
            span.record("slot_index", slot_idx);
            // Zero the header on disk so recovery won't find this chunk
            let zero_header = [0u8; CHUNK_HEADER_SIZE];
            let offset = slot_idx * SLOT_SIZE as u64;
            #[cfg(unix)]
            write_all_at(&self.file, &zero_header, offset)?;
            if self.fsync_on_delete {
                self.file.sync_data()?;
            }
            self.clear_slot(slot_idx)?;
        } else {
            span.record("warning", "chunk id not found in index");
        }
        Ok(())
    }

    /// Reads through the slots stored on disk, and constructs the mapping.
    /// Uses optimized header-only reads to avoid loading full 8MB slots.
    #[tracing::instrument(skip(self), fields(total_slots, recovered_chunks, recovered_corrupted))]
    pub fn recover_index(&self, fast_recover: bool) -> std::io::Result<()> {
        let span = tracing::Span::current();
        let file_len = self.file.metadata()?.len();
        let slot_count = file_len / SLOT_SIZE as u64;
        span.record("total_slots", slot_count);

        let mut index = self.index.write().unwrap();
        index.clear();
        let mut free_slots = self.free_slots.lock().unwrap();
        free_slots.clear();
        let mut recovered_count = 0u64;
        let mut corrupted_count = 0u64;

        for i in 0..slot_count {
            let header = read_chunk_header(&self.file, i)?;
            if header.magic == MAGIC {
                if header.data_len as usize > SLOT_DATA_SIZE {
                    tracing::warn!(
                        slot_index = i,
                        chunk_id = header.id,
                        data_len = header.data_len,
                        "Invalid data_len during recovery, treating slot as free"
                    );
                    corrupted_count += 1;
                    free_slots.push(i);
                    continue;
                }

                if fast_recover {
                    index.insert(header.id, i);
                    recovered_count += 1;
                    continue;
                }

                // Verify data integrity by reading chunk data and checking CRC32
                let (_, data) = read_chunk_optimized(&self.file, i)?;
                let computed_checksum = compute_checksum(&data);
                if header.checksum != computed_checksum {
                    tracing::warn!(
                        slot_index = i,
                        chunk_id = header.id,
                        stored_checksum = header.checksum,
                        computed_checksum = computed_checksum,
                        "Checksum mismatch during recovery, treating slot as free"
                    );
                    corrupted_count += 1;
                    free_slots.push(i);
                } else {
                    index.insert(header.id, i);
                    recovered_count += 1;
                }
            } else {
                free_slots.push(i);
            }
        }
        span.record("recovered_chunks", recovered_count);
        span.record("recovered_corrupted", corrupted_count);
        Ok(())
    }

    pub fn free_slots(&self) -> u64 {
        self.free_slots.lock().unwrap().len() as u64
    }

    pub fn get_chunk_size(&self) -> u64 {
        SLOT_DATA_SIZE as u64
    }

    fn should_fsync_write(&self) -> bool {
        match self.fsync_every_n_writes {
            Some(n) if n > 0 => {
                let count = self.write_counter.fetch_add(1, Ordering::Relaxed) + 1;
                count % n == 0
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test file path with cleanup on drop
    struct TestFile {
        path: String,
    }

    impl TestFile {
        fn new(name: &str) -> Self {
            Self {
                path: format!("test_{}.dat", name),
            }
        }

        fn path(&self) -> &str {
            &self.path
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn default_options() -> ChunkStoreOptions {
        ChunkStoreOptions::default()
    }

    // ===================
    // ChunkHeader Tests
    // ===================

    #[test]
    fn test_chunk_header_serialization_roundtrip() {
        let header = ChunkHeader::new(12345, 0xDEADBEEF, 1024);

        let bytes = header.to_bytes();
        let restored = ChunkHeader::from_bytes(&bytes);

        assert_eq!(restored.magic, MAGIC);
        assert_eq!(restored.id, 12345);
        assert_eq!(restored.checksum, 0xDEADBEEF);
        assert_eq!(restored.data_len, 1024);
    }

    #[test]
    fn test_chunk_header_magic_is_correct() {
        let header = ChunkHeader::new(0, 0, 0);
        assert_eq!(header.magic, 0x43484B01); // "CHK\x01" in little endian
    }

    #[test]
    fn test_chunk_header_size_includes_alignment_padding() {
        // ChunkHeader is 24 bytes due to alignment padding:
        // magic (4) + padding (4) + id (8) + checksum (4) + data_len (4) = 24
        // Note: to_bytes/from_bytes only use the first 20 bytes of actual data
        assert_eq!(CHUNK_HEADER_SIZE, 24);
        let header = ChunkHeader::new(0, 0, 0);
        assert_eq!(header.to_bytes().len(), 24);
    }

    // ===================
    // Checksum Tests
    // ===================

    #[test]
    fn test_compute_checksum_deterministic() {
        let data = b"hello world";
        let checksum1 = compute_checksum(data);
        let checksum2 = compute_checksum(data);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_compute_checksum_different_data_different_checksum() {
        let checksum1 = compute_checksum(b"hello");
        let checksum2 = compute_checksum(b"world");
        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_compute_checksum_empty_data() {
        let checksum = compute_checksum(b"");
        // CRC32 of empty data is 0
        assert_eq!(checksum, 0);
    }

    // ===================
    // Low-Level I/O Tests
    // ===================

    #[test]
    fn test_write_and_read_chunk_with_buffer() {
        let test_file = TestFile::new("write_read_buffer");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(test_file.path())
            .unwrap();
        file.set_len(SLOT_SIZE as u64).unwrap();

        let data = b"test data for write/read";
        let checksum = compute_checksum(data);
        let header = ChunkHeader::new(42, checksum, data.len() as u32);

        write_chunk_at(&file, 0, header.clone(), data, false).unwrap();

        let (read_header, read_data) = read_chunk_optimized(&file, 0).unwrap();
        assert_eq!(read_header, header);
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_read_chunk_header_only() {
        let test_file = TestFile::new("header_only");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(test_file.path())
            .unwrap();
        file.set_len(SLOT_SIZE as u64).unwrap();

        let data = b"some data";
        let checksum = compute_checksum(data);
        let header = ChunkHeader::new(99, checksum, data.len() as u32);

        write_chunk_at(&file, 0, header.clone(), data, false).unwrap();

        // Read only header
        let read_header = read_chunk_header(&file, 0).unwrap();
        assert_eq!(read_header.magic, MAGIC);
        assert_eq!(read_header.id, 99);
        assert_eq!(read_header.checksum, checksum);
        assert_eq!(read_header.data_len, data.len() as u32);
    }

    #[test]
    fn test_read_chunk_optimized_returns_exact_data_size() {
        let test_file = TestFile::new("optimized_exact_size");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(test_file.path())
            .unwrap();
        file.set_len(SLOT_SIZE as u64).unwrap();

        // 100 bytes of data in an 8MB slot
        let data = vec![0xAB; 100];
        let checksum = compute_checksum(&data);
        let header = ChunkHeader::new(1, checksum, data.len() as u32);

        write_chunk_at(&file, 0, header, &data, false).unwrap();

        let (_, read_data) = read_chunk_optimized(&file, 0).unwrap();
        // Should return exactly 100 bytes, not 8MB
        assert_eq!(read_data.len(), 100);
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_read_empty_slot_returns_empty_data() {
        let test_file = TestFile::new("empty_slot");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(test_file.path())
            .unwrap();
        file.set_len(SLOT_SIZE as u64).unwrap();

        // Don't write anything, just read
        let (header, data) = read_chunk_optimized(&file, 0).unwrap();
        // Magic will be 0, indicating empty slot
        assert_ne!(header.magic, MAGIC);
        assert!(data.is_empty());
    }

    #[test]
    fn test_write_to_different_slots() {
        let test_file = TestFile::new("different_slots");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(test_file.path())
            .unwrap();
        file.set_len((SLOT_SIZE * 3) as u64).unwrap();

        let data1 = b"slot zero data";
        let data2 = b"slot one data";
        let data3 = b"slot two data";

        // Write to slot 0, 1, 2
        write_chunk_at(
            &file,
            0,
            ChunkHeader::new(100, compute_checksum(data1), data1.len() as u32),
            data1,
            false,
        )
        .unwrap();
        write_chunk_at(
            &file,
            1,
            ChunkHeader::new(101, compute_checksum(data2), data2.len() as u32),
            data2,
            false,
        )
        .unwrap();
        write_chunk_at(
            &file,
            2,
            ChunkHeader::new(102, compute_checksum(data3), data3.len() as u32),
            data3,
            false,
        )
        .unwrap();

        // Read back and verify
        let (h0, d0) = read_chunk_optimized(&file, 0).unwrap();
        let (h1, d1) = read_chunk_optimized(&file, 1).unwrap();
        let (h2, d2) = read_chunk_optimized(&file, 2).unwrap();

        assert_eq!(h0.id, 100);
        assert_eq!(d0, data1);
        assert_eq!(h1.id, 101);
        assert_eq!(d1, data2);
        assert_eq!(h2.id, 102);
        assert_eq!(d2, data3);
    }

    // ===================
    // ChunkStore Tests
    // ===================

    #[test]
    fn test_chunkstore_open_creates_file() {
        let test_file = TestFile::new("open_creates");
        let _store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();
        assert!(std::path::Path::new(test_file.path()).exists());
    }

    #[test]
    fn test_chunkstore_preallocates_slots() {
        let test_file = TestFile::new("preallocate");
        let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
        assert_eq!(store.free_slots(), 4);

        let metadata = std::fs::metadata(test_file.path()).unwrap();
        assert_eq!(metadata.len(), (SLOT_SIZE * 4) as u64);
    }

    #[test]
    fn test_chunkstore_get_chunk_size() {
        let test_file = TestFile::new("chunk_size");
        let store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();
        assert_eq!(store.get_chunk_size(), SLOT_DATA_SIZE as u64);
    }

    #[test]
    fn test_chunkstore_put_and_get_chunk() {
        let test_file = TestFile::new("put_get");
        let store = ChunkStore::open(test_file.path(), Some(2), default_options()).unwrap();

        let data = b"hello world";
        store.put_chunk(42, data).unwrap();

        let (retrieved, checksum) = store.get_chunk(42).unwrap();
        assert_eq!(retrieved, data);
        assert_eq!(checksum, compute_checksum(data));
    }

    #[test]
    fn test_chunkstore_put_chunk_decrements_free_slots() {
        let test_file = TestFile::new("decrement_free");
        let store = ChunkStore::open(test_file.path(), Some(3), default_options()).unwrap();
        assert_eq!(store.free_slots(), 3);

        store.put_chunk(1, b"data").unwrap();
        assert_eq!(store.free_slots(), 2);

        store.put_chunk(2, b"more").unwrap();
        assert_eq!(store.free_slots(), 1);
    }

    #[test]
    fn test_chunkstore_put_chunk_duplicate_id_fails() {
        let test_file = TestFile::new("duplicate_id");
        let store = ChunkStore::open(test_file.path(), Some(2), default_options()).unwrap();

        store.put_chunk(100, b"first").unwrap();
        let result = store.put_chunk(100, b"second");

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );

        // Original data should be preserved
        let (data, _) = store.get_chunk(100).unwrap();
        assert_eq!(data, b"first");
    }

    #[test]
    fn test_chunkstore_put_chunk_storage_full() {
        let test_file = TestFile::new("storage_full");
        let store = ChunkStore::open(test_file.path(), Some(2), default_options()).unwrap();

        store.put_chunk(1, b"first").unwrap();
        store.put_chunk(2, b"second").unwrap();

        let result = store.put_chunk(3, b"third");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::StorageFull);
    }

    #[test]
    fn test_chunkstore_get_chunk_not_found() {
        let test_file = TestFile::new("not_found");
        let store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();

        let result = store.get_chunk(99999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_chunkstore_delete_chunk() {
        let test_file = TestFile::new("delete");
        let store = ChunkStore::open(test_file.path(), Some(2), default_options()).unwrap();

        store.put_chunk(42, b"data to delete").unwrap();
        assert_eq!(store.free_slots(), 1);

        store.delete_chunk(42).unwrap();
        assert_eq!(store.free_slots(), 2);

        // Chunk should no longer be retrievable
        assert!(store.get_chunk(42).is_err());
    }

    #[test]
    fn test_chunkstore_delete_chunk_not_found_is_ok() {
        let test_file = TestFile::new("delete_not_found");
        let store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();

        // Deleting non-existent chunk should not error (idempotent)
        let result = store.delete_chunk(99999);
        assert!(result.is_ok());
    }

    #[test]
    fn test_chunkstore_delete_frees_slot_for_reuse() {
        let test_file = TestFile::new("delete_reuse");
        let store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();

        store.put_chunk(1, b"first").unwrap();
        assert!(store.put_chunk(2, b"second").is_err()); // Full

        store.delete_chunk(1).unwrap();

        // Now we should be able to write again
        store.put_chunk(2, b"second").unwrap();
        let (data, _) = store.get_chunk(2).unwrap();
        assert_eq!(data, b"second");
    }

    // ===================
    // Index Recovery Tests
    // ===================

    #[test]
    fn test_chunkstore_recover_index_after_writes() {
        let test_file = TestFile::new("recover_after_writes");

        // Scope to drop the store
        {
            let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
            store.put_chunk(100, b"chunk one").unwrap();
            store.put_chunk(200, b"chunk two").unwrap();
            store.put_chunk(300, b"chunk three").unwrap();
        }

        // Open again and recover
        let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
        // Index is empty after open
        assert!(store.index.read().unwrap().is_empty());

        store.recover_index(false).unwrap();

        // Index should be rebuilt
        assert_eq!(store.index.read().unwrap().len(), 3);

        // Verify we can read the chunks
        let (d1, _) = store.get_chunk(100).unwrap();
        let (d2, _) = store.get_chunk(200).unwrap();
        let (d3, _) = store.get_chunk(300).unwrap();

        assert_eq!(d1, b"chunk one");
        assert_eq!(d2, b"chunk two");
        assert_eq!(d3, b"chunk three");
    }

    #[test]
    fn test_chunkstore_recover_index_updates_free_slots() {
        let test_file = TestFile::new("recover_free_slots");

        {
            let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
            store.put_chunk(1, b"a").unwrap();
            store.put_chunk(2, b"b").unwrap();
        }

        let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
        store.recover_index(false).unwrap();

        // 4 slots, 2 used = 2 free
        assert_eq!(store.free_slots(), 2);
    }

    #[test]
    fn test_chunkstore_recover_index_empty_file() {
        let test_file = TestFile::new("recover_empty");

        let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
        store.recover_index(false).unwrap();

        assert!(store.index.read().unwrap().is_empty());
        assert_eq!(store.free_slots(), 4);
    }

    #[test]
    fn test_chunkstore_recover_index_with_deleted_chunks() {
        let test_file = TestFile::new("recover_with_deleted");

        {
            let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
            store.put_chunk(1, b"keep").unwrap();
            store.put_chunk(2, b"delete me").unwrap();
            store.put_chunk(3, b"also keep").unwrap();
            store.delete_chunk(2).unwrap();
        }

        let store = ChunkStore::open(test_file.path(), Some(4), default_options()).unwrap();
        store.recover_index(false).unwrap();

        // delete_chunk zeroes the header on disk, so recovery should only find 2 chunks
        assert_eq!(store.index.read().unwrap().len(), 2);
    }

    // ===================
    // Empty and Edge Case Data Tests
    // ===================

    #[test]
    fn test_chunkstore_put_empty_data() {
        let test_file = TestFile::new("empty_data");
        let store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();

        store.put_chunk(1, b"").unwrap();

        let (data, checksum) = store.get_chunk(1).unwrap();
        assert!(data.is_empty());
        assert_eq!(checksum, 0); // CRC32 of empty is 0
    }

    #[test]
    fn test_chunkstore_put_max_size_data() {
        let test_file = TestFile::new("max_size");
        let store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();

        // Fill entire slot (minus header)
        let data = vec![0x42; SLOT_DATA_SIZE];
        store.put_chunk(1, &data).unwrap();

        let (retrieved, _) = store.get_chunk(1).unwrap();
        assert_eq!(retrieved.len(), SLOT_DATA_SIZE);
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_chunkstore_multiple_operations_sequence() {
        let test_file = TestFile::new("operation_sequence");
        let store = ChunkStore::open(test_file.path(), Some(3), default_options()).unwrap();

        // Put, get, delete, put again sequence
        store.put_chunk(1, b"first").unwrap();
        let (d, _) = store.get_chunk(1).unwrap();
        assert_eq!(d, b"first");

        store.delete_chunk(1).unwrap();
        assert!(store.get_chunk(1).is_err());

        store.put_chunk(2, b"second").unwrap();
        store.put_chunk(3, b"third").unwrap();

        let (d2, _) = store.get_chunk(2).unwrap();
        let (d3, _) = store.get_chunk(3).unwrap();
        assert_eq!(d2, b"second");
        assert_eq!(d3, b"third");
    }

    // ===================
    // Slot Allocation Tests
    // ===================

    #[test]
    fn test_find_slot_returns_unique_indices() {
        let test_file = TestFile::new("unique_indices");
        let store = ChunkStore::open(test_file.path(), Some(3), default_options()).unwrap();

        let idx1 = store.find_slot().unwrap();
        let idx2 = store.find_slot().unwrap();
        let idx3 = store.find_slot().unwrap();

        // All should be different
        assert_ne!(idx1, idx2);
        assert_ne!(idx2, idx3);
        assert_ne!(idx1, idx3);

        // Should be in range
        assert!(idx1 < 3);
        assert!(idx2 < 3);
        assert!(idx3 < 3);
    }

    #[test]
    fn test_find_slot_returns_none_when_full() {
        let test_file = TestFile::new("slot_full");
        let store = ChunkStore::open(test_file.path(), Some(2), default_options()).unwrap();

        store.find_slot().unwrap();
        store.find_slot().unwrap();

        assert!(store.find_slot().is_none());
    }

    #[test]
    fn test_clear_slot_makes_slot_available() {
        let test_file = TestFile::new("clear_slot");
        let store = ChunkStore::open(test_file.path(), Some(1), default_options()).unwrap();

        let idx = store.find_slot().unwrap();
        assert!(store.find_slot().is_none()); // Full

        store.clear_slot(idx).unwrap();
        let idx2 = store.find_slot().unwrap();
        assert_eq!(idx, idx2); // Same slot reused
    }
}

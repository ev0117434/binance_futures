use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 8] = b"QSHM1\0\0\0";
const HEADER_SIZE: u64 = 4096;
const RECORD_SIZE: u64 = 64;
const RECORDS_OFFSET: u64 = 4096;
const PRICE_SCALE: u64 = 100_000_000; // 1e8
const TS_SCALE: u64 = 1_000_000; // 1e6 (microseconds)

#[repr(C)]
pub struct Quote64 {
    pub seq: AtomicU64,
    pub source_id: u64,
    pub symbol_id: u64,
    pub bid: i64,
    pub ask: i64,
    pub ts: i64,
    pub reserved0: u64,
    pub reserved1: u64,
}

const _: () = assert!(std::mem::size_of::<Quote64>() == 64);

pub struct ShmWriter {
    _mmap: MmapMut,
    base_ptr: *mut u8,
    records_offset: u64,
    n_symbols: u64,
    n_sources: u64,
}

unsafe impl Send for ShmWriter {}
unsafe impl Sync for ShmWriter {}

impl ShmWriter {
    pub fn open(path: &str) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("Failed to open SHM file: {}", e))?;

        let metadata = file.metadata()
            .map_err(|e| format!("Failed to get file metadata: {}", e))?;
        let file_size = metadata.len();

        let mut mmap = unsafe {
            MmapMut::map_mut(&file)
                .map_err(|e| format!("Failed to mmap: {}", e))?
        };

        let base_ptr = mmap.as_mut_ptr();

        // Validate header
        unsafe {
            let magic = std::slice::from_raw_parts(base_ptr, 8);
            if magic != MAGIC {
                return Err(format!("Invalid magic: expected {:?}, got {:?}", MAGIC, magic));
            }

            let header_size = read_u64(base_ptr, 16);
            if header_size != HEADER_SIZE {
                return Err(format!("Invalid header_size: expected {}, got {}", HEADER_SIZE, header_size));
            }

            let record_size = read_u64(base_ptr, 24);
            if record_size != RECORD_SIZE {
                return Err(format!("Invalid record_size: expected {}, got {}", RECORD_SIZE, record_size));
            }

            let records_offset = read_u64(base_ptr, 32);
            if records_offset != RECORDS_OFFSET {
                return Err(format!("Invalid records_offset: expected {}, got {}", RECORDS_OFFSET, records_offset));
            }

            let price_scale = read_u64(base_ptr, 40);
            if price_scale != PRICE_SCALE {
                return Err(format!("Invalid price_scale: expected {}, got {}", PRICE_SCALE, price_scale));
            }

            let ts_scale = read_u64(base_ptr, 48);
            if ts_scale != TS_SCALE {
                return Err(format!("Invalid ts_scale: expected {}, got {}", TS_SCALE, ts_scale));
            }

            let n_sources = read_u64(base_ptr, 56);
            let n_symbols = read_u64(base_ptr, 64);
            let n_records = read_u64(base_ptr, 72);
            let shm_total_size = read_u64(base_ptr, 80);

            if file_size != shm_total_size {
                return Err(format!("File size mismatch: expected {}, got {}", shm_total_size, file_size));
            }

            if n_records != n_sources * n_symbols {
                return Err(format!("Invalid n_records: expected {}, got {}", n_sources * n_symbols, n_records));
            }

            Ok(ShmWriter {
                _mmap: mmap,
                base_ptr,
                records_offset,
                n_symbols,
                n_sources,
            })
        }
    }

    pub fn get_slot(&self, source_id: u64, symbol_id: u64) -> Result<&Quote64, String> {
        if source_id >= self.n_sources {
            return Err(format!("source_id {} out of range (max {})", source_id, self.n_sources));
        }
        if symbol_id >= self.n_symbols {
            return Err(format!("symbol_id {} out of range (max {})", symbol_id, self.n_symbols));
        }

        let idx = source_id * self.n_symbols + symbol_id;
        let offset = self.records_offset + idx * RECORD_SIZE;

        unsafe {
            let ptr = self.base_ptr.add(offset as usize) as *const Quote64;
            Ok(&*ptr)
        }
    }

    pub fn write_quote(
        &self,
        source_id: u64,
        symbol_id: u64,
        bid: i64,
        ask: i64,
        ts_us: i64,
    ) -> Result<(), String> {
        let slot = self.get_slot(source_id, symbol_id)?;

        let seq0 = slot.seq.load(Ordering::Relaxed);
        slot.seq.store(seq0.wrapping_add(1), Ordering::Release);

        unsafe {
            let ptr = slot as *const Quote64 as *mut Quote64;
            (*ptr).source_id = source_id;
            (*ptr).symbol_id = symbol_id;
            (*ptr).bid = bid;
            (*ptr).ask = ask;
            (*ptr).ts = ts_us;
            (*ptr).reserved0 = 0;
            (*ptr).reserved1 = 0;
        }

        slot.seq.store(seq0.wrapping_add(2), Ordering::Release);

        Ok(())
    }
}

unsafe fn read_u64(ptr: *const u8, offset: usize) -> u64 {
    let slice = std::slice::from_raw_parts(ptr.add(offset), 8);
    u64::from_le_bytes(slice.try_into().unwrap())
}

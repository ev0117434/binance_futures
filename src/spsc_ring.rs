use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};

const SPSC_MAGIC: u32 = 0x53505343; // "SPSC"
const SPSC_ABI_VERSION: u16 = 1;
const MSG_SIZE: u16 = 256;
const HEADER_SIZE: usize = 128;

/// Message types for SPSC ring buffer
#[repr(u16)]
#[derive(Debug, Clone, Copy)]
pub enum MsgType {
    Info = 1,
    Warning = 2,
    Error = 3,
}

/// Message header (48 bytes)
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy)]
pub struct MsgHeader {
    pub abi_version: u16,
    pub msg_type: u16,
    pub msg_size: u16,
    pub reserved: u16,
    pub timestamp_us: i64,
    pub seq: u64,
    pub payload_len: u32,
    pub reserved2: u32,
    pub reserved3: [u64; 2], // padding to 48 bytes
}

const _: () = assert!(std::mem::size_of::<MsgHeader>() == 48);

/// Fixed-size message (256 bytes)
#[repr(C, align(8))]
pub struct SpscMsg256 {
    pub header: MsgHeader,
    pub body: [u8; 256 - 48], // 208 bytes payload
}

const _: () = assert!(std::mem::size_of::<SpscMsg256>() == 256);

/// SPSC Ring buffer header in shared memory (128 bytes)
#[repr(C, align(8))]
struct RingHeader {
    magic: u32,
    abi_version: u16,
    msg_size: u16,
    ring_slots: u64,
    data_offset: u64,
    write_seq: AtomicU64,
    read_seq: AtomicU64,
    reserved: [u64; 11], // padding to 128 bytes
}

const _: () = assert!(std::mem::size_of::<RingHeader>() == 128);

/// SPSC Ring buffer writer
pub struct SpscWriter {
    _mmap: MmapMut,
    data_ptr: *mut u8,
    ring_mask: u64,
    write_seq_ptr: *const AtomicU64,
}

unsafe impl Send for SpscWriter {}
unsafe impl Sync for SpscWriter {}

impl SpscWriter {
    /// Open or create SPSC ring buffer in shared memory
    pub fn open(path: &str, ring_slots: u64) -> Result<Self, String> {
        // Ensure ring_slots is power of 2
        if !ring_slots.is_power_of_two() {
            return Err(format!("ring_slots must be power of 2, got {}", ring_slots));
        }

        let total_size = HEADER_SIZE + (ring_slots as usize * MSG_SIZE as usize);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| format!("Failed to open SPSC ring: {}", e))?;

        // Set file size
        file.set_len(total_size as u64)
            .map_err(|e| format!("Failed to set file size: {}", e))?;

        let mut mmap = unsafe {
            MmapMut::map_mut(&file)
                .map_err(|e| format!("Failed to mmap: {}", e))?
        };

        let base_ptr = mmap.as_mut_ptr();

        unsafe {
            // Check if already initialized
            let magic_ptr = base_ptr as *const u32;
            let magic = std::ptr::read_volatile(magic_ptr);

            if magic == SPSC_MAGIC {
                // Already initialized, validate
                Self::validate_header(base_ptr, ring_slots)?;
            } else {
                // Initialize header
                Self::initialize_header(base_ptr, ring_slots);
            }

            let header_ptr = base_ptr as *const RingHeader;
            let write_seq_ptr = &(*header_ptr).write_seq as *const AtomicU64;
            let data_ptr = base_ptr.add(HEADER_SIZE);
            let ring_mask = ring_slots - 1;

            Ok(SpscWriter {
                _mmap: mmap,
                data_ptr,
                ring_mask,
                write_seq_ptr,
            })
        }
    }

    /// Initialize ring buffer header
    unsafe fn initialize_header(base_ptr: *mut u8, ring_slots: u64) {
        let header_ptr = base_ptr as *mut RingHeader;

        // Write magic
        std::ptr::write_volatile(&mut (*header_ptr).magic, SPSC_MAGIC);

        // Write ABI version
        std::ptr::write_volatile(&mut (*header_ptr).abi_version, SPSC_ABI_VERSION);

        // Write message size
        std::ptr::write_volatile(&mut (*header_ptr).msg_size, MSG_SIZE);

        // Write ring slots
        std::ptr::write_volatile(&mut (*header_ptr).ring_slots, ring_slots);

        // Write data offset
        std::ptr::write_volatile(&mut (*header_ptr).data_offset, HEADER_SIZE as u64);

        // Initialize sequences to 0
        (*header_ptr).write_seq.store(0, Ordering::Release);
        (*header_ptr).read_seq.store(0, Ordering::Release);

        // Zero reserved fields
        std::ptr::write_bytes(&mut (*header_ptr).reserved as *mut _, 0, 11);
    }

    /// Validate existing ring buffer header
    unsafe fn validate_header(base_ptr: *const u8, expected_slots: u64) -> Result<(), String> {
        let header_ptr = base_ptr as *const RingHeader;

        let magic = std::ptr::read_volatile(&(*header_ptr).magic);
        if magic != SPSC_MAGIC {
            return Err(format!("Invalid magic: expected 0x{:08X}, got 0x{:08X}", SPSC_MAGIC, magic));
        }

        let abi_version = std::ptr::read_volatile(&(*header_ptr).abi_version);
        if abi_version != SPSC_ABI_VERSION {
            return Err(format!("Invalid ABI version: expected {}, got {}", SPSC_ABI_VERSION, abi_version));
        }

        let msg_size = std::ptr::read_volatile(&(*header_ptr).msg_size);
        if msg_size != MSG_SIZE {
            return Err(format!("Invalid msg_size: expected {}, got {}", MSG_SIZE, msg_size));
        }

        let ring_slots = std::ptr::read_volatile(&(*header_ptr).ring_slots);
        if ring_slots != expected_slots {
            return Err(format!("Invalid ring_slots: expected {}, got {}", expected_slots, ring_slots));
        }

        if !ring_slots.is_power_of_two() {
            return Err(format!("ring_slots is not power of 2: {}", ring_slots));
        }

        Ok(())
    }

    /// Publish a message to the ring buffer with correct memory ordering
    pub fn publish(&self, msg: &SpscMsg256) -> Result<(), String> {
        unsafe {
            // Load current write sequence (Relaxed is OK here)
            let seq = (*self.write_seq_ptr).load(Ordering::Relaxed);

            // Calculate slot index
            let idx = seq & self.ring_mask;
            let slot_ptr = self.data_ptr.add((idx as usize) * 256);

            // CRITICAL: Copy data BEFORE incrementing sequence
            // This ensures no partial writes are visible
            std::ptr::copy_nonoverlapping(
                msg as *const _ as *const u8,
                slot_ptr,
                256,
            );

            // CRITICAL: Release store to publish the message
            // This ensures all previous writes are visible before sequence increment
            (*self.write_seq_ptr).store(seq + 1, Ordering::Release);

            Ok(())
        }
    }

    /// Create a message from log data
    pub fn create_message(
        msg_type: MsgType,
        timestamp_us: i64,
        seq: u64,
        payload: &[u8],
    ) -> SpscMsg256 {
        let mut msg = SpscMsg256 {
            header: MsgHeader {
                abi_version: SPSC_ABI_VERSION,
                msg_type: msg_type as u16,
                msg_size: MSG_SIZE,
                reserved: 0,
                timestamp_us,
                seq,
                payload_len: payload.len().min(208) as u32,
                reserved2: 0,
                reserved3: [0; 2],
            },
            body: [0; 208],
        };

        // Copy payload (max 208 bytes)
        let copy_len = payload.len().min(208);
        msg.body[..copy_len].copy_from_slice(&payload[..copy_len]);

        msg
    }
}

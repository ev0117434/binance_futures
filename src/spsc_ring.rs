use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: u32 = 0x53505343; // "SPSC" in hex
const ABI_VERSION: u16 = 1;
const MSG_SIZE: usize = 256;

/// SPSC Ring Buffer Header (in shared memory)
/// Layout (little-endian):
/// - offset 0x00: u32 magic
/// - offset 0x04: u16 abi_version
/// - offset 0x06: u16 layout_version
/// - offset 0x08: u16 msg_size
/// - offset 0x0A: u16 flags
/// - offset 0x0C: u32 ring_slots
/// - offset 0x10: u64 _reserved0
/// - offset 0x18: u64 write_seq (atomic)
#[repr(C)]
struct RingHeader {
    magic: u32,           // 0x00
    abi_version: u16,     // 0x04
    layout_version: u16,  // 0x06
    msg_size: u16,        // 0x08
    flags: u16,           // 0x0A
    ring_slots: u32,      // 0x0C
    _reserved0: u64,      // 0x10
    write_seq: AtomicU64, // 0x18
}

pub struct RingWriter {
    _mmap: MmapMut,
    base_ptr: *mut u8,
    ring_mask: u32,
    data_off: usize,
    write_seq_ptr: *const AtomicU64,
}

unsafe impl Send for RingWriter {}
unsafe impl Sync for RingWriter {}

impl RingWriter {
    /// Open and validate SPSC ring buffer in shared memory
    pub fn open(path: &str) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("Failed to open ring file: {}", e))?;

        let mut mmap = unsafe {
            MmapMut::map_mut(&file)
                .map_err(|e| format!("Failed to mmap ring: {}", e))?
        };

        let base_ptr = mmap.as_mut_ptr();

        // Validate header
        unsafe {
            let header = &*(base_ptr as *const RingHeader);

            // Check magic
            if header.magic != MAGIC {
                return Err(format!(
                    "Invalid magic: expected 0x{:08X}, got 0x{:08X}",
                    MAGIC, header.magic
                ));
            }

            // Check ABI version (u16)
            if header.abi_version != ABI_VERSION {
                return Err(format!(
                    "Invalid abi_version: expected {}, got {}",
                    ABI_VERSION, header.abi_version
                ));
            }

            // Check message size (u16)
            if header.msg_size as usize != MSG_SIZE {
                return Err(format!(
                    "Invalid msg_size: expected {}, got {}",
                    MSG_SIZE, header.msg_size
                ));
            }

            // Check ring_slots is power of 2
            let ring_slots = header.ring_slots;
            if ring_slots == 0 || (ring_slots & (ring_slots - 1)) != 0 {
                return Err(format!(
                    "ring_slots must be power of 2, got {}",
                    ring_slots
                ));
            }

            let ring_mask = ring_slots - 1;
            let data_off = std::mem::size_of::<RingHeader>();
            let write_seq_ptr = &header.write_seq as *const AtomicU64;

            Ok(RingWriter {
                _mmap: mmap,
                base_ptr,
                ring_mask,
                data_off,
                write_seq_ptr,
            })
        }
    }

    /// Publish a 256-byte message to the ring buffer (lock-free)
    pub fn publish(&self, msg: &[u8; 256]) -> Result<(), String> {
        unsafe {
            // 1. Load current write sequence
            let seq = (*self.write_seq_ptr).load(Ordering::Relaxed);

            // 2. Calculate slot index
            let idx = (seq as u32) & self.ring_mask;

            // 3. Calculate slot pointer
            let slot_ptr = self.base_ptr.add(self.data_off + (idx as usize) * MSG_SIZE);

            // 4. Copy 256 bytes to slot
            std::ptr::copy_nonoverlapping(msg.as_ptr(), slot_ptr, MSG_SIZE);

            // 5. Increment write_seq (Release ordering for visibility)
            (*self.write_seq_ptr).store(seq + 1, Ordering::Release);

            Ok(())
        }
    }
}

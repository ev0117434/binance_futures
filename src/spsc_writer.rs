use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};

const MSG_SIZE: usize = 256;
const HEADER_SIZE: usize = 128;

/// Simple SPSC writer that writes to existing ring buffer
/// Does NOT create or initialize the ring buffer
pub struct SpscWriter {
    _mmap: MmapMut,
    data_ptr: *mut u8,
    ring_mask: u64,
    write_seq_ptr: *const AtomicU64,
}

unsafe impl Send for SpscWriter {}
unsafe impl Sync for SpscWriter {}

impl SpscWriter {
    /// Open existing SPSC ring buffer (read-only access to header, write to data)
    pub fn open(path: &str) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("Failed to open SPSC ring: {}", e))?;

        let mut mmap = unsafe {
            MmapMut::map_mut(&file)
                .map_err(|e| format!("Failed to mmap: {}", e))?
        };

        let base_ptr = mmap.as_mut_ptr();

        unsafe {
            // Read ring_slots from offset 16 (assuming standard layout)
            let ring_slots_ptr = base_ptr.add(16) as *const u64;
            let ring_slots = std::ptr::read_volatile(ring_slots_ptr);

            if ring_slots == 0 || !ring_slots.is_power_of_two() {
                return Err(format!("Invalid ring_slots: {}", ring_slots));
            }

            let ring_mask = ring_slots - 1;

            // write_seq at offset 32 (assuming standard layout)
            let write_seq_ptr = base_ptr.add(32) as *const AtomicU64;
            let data_ptr = base_ptr.add(HEADER_SIZE);

            Ok(SpscWriter {
                _mmap: mmap,
                data_ptr,
                ring_mask,
                write_seq_ptr,
            })
        }
    }

    /// Publish a 256-byte message to the ring buffer
    pub fn publish(&self, msg: &[u8; 256]) -> Result<(), String> {
        if msg.len() != 256 {
            return Err(format!("Message must be exactly 256 bytes, got {}", msg.len()));
        }

        unsafe {
            // Load current write sequence (Relaxed)
            let seq = (*self.write_seq_ptr).load(Ordering::Relaxed);

            // Calculate slot index
            let idx = seq & self.ring_mask;
            let slot_ptr = self.data_ptr.add((idx as usize) * MSG_SIZE);

            // CRITICAL: Copy BEFORE incrementing sequence
            std::ptr::copy_nonoverlapping(
                msg.as_ptr(),
                slot_ptr,
                256,
            );

            // CRITICAL: Release store to publish
            (*self.write_seq_ptr).store(seq + 1, Ordering::Release);

            Ok(())
        }
    }

    /// Create a 256-byte message from text
    pub fn create_message(text: &str) -> [u8; 256] {
        let mut msg = [0u8; 256];
        let bytes = text.as_bytes();
        let copy_len = bytes.len().min(256);
        msg[..copy_len].copy_from_slice(&bytes[..copy_len]);
        msg
    }
}

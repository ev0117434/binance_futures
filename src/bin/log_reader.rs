use memmap2::Mmap;
use std::env;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

const SPSC_MAGIC: u32 = 0x53505343; // "SPSC"
const SPSC_ABI_VERSION: u16 = 1;
const MSG_SIZE: u16 = 256;
const HEADER_SIZE: usize = 128;

/// Message types
#[derive(Debug)]
enum MsgType {
    Info,
    Warning,
    Error,
    Unknown(u16),
}

impl From<u16> for MsgType {
    fn from(val: u16) -> Self {
        match val {
            1 => MsgType::Info,
            2 => MsgType::Warning,
            3 => MsgType::Error,
            _ => MsgType::Unknown(val),
        }
    }
}

/// Message header (48 bytes)
#[repr(C, align(8))]
struct MsgHeader {
    abi_version: u16,
    msg_type: u16,
    msg_size: u16,
    reserved: u16,
    timestamp_us: i64,
    seq: u64,
    payload_len: u32,
    reserved2: u32,
    reserved3: [u64; 2],
}

/// Fixed-size message (256 bytes)
#[repr(C, align(8))]
struct SpscMsg256 {
    header: MsgHeader,
    body: [u8; 208],
}

/// SPSC Ring buffer header (128 bytes)
#[repr(C, align(8))]
struct RingHeader {
    magic: u32,
    abi_version: u16,
    msg_size: u16,
    ring_slots: u64,
    data_offset: u64,
    write_seq: AtomicU64,
    read_seq: AtomicU64,
    reserved: [u64; 11],
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <ring_buffer_path>", args[0]);
        eprintln!("Example: {} /dev/shm/ring_spsc_binance_f_log", args[0]);
        std::process::exit(1);
    }

    let ring_path = &args[1];

    let file = match OpenOptions::new()
        .read(true)
        .open(ring_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open ring buffer {}: {}", ring_path, e);
            std::process::exit(1);
        }
    };

    let mmap = unsafe {
        match Mmap::map(&file) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Failed to mmap: {}", e);
                std::process::exit(1);
            }
        }
    };

    let base_ptr = mmap.as_ptr();

    // Validate header
    unsafe {
        let header_ptr = base_ptr as *const RingHeader;

        let magic = std::ptr::read_volatile(&(*header_ptr).magic);
        if magic != SPSC_MAGIC {
            eprintln!("Invalid magic: expected 0x{:08X}, got 0x{:08X}", SPSC_MAGIC, magic);
            std::process::exit(1);
        }

        let abi_version = std::ptr::read_volatile(&(*header_ptr).abi_version);
        if abi_version != SPSC_ABI_VERSION {
            eprintln!("Invalid ABI version: expected {}, got {}", SPSC_ABI_VERSION, abi_version);
            std::process::exit(1);
        }

        let msg_size = std::ptr::read_volatile(&(*header_ptr).msg_size);
        if msg_size != MSG_SIZE {
            eprintln!("Invalid msg_size: expected {}, got {}", MSG_SIZE, msg_size);
            std::process::exit(1);
        }

        let ring_slots = std::ptr::read_volatile(&(*header_ptr).ring_slots);
        let ring_mask = ring_slots - 1;

        eprintln!("SPSC Ring Buffer Reader");
        eprintln!("Path: {}", ring_path);
        eprintln!("Ring slots: {}", ring_slots);
        eprintln!("Message size: {} bytes", msg_size);
        eprintln!("Total size: {} KB", (HEADER_SIZE + (ring_slots as usize * msg_size as usize)) / 1024);
        eprintln!("---");

        let write_seq_ptr = &(*header_ptr).write_seq as *const AtomicU64;
        let read_seq_ptr = &(*header_ptr).read_seq as *const AtomicU64;
        let data_ptr = base_ptr.add(HEADER_SIZE);

        // Start reading from current write position
        let mut local_read_seq = (*write_seq_ptr).load(Ordering::Acquire);

        loop {
            let write_seq = (*write_seq_ptr).load(Ordering::Acquire);

            if local_read_seq < write_seq {
                // Messages available
                let idx = local_read_seq & ring_mask;
                let slot_ptr = data_ptr.add((idx as usize) * 256) as *const SpscMsg256;

                // Read message
                let msg: SpscMsg256 = std::ptr::read_volatile(slot_ptr);

                // Validate message
                if msg.header.abi_version == SPSC_ABI_VERSION && msg.header.msg_size == MSG_SIZE {
                    let msg_type = MsgType::from(msg.header.msg_type);
                    let payload_len = msg.header.payload_len.min(208) as usize;
                    let payload = &msg.body[..payload_len];

                    // Format output
                    let type_str = match msg_type {
                        MsgType::Info => "INFO",
                        MsgType::Warning => "WARN",
                        MsgType::Error => "ERROR",
                        MsgType::Unknown(t) => {
                            eprintln!("Unknown msg_type: {}", t);
                            "UNKNOWN"
                        }
                    };

                    let payload_str = String::from_utf8_lossy(payload);

                    println!("[{}] [{}] {}", msg.header.timestamp_us, type_str, payload_str);
                }

                local_read_seq += 1;

                // Update read sequence (optional, for monitoring)
                (*read_seq_ptr).store(local_read_seq, Ordering::Release);
            } else {
                // No new messages, sleep briefly
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

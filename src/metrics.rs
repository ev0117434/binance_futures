use std::time::{SystemTime, UNIX_EPOCH};

/// Get current Unix timestamp in milliseconds
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// WebSocket state enum
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsState {
    Connecting = 0,
    Handshake = 1,
    Subscribed = 2,
    Running = 3,
    Reconnecting = 4,
    Failed = 5,
}

/// Stream type enum
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum StreamType {
    BookTicker = 1,
}

/// Error domain enum
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum ErrorDomain {
    WS = 1,
    SHM = 2,
    SYS = 3,
}

/// Message type enum
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum MessageType {
    SnapshotStart = 1,
    SnapshotPeriodic = 2,
    EventError = 3,
}

/// Latency aggregator for a period
#[derive(Debug, Clone)]
pub struct LatencyAggregator {
    pub count: u32,
    pub min_ms: u32,
    pub max_ms: u32,
    pub sum_ms: u64,
}

impl LatencyAggregator {
    pub fn new() -> Self {
        Self {
            count: 0,
            min_ms: u32::MAX,
            max_ms: 0,
            sum_ms: 0,
        }
    }

    pub fn record(&mut self, value_ms: u32) {
        self.count += 1;
        self.min_ms = self.min_ms.min(value_ms);
        self.max_ms = self.max_ms.max(value_ms);
        self.sum_ms += value_ms as u64;
    }

    pub fn avg(&self) -> u32 {
        if self.count > 0 {
            (self.sum_ms / self.count as u64) as u32
        } else {
            0
        }
    }

    pub fn reset(&mut self) {
        self.count = 0;
        self.min_ms = u32::MAX;
        self.max_ms = 0;
        self.sum_ms = 0;
    }
}

/// Runtime metrics state
#[derive(Debug, Clone)]
pub struct Metrics {
    // Session info
    pub session_id: u32,
    pub session_start_ms: u64,
    pub ws_state: WsState,

    // WS liveness
    pub ws_last_frame_rx_ms: u64,
    pub ws_last_data_rx_ms: u64,
    pub ws_rx_frames_total: u64,
    pub ws_rx_bytes_total: u64,
    pub ws_rx_msgs_total: u64,

    // WS errors and reconnects
    pub ws_reconnect_total: u32,
    pub ws_disconnect_total: u32,
    pub ws_handshake_fail_total: u32,
    pub ws_subscribe_ok_total: u32,
    pub ws_subscribe_fail_total: u32,
    pub ws_parse_error_total: u32,
    pub ws_protocol_error_total: u32,
    pub ws_last_error_code: i32,
    pub ws_last_error_ms: u64,
    pub ws_last_close_code: u16,

    // SHM write
    pub shm_write_ok_total: u64,
    pub shm_write_fail_total: u32,
    pub shm_last_write_ok_ms: u64,
    pub shm_last_write_fail_ms: u64,

    // Latency aggregator
    pub lat_rx_to_written: LatencyAggregator,

    // Previous totals for deltas
    pub prev_rx_msgs_total: u64,
    pub prev_reconnect_total: u32,
    pub prev_subscribe_fail_total: u32,
    pub prev_parse_error_total: u32,
    pub prev_protocol_error_total: u32,
    pub prev_shm_write_ok_total: u64,
    pub prev_shm_write_fail_total: u32,

    // Logger sequence
    pub log_seq: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            session_id: 0,
            session_start_ms: now_ms(),
            ws_state: WsState::Connecting,
            ws_last_frame_rx_ms: 0,
            ws_last_data_rx_ms: 0,
            ws_rx_frames_total: 0,
            ws_rx_bytes_total: 0,
            ws_rx_msgs_total: 0,
            ws_reconnect_total: 0,
            ws_disconnect_total: 0,
            ws_handshake_fail_total: 0,
            ws_subscribe_ok_total: 0,
            ws_subscribe_fail_total: 0,
            ws_parse_error_total: 0,
            ws_protocol_error_total: 0,
            ws_last_error_code: 0,
            ws_last_error_ms: 0,
            ws_last_close_code: 0,
            shm_write_ok_total: 0,
            shm_write_fail_total: 0,
            shm_last_write_ok_ms: 0,
            shm_last_write_fail_ms: 0,
            lat_rx_to_written: LatencyAggregator::new(),
            prev_rx_msgs_total: 0,
            prev_reconnect_total: 0,
            prev_subscribe_fail_total: 0,
            prev_parse_error_total: 0,
            prev_protocol_error_total: 0,
            prev_shm_write_ok_total: 0,
            prev_shm_write_fail_total: 0,
            log_seq: 0,
        }
    }

    pub fn new_session(&mut self) {
        self.session_id += 1;
        self.session_start_ms = now_ms();
        self.ws_state = WsState::Connecting;
    }
}

/// Binary message structure (256 bytes)
#[repr(C, packed)]
#[derive(Copy, Clone)]
pub struct SpscMsg256 {
    // Common header (32 bytes) - packed to avoid padding
    pub msg_type: u8,          // 0
    pub abi_version: u8,       // 1
    pub layout_version: u8,    // 2
    pub _reserved0: u8,        // 3
    pub ts_ms: u64,            // 4-11
    pub session_id: u32,       // 12-15
    pub pid: u32,              // 16-19
    pub ws_state: u8,          // 20
    pub _pad0: [u8; 3],        // 21-23
    pub log_seq: u64,          // 24-31
    // Total header: 32 bytes

    // Payload (224 bytes) - union depending on msg_type
    pub payload: [u8; 224],    // 32-255
}

impl SpscMsg256 {
    pub fn as_bytes(&self) -> &[u8; 256] {
        unsafe { std::mem::transmute(self) }
    }
}

const _: () = assert!(std::mem::size_of::<SpscMsg256>() == 256);

/// SNAPSHOT_START payload
#[repr(C, packed)]
#[derive(Copy, Clone)]
pub struct SnapshotStartPayload {
    pub start_ms: u64,              // 0
    pub build_version_hash: u64,    // 8
    pub symbols_count: u32,          // 16
    pub stream_type: u8,            // 20
    pub _pad0: [u8; 3],             // 21
    pub ring_size: u32,             // 24
    pub msg_size: u32,              // 28
    pub ws_url_hash: u64,           // 32

    // Totals
    pub ws_rx_frames_total: u64,    // 40
    pub ws_rx_bytes_total: u64,     // 48
    pub ws_rx_msgs_total: u64,      // 56
    pub ws_reconnect_total: u32,    // 64
    pub ws_disconnect_total: u32,   // 68
    pub ws_handshake_fail_total: u32, // 72
    pub ws_subscribe_ok_total: u32, // 76
    pub ws_subscribe_fail_total: u32, // 80
    pub ws_parse_error_total: u32,  // 84
    pub ws_protocol_error_total: u32, // 88
    pub _pad1: u32,                 // 92
    pub shm_write_ok_total: u64,    // 96
    pub shm_write_fail_total: u32,  // 104

    // Last times
    pub _pad2: u32,                 // 108 (alignment)
    pub last_frame_rx_ms: u64,      // 112
    pub last_data_rx_ms: u64,       // 120
    pub last_error_code: i32,       // 128
    pub last_close_code: u16,       // 132

    pub _padding: [u8; 90],         // 134 to 224
}

const _: () = assert!(std::mem::size_of::<SnapshotStartPayload>() == 224);

/// SNAPSHOT_PERIODIC payload
#[repr(C, packed)]
#[derive(Copy, Clone)]
pub struct SnapshotPeriodicPayload {
    pub session_start_ms: u64,      // 0
    pub last_frame_rx_ms: u64,      // 8
    pub last_data_rx_ms: u64,       // 16
    pub uptime_ms: u64,             // 24
    pub silence_ms: u64,            // 32

    // Deltas
    pub rx_msgs_delta: u32,         // 40
    pub reconnect_delta: u32,       // 44
    pub subscribe_fail_delta: u32,  // 48
    pub parse_error_delta: u32,     // 52
    pub protocol_error_delta: u32,  // 56
    pub shm_write_ok_delta: u32,    // 60
    pub shm_write_fail_delta: u32,  // 64
    pub _pad0: u32,                 // 68

    // Last write times
    pub shm_last_write_ok_ms: u64,  // 72
    pub shm_last_write_fail_ms: u64, // 80

    // Latency aggregator
    pub lat_count: u32,             // 88
    pub lat_min_ms: u32,            // 92
    pub lat_max_ms: u32,            // 96
    pub lat_avg_ms: u32,            // 100

    // Last error codes
    pub last_error_code: i32,       // 104
    pub last_close_code: u16,       // 108

    pub _padding: [u8; 114],        // 110 to 224
}

const _: () = assert!(std::mem::size_of::<SnapshotPeriodicPayload>() == 224);

/// EVENT_ERROR payload
#[repr(C, packed)]
#[derive(Copy, Clone)]
pub struct EventErrorPayload {
    pub domain: u8,             // 0
    pub _pad0: u8,              // 1
    pub code: i16,              // 2
    pub aux_u32: u32,           // 4
    pub aux_u64_0: u64,         // 8
    pub aux_u64_1: u64,         // 16
    pub last_frame_rx_ms: u64,  // 24
    pub last_data_rx_ms: u64,   // 32

    pub _padding: [u8; 184],    // 40 to 224
}

const _: () = assert!(std::mem::size_of::<EventErrorPayload>() == 224);

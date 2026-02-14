use crate::metrics::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

pub fn build_snapshot_start(
    metrics: &Metrics,
    symbols_count: u32,
    ws_url: &str,
    ring_size: u32,
) -> [u8; 256] {
    let mut msg = SpscMsg256 {
        msg_type: MessageType::SnapshotStart as u8,
        abi_version: 1,
        layout_version: 1,
        _reserved0: 0,
        ts_ms: now_ms(),
        ws_state: metrics.ws_state as u8,
        _pad0: [0; 3],
        session_id: metrics.session_id,
        pid: std::process::id(),
        log_seq: metrics.log_seq,
        payload: [0; 224],
    };

    let payload = SnapshotStartPayload {
        start_ms: metrics.session_start_ms,
        build_version_hash: hash_string(env!("CARGO_PKG_VERSION")),
        symbols_count,
        stream_type: StreamType::BookTicker as u8,
        _pad0: [0; 3],
        ring_size,
        msg_size: 256,
        ws_url_hash: hash_string(ws_url),
        ws_rx_frames_total: metrics.ws_rx_frames_total,
        ws_rx_bytes_total: metrics.ws_rx_bytes_total,
        ws_rx_msgs_total: metrics.ws_rx_msgs_total,
        ws_reconnect_total: metrics.ws_reconnect_total,
        ws_disconnect_total: metrics.ws_disconnect_total,
        ws_handshake_fail_total: metrics.ws_handshake_fail_total,
        ws_subscribe_ok_total: metrics.ws_subscribe_ok_total,
        ws_subscribe_fail_total: metrics.ws_subscribe_fail_total,
        ws_parse_error_total: metrics.ws_parse_error_total,
        ws_protocol_error_total: metrics.ws_protocol_error_total,
        _pad1: 0,
        shm_write_ok_total: metrics.shm_write_ok_total,
        shm_write_fail_total: metrics.shm_write_fail_total,
        _pad2: 0,
        last_frame_rx_ms: metrics.ws_last_frame_rx_ms,
        last_data_rx_ms: metrics.ws_last_data_rx_ms,
        last_error_code: metrics.ws_last_error_code,
        last_close_code: metrics.ws_last_close_code,
        _padding: [0; 90],
    };

    unsafe {
        std::ptr::copy_nonoverlapping(
            &payload as *const SnapshotStartPayload as *const u8,
            msg.payload.as_mut_ptr(),
            224,
        );
    }

    *msg.as_bytes()
}

pub fn build_snapshot_periodic(metrics: &mut Metrics) -> [u8; 256] {
    let ts_ms = now_ms();
    let uptime_ms = ts_ms.saturating_sub(metrics.session_start_ms);
    let silence_ms = if metrics.ws_last_data_rx_ms > 0 {
        ts_ms.saturating_sub(metrics.ws_last_data_rx_ms)
    } else {
        ts_ms
    };

    // Calculate deltas
    let rx_msgs_delta = (metrics.ws_rx_msgs_total - metrics.prev_rx_msgs_total) as u32;
    let reconnect_delta = metrics.ws_reconnect_total - metrics.prev_reconnect_total;
    let subscribe_fail_delta = metrics.ws_subscribe_fail_total - metrics.prev_subscribe_fail_total;
    let parse_error_delta = metrics.ws_parse_error_total - metrics.prev_parse_error_total;
    let protocol_error_delta = metrics.ws_protocol_error_total - metrics.prev_protocol_error_total;
    let shm_write_ok_delta = (metrics.shm_write_ok_total - metrics.prev_shm_write_ok_total) as u32;
    let shm_write_fail_delta = metrics.shm_write_fail_total - metrics.prev_shm_write_fail_total;

    let mut msg = SpscMsg256 {
        msg_type: MessageType::SnapshotPeriodic as u8,
        abi_version: 1,
        layout_version: 1,
        _reserved0: 0,
        ts_ms,
        ws_state: metrics.ws_state as u8,
        _pad0: [0; 3],
        session_id: metrics.session_id,
        pid: std::process::id(),
        log_seq: metrics.log_seq,
        payload: [0; 224],
    };

    let payload = SnapshotPeriodicPayload {
        session_start_ms: metrics.session_start_ms,
        last_frame_rx_ms: metrics.ws_last_frame_rx_ms,
        last_data_rx_ms: metrics.ws_last_data_rx_ms,
        uptime_ms,
        silence_ms,
        rx_msgs_delta,
        reconnect_delta,
        subscribe_fail_delta,
        parse_error_delta,
        protocol_error_delta,
        shm_write_ok_delta,
        shm_write_fail_delta,
        _pad0: 0,
        shm_last_write_ok_ms: metrics.shm_last_write_ok_ms,
        shm_last_write_fail_ms: metrics.shm_last_write_fail_ms,
        lat_count: metrics.lat_rx_to_written.count,
        lat_min_ms: metrics.lat_rx_to_written.min_ms,
        lat_max_ms: metrics.lat_rx_to_written.max_ms,
        lat_avg_ms: metrics.lat_rx_to_written.avg(),
        last_error_code: metrics.ws_last_error_code,
        last_close_code: metrics.ws_last_close_code,
        _padding: [0; 114],
    };

    unsafe {
        std::ptr::copy_nonoverlapping(
            &payload as *const SnapshotPeriodicPayload as *const u8,
            msg.payload.as_mut_ptr(),
            224,
        );
    }

    // Update previous totals for next delta calculation
    metrics.prev_rx_msgs_total = metrics.ws_rx_msgs_total;
    metrics.prev_reconnect_total = metrics.ws_reconnect_total;
    metrics.prev_subscribe_fail_total = metrics.ws_subscribe_fail_total;
    metrics.prev_parse_error_total = metrics.ws_parse_error_total;
    metrics.prev_protocol_error_total = metrics.ws_protocol_error_total;
    metrics.prev_shm_write_ok_total = metrics.shm_write_ok_total;
    metrics.prev_shm_write_fail_total = metrics.shm_write_fail_total;

    // Reset latency aggregator
    metrics.lat_rx_to_written.reset();

    *msg.as_bytes()
}

pub fn build_event_error(
    metrics: &Metrics,
    domain: ErrorDomain,
    code: i16,
    aux_u32: u32,
    aux_u64_0: u64,
    aux_u64_1: u64,
) -> [u8; 256] {
    let mut msg = SpscMsg256 {
        msg_type: MessageType::EventError as u8,
        abi_version: 1,
        layout_version: 1,
        _reserved0: 0,
        ts_ms: now_ms(),
        ws_state: metrics.ws_state as u8,
        _pad0: [0; 3],
        session_id: metrics.session_id,
        pid: std::process::id(),
        log_seq: metrics.log_seq,
        payload: [0; 224],
    };

    let payload = EventErrorPayload {
        domain: domain as u8,
        _pad0: 0,
        code,
        aux_u32,
        aux_u64_0,
        aux_u64_1,
        last_frame_rx_ms: metrics.ws_last_frame_rx_ms,
        last_data_rx_ms: metrics.ws_last_data_rx_ms,
        _padding: [0; 184],
    };

    unsafe {
        std::ptr::copy_nonoverlapping(
            &payload as *const EventErrorPayload as *const u8,
            msg.payload.as_mut_ptr(),
            224,
        );
    }

    *msg.as_bytes()
}

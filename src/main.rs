mod shm;
mod symbols;
mod spsc_ring;
mod metrics;
mod message_builder;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use spsc_ring::RingWriter;
use metrics::{Metrics, WsState, ErrorDomain, now_ms};
use message_builder::*;

const SUBSCRIBE_FILE: &str = "/root/siro/dictionaries/subscribe/binance/binance_futures.txt";
const SYMBOLS_TSV: &str = "/root/siro/dictionaries/configs/symbols.tsv";
const SHM_PATH: &str = "/dev/shm/quotes_v1.dat";
const SPSC_RING_PATH: &str = "/dev/shm/ring_spsc_binance_f_log";
const SNAPSHOT_INTERVAL_MS: u64 = 5000; // 5 seconds
const SOURCE_ID: u64 = 1;
const STREAMS_PER_CONNECTION: usize = 512;
const PRICE_SCALE: i64 = 100_000_000; // 1e8

#[derive(Debug, Deserialize, Serialize)]
struct BookTickerData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid: String,
    #[serde(rename = "a")]
    ask: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct BookTickerMessage {
    stream: String,
    data: BookTickerData,
}

fn get_monotonic_us() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap();
    (now.as_secs() * 1_000_000 + now.subsec_micros() as u64) as i64
}

fn parse_decimal_to_scaled(s: &str, scale: i64) -> Result<i64, String> {
    let parts: Vec<&str> = s.split('.').collect();

    match parts.len() {
        1 => {
            let int_part = parts[0].parse::<i64>()
                .map_err(|e| format!("Failed to parse integer part: {}", e))?;
            Ok(int_part * scale)
        }
        2 => {
            let int_part = parts[0].parse::<i64>()
                .map_err(|e| format!("Failed to parse integer part: {}", e))?;

            let mut frac_str = parts[1].to_string();

            let scale_digits = 8;

            if frac_str.len() > scale_digits {
                let next_digit = frac_str.chars().nth(scale_digits).unwrap().to_digit(10).unwrap();
                frac_str.truncate(scale_digits);

                let mut frac_part = frac_str.parse::<i64>()
                    .map_err(|e| format!("Failed to parse fractional part: {}", e))?;

                if next_digit >= 5 {
                    frac_part += 1;
                }

                let result = int_part * scale + if int_part >= 0 { frac_part } else { -frac_part };
                Ok(result)
            } else {
                while frac_str.len() < scale_digits {
                    frac_str.push('0');
                }

                let frac_part = frac_str.parse::<i64>()
                    .map_err(|e| format!("Failed to parse fractional part: {}", e))?;

                let result = int_part * scale + if int_part >= 0 { frac_part } else { -frac_part };
                Ok(result)
            }
        }
        _ => Err(format!("Invalid decimal format: {}", s))
    }
}

async fn run_websocket_connection(
    url: String,
    symbol_map: Arc<HashMap<String, u64>>,
    shm: Arc<shm::ShmWriter>,
    ring: Arc<RingWriter>,
    metrics: Arc<Mutex<Metrics>>,
) -> Result<(), String> {
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(30);
    let mut next_snapshot_ms = now_ms() + SNAPSHOT_INTERVAL_MS;

    loop {
        // Update metrics: new session
        {
            let mut m = metrics.lock().unwrap();
            m.new_session();
            m.ws_state = WsState::Connecting;
        }

        eprintln!("[INFO] Connecting to {}", url);

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                eprintln!("[INFO] Connected to {}", url);

                // Update metrics: connected
                {
                    let mut m = metrics.lock().unwrap();
                    m.ws_state = WsState::Running;
                }

                backoff = Duration::from_millis(200);

                let (mut _write, mut read) = ws_stream.split();

                while let Some(msg_result) = read.next().await {
                    // Check if snapshot is due
                    let current_ms = now_ms();
                    if current_ms >= next_snapshot_ms {
                        let msg = {
                            let mut m = metrics.lock().unwrap();
                            m.log_seq += 1;
                            build_snapshot_periodic(&mut m)
                        };
                        let _ = ring.publish(&msg);
                        next_snapshot_ms += SNAPSHOT_INTERVAL_MS;
                    }

                    match msg_result {
                        Ok(Message::Text(text)) => {
                            let t_rx_ms = now_ms();

                            // Update WS frame metrics
                            {
                                let mut m = metrics.lock().unwrap();
                                m.ws_last_frame_rx_ms = t_rx_ms;
                                m.ws_rx_frames_total += 1;
                                m.ws_rx_bytes_total += text.len() as u64;
                            }

                            match serde_json::from_str::<BookTickerMessage>(&text) {
                                Ok(book_ticker) => {
                                    let t_parsed_ms = now_ms();

                                    let symbol = &book_ticker.data.symbol;

                                    if let Some(&symbol_id) = symbol_map.get(symbol) {
                                        // Update WS data received metrics
                                        {
                                            let mut m = metrics.lock().unwrap();
                                            m.ws_last_data_rx_ms = t_parsed_ms;
                                            m.ws_rx_msgs_total += 1;
                                        }

                                        match (
                                            parse_decimal_to_scaled(&book_ticker.data.bid, PRICE_SCALE),
                                            parse_decimal_to_scaled(&book_ticker.data.ask, PRICE_SCALE),
                                        ) {
                                            (Ok(bid_i64), Ok(ask_i64)) => {
                                                let t_mapped_ms = now_ms();
                                                let ts_us = get_monotonic_us();

                                                if let Err(_e) = shm.write_quote(
                                                    SOURCE_ID,
                                                    symbol_id,
                                                    bid_i64,
                                                    ask_i64,
                                                    ts_us,
                                                ) {
                                                    let t_fail_ms = now_ms();

                                                    // Update SHM write fail metrics
                                                    {
                                                        let mut m = metrics.lock().unwrap();
                                                        m.shm_write_fail_total += 1;
                                                        m.shm_last_write_fail_ms = t_fail_ms;
                                                        m.ws_last_error_code = -1;
                                                        m.log_seq += 1;

                                                        let error_msg = build_event_error(
                                                            &m,
                                                            ErrorDomain::SHM,
                                                            -1,
                                                            0,
                                                            0,
                                                            0,
                                                        );
                                                        let _ = ring.publish(&error_msg);
                                                    }

                                                    std::process::exit(11);
                                                } else {
                                                    let t_written_ms = now_ms();

                                                    // Update SHM write success metrics and latency
                                                    {
                                                        let mut m = metrics.lock().unwrap();
                                                        m.shm_write_ok_total += 1;
                                                        m.shm_last_write_ok_ms = t_written_ms;

                                                        // Calculate and record latency
                                                        let lat_ms = (t_written_ms - t_rx_ms) as u32;
                                                        m.lat_rx_to_written.record(lat_ms);
                                                    }
                                                }
                                            }
                                            (Err(_e), _) | (_, Err(_e)) => {
                                                // Update parse error metrics
                                                let mut m = metrics.lock().unwrap();
                                                m.ws_parse_error_total += 1;
                                            }
                                        }
                                    } else {
                                        // Symbol not in map - fatal error
                                        eprintln!("[ERROR] Symbol {} not in map", symbol);
                                        std::process::exit(10);
                                    }
                                }
                                Err(_e) => {
                                    // Update parse error metrics
                                    let mut m = metrics.lock().unwrap();
                                    m.ws_parse_error_total += 1;
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {
                            let t_ms = now_ms();
                            let mut m = metrics.lock().unwrap();
                            m.ws_last_frame_rx_ms = t_ms;
                            m.ws_rx_frames_total += 1;
                        }
                        Ok(Message::Pong(_)) => {
                            let t_ms = now_ms();
                            let mut m = metrics.lock().unwrap();
                            m.ws_last_frame_rx_ms = t_ms;
                            m.ws_rx_frames_total += 1;
                        }
                        Ok(Message::Close(frame)) => {
                            eprintln!("[WARN] Connection closed");

                            let close_code = frame.as_ref().map(|f| f.code.into()).unwrap_or(0);

                            // Update metrics
                            {
                                let mut m = metrics.lock().unwrap();
                                m.ws_disconnect_total += 1;
                                m.ws_last_close_code = close_code;
                                m.ws_state = WsState::Reconnecting;
                            }

                            break;
                        }
                        Err(_e) => {
                            eprintln!("[ERROR] WebSocket error");

                            // Update metrics
                            {
                                let mut m = metrics.lock().unwrap();
                                m.ws_protocol_error_total += 1;
                                m.ws_last_error_ms = now_ms();
                                m.ws_state = WsState::Reconnecting;
                            }

                            break;
                        }
                        _ => {}
                    }
                }

                eprintln!("[WARN] Connection lost, reconnecting...");

                // Update reconnect metrics
                {
                    let mut m = metrics.lock().unwrap();
                    m.ws_reconnect_total += 1;
                    m.log_seq += 1;

                    let error_msg = build_event_error(
                        &m,
                        ErrorDomain::WS,
                        -2,
                        0,
                        0,
                        m.session_id as u64,
                    );
                    let _ = ring.publish(&error_msg);
                }
            }
            Err(_e) => {
                eprintln!("[ERROR] Failed to connect");

                // Update handshake fail metrics
                {
                    let mut m = metrics.lock().unwrap();
                    m.ws_handshake_fail_total += 1;
                    m.ws_last_error_ms = now_ms();
                    m.log_seq += 1;

                    let error_msg = build_event_error(
                        &m,
                        ErrorDomain::WS,
                        -3,
                        0,
                        0,
                        0,
                    );
                    let _ = ring.publish(&error_msg);
                }
            }
        }

        sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
}

#[tokio::main]
async fn main() {
    eprintln!("[INFO] Starting binance_futures_writer with SPSC ring metrics");

    let mapper = match symbols::SymbolMapper::load_from_tsv(SYMBOLS_TSV) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[FATAL] {}", e);
            std::process::exit(20);
        }
    };

    eprintln!("[INFO] Loaded {} symbols from {}", mapper.len(), SYMBOLS_TSV);

    let subscribe_symbols = match symbols::load_subscribe_list(SUBSCRIBE_FILE) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[FATAL] {}", e);
            std::process::exit(20);
        }
    };

    eprintln!("[INFO] Loaded {} symbols to subscribe from {}", subscribe_symbols.len(), SUBSCRIBE_FILE);

    let symbol_map = match symbols::validate_symbols(&subscribe_symbols, &mapper) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[FATAL] {}", e);
            std::process::exit(20);
        }
    };

    let shm = match shm::ShmWriter::open(SHM_PATH) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("[FATAL] {}", e);
            std::process::exit(1);
        }
    };

    eprintln!("[INFO] SHM opened successfully");

    // Open SPSC ring for metrics
    let ring = match RingWriter::open(SPSC_RING_PATH) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("[FATAL] Failed to open SPSC ring: {}", e);
            std::process::exit(1);
        }
    };

    eprintln!("[INFO] SPSC ring opened successfully");

    // Initialize metrics
    let metrics = Arc::new(Mutex::new(Metrics::new()));

    // Publish SNAPSHOT_START
    {
        let mut m = metrics.lock().unwrap();
        m.log_seq += 1;
        let start_msg = build_snapshot_start(
            &m,
            subscribe_symbols.len() as u32,
            "wss://fstream.binance.com/stream",
            1024, // example ring size
        );
        if let Err(e) = ring.publish(&start_msg) {
            eprintln!("[FATAL] Failed to publish SNAPSHOT_START: {}", e);
            std::process::exit(1);
        }
    }

    eprintln!("[INFO] Published SNAPSHOT_START");

    let chunks: Vec<Vec<String>> = subscribe_symbols
        .chunks(STREAMS_PER_CONNECTION)
        .map(|chunk| chunk.to_vec())
        .collect();

    eprintln!("[INFO] Created {} connection(s) for {} symbols", chunks.len(), subscribe_symbols.len());

    let symbol_map = Arc::new(symbol_map);
    let mut handles = vec![];

    for (idx, chunk) in chunks.iter().enumerate() {
        let streams: Vec<String> = chunk
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();

        let url = format!(
            "wss://fstream.binance.com/stream?streams={}",
            streams.join("/")
        );

        let symbol_map_clone = Arc::clone(&symbol_map);
        let shm_clone = Arc::clone(&shm);
        let ring_clone = Arc::clone(&ring);
        let metrics_clone = Arc::clone(&metrics);

        let handle = tokio::spawn(async move {
            if let Err(e) = run_websocket_connection(url, symbol_map_clone, shm_clone, ring_clone, metrics_clone).await {
                eprintln!("[FATAL] Connection {} failed: {}", idx, e);
                std::process::exit(2);
            }
        });

        handles.push(handle);

        if idx < chunks.len() - 1 {
            sleep(Duration::from_secs(1)).await;
        }
    }

    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("[FATAL] Task failed: {}", e);
            std::process::exit(3);
        }
    }
}

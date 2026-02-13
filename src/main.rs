mod shm;
mod symbols;
mod logger;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use logger::Logger;

const SUBSCRIBE_FILE: &str = "/root/siro/dictionaries/subscribe/binance/binance_futures.txt";
const SYMBOLS_TSV: &str = "/root/siro/dictionaries/configs/symbols.tsv";
const SHM_PATH: &str = "/dev/shm/quotes_v1.dat";
const LOG_PATH: &str = "/var/log/binance_futures_writer.log";
const LOG_RING_SIZE: usize = 8192;
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
    logger: Arc<Logger>,
) -> Result<(), String> {
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(30);

    loop {
        log_info!(logger, "Connecting to {}", url);

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                log_info!(logger, "Connected to {}", url);
                backoff = Duration::from_millis(200);

                let (mut _write, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<BookTickerMessage>(&text) {
                                Ok(book_ticker) => {
                                    let symbol = &book_ticker.data.symbol;

                                    if let Some(&symbol_id) = symbol_map.get(symbol) {
                                        match (
                                            parse_decimal_to_scaled(&book_ticker.data.bid, PRICE_SCALE),
                                            parse_decimal_to_scaled(&book_ticker.data.ask, PRICE_SCALE),
                                        ) {
                                            (Ok(bid_i64), Ok(ask_i64)) => {
                                                let ts_us = get_monotonic_us();

                                                if let Err(e) = shm.write_quote(
                                                    SOURCE_ID,
                                                    symbol_id,
                                                    bid_i64,
                                                    ask_i64,
                                                    ts_us,
                                                ) {
                                                    log_error!(logger, "Failed to write quote for {}: {}", symbol, e);
                                                    std::process::exit(11);
                                                }
                                            }
                                            (Err(e), _) | (_, Err(e)) => {
                                                log_error!(logger, "Failed to parse price for {}: {}", symbol, e);
                                            }
                                        }
                                    } else {
                                        log_error!(logger, "Symbol {} not in map", symbol);
                                        std::process::exit(10);
                                    }
                                }
                                Err(e) => {
                                    log_warn!(logger, "Failed to parse message: {}", e);
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {}
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => {
                            log_warn!(logger, "Connection closed");
                            break;
                        }
                        Err(e) => {
                            log_error!(logger, "WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }

                log_warn!(logger, "Connection lost, reconnecting...");
            }
            Err(e) => {
                log_error!(logger, "Failed to connect: {}", e);
            }
        }

        sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
}

#[tokio::main]
async fn main() {
    // Initialize async logger with SPSC ring buffer
    let logger = Arc::new(Logger::new(LOG_RING_SIZE, LOG_PATH));

    log_info!(logger, "Starting binance_futures_writer");

    let mapper = match symbols::SymbolMapper::load_from_tsv(SYMBOLS_TSV) {
        Ok(m) => m,
        Err(e) => {
            log_fatal!(logger, "{}", e);
            std::process::exit(20);
        }
    };

    log_info!(logger, "Loaded {} symbols from {}", mapper.len(), SYMBOLS_TSV);

    let subscribe_symbols = match symbols::load_subscribe_list(SUBSCRIBE_FILE) {
        Ok(s) => s,
        Err(e) => {
            log_fatal!(logger, "{}", e);
            std::process::exit(20);
        }
    };

    log_info!(logger, "Loaded {} symbols to subscribe from {}", subscribe_symbols.len(), SUBSCRIBE_FILE);

    let symbol_map = match symbols::validate_symbols(&subscribe_symbols, &mapper) {
        Ok(m) => m,
        Err(e) => {
            log_fatal!(logger, "{}", e);
            std::process::exit(20);
        }
    };

    let shm = match shm::ShmWriter::open(SHM_PATH) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            log_fatal!(logger, "{}", e);
            std::process::exit(1);
        }
    };

    log_info!(logger, "SHM opened successfully");

    let chunks: Vec<Vec<String>> = subscribe_symbols
        .chunks(STREAMS_PER_CONNECTION)
        .map(|chunk| chunk.to_vec())
        .collect();

    log_info!(logger, "Created {} connection(s) for {} symbols", chunks.len(), subscribe_symbols.len());

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
        let logger_clone = Arc::clone(&logger);

        let handle = tokio::spawn(async move {
            if let Err(e) = run_websocket_connection(url, symbol_map_clone, shm_clone, logger_clone).await {
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
            log_fatal!(logger, "Task failed: {}", e);
            std::process::exit(3);
        }
    }
}

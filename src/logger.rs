use crate::spsc_writer::SpscWriter;
use ringbuf::traits::{Consumer, Producer, Split};
use ringbuf::HeapRb;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const RING_BUFFER_SIZE: usize = 65536; // 64K entries (internal ringbuf)
const SPSC_PATH: &str = "/dev/shm/ring_spsc_binance_f_log";

/// Log entry types for different events
#[derive(Debug, Clone)]
pub enum LogEntry {
    /// Quote received and written to SHM
    QuoteWritten {
        symbol: String,
        symbol_id: u64,
        bid: i64,
        ask: i64,
        ts_us: i64,
    },
    /// WebSocket connection event
    ConnectionEvent {
        event: String,
        url: String,
    },
    /// Error event
    Error {
        message: String,
        context: String,
    },
    /// Warning event
    Warning {
        message: String,
    },
    /// Info event
    Info {
        message: String,
    },
    /// Shutdown signal
    Shutdown,
}

/// Logger that writes to SPSC ring buffer
pub struct Logger {
    producer: Arc<std::sync::Mutex<ringbuf::HeapProd<LogEntry>>>,
}

impl Logger {
    /// Create a new logger and start the consumer thread
    pub fn new() -> (Self, std::thread::JoinHandle<()>) {
        let ring_buffer = HeapRb::<LogEntry>::new(RING_BUFFER_SIZE);
        let (producer, consumer) = ring_buffer.split();

        let producer = Arc::new(std::sync::Mutex::new(producer));

        // Spawn consumer thread
        let consumer_handle = std::thread::spawn(move || {
            Self::consumer_thread(consumer);
        });

        (
            Logger { producer },
            consumer_handle,
        )
    }

    /// Log a quote written to SHM (minimal overhead for hot-path)
    pub fn log_quote_written(
        &self,
        symbol: String,
        symbol_id: u64,
        bid: i64,
        ask: i64,
        ts_us: i64,
    ) {
        let entry = LogEntry::QuoteWritten {
            symbol,
            symbol_id,
            bid,
            ask,
            ts_us,
        };
        self.push_entry(entry);
    }

    /// Log connection event
    pub fn log_connection(&self, event: &str, url: &str) {
        let entry = LogEntry::ConnectionEvent {
            event: event.to_string(),
            url: url.to_string(),
        };
        self.push_entry(entry);
    }

    /// Log error
    pub fn log_error(&self, message: &str, context: &str) {
        let entry = LogEntry::Error {
            message: message.to_string(),
            context: context.to_string(),
        };
        self.push_entry(entry);
    }

    /// Log warning
    pub fn log_warning(&self, message: &str) {
        let entry = LogEntry::Warning {
            message: message.to_string(),
        };
        self.push_entry(entry);
    }

    /// Log info
    pub fn log_info(&self, message: &str) {
        let entry = LogEntry::Info {
            message: message.to_string(),
        };
        self.push_entry(entry);
    }

    /// Send shutdown signal to consumer thread
    pub fn shutdown(&self) {
        self.push_entry(LogEntry::Shutdown);
    }

    /// Push entry to ring buffer (non-blocking)
    fn push_entry(&self, entry: LogEntry) {
        if let Ok(mut producer) = self.producer.try_lock() {
            // Try to push, if buffer is full, drop the log entry (acceptable for logging)
            let _ = producer.try_push(entry);
        }
        // If we can't get the lock, drop the log entry to avoid blocking
    }

    /// Consumer thread that reads from ring buffer and writes to SPSC ring buffer
    fn consumer_thread(mut consumer: ringbuf::HeapCons<LogEntry>) {
        // Open existing SPSC ring buffer
        let spsc_writer = match SpscWriter::open(SPSC_PATH) {
            Ok(writer) => {
                eprintln!("[LOGGER INFO] SPSC ring buffer opened at {}", SPSC_PATH);
                Some(writer)
            }
            Err(e) => {
                eprintln!("[LOGGER ERROR] Failed to open SPSC ring buffer {}: {}", SPSC_PATH, e);
                eprintln!("[LOGGER INFO] Logging to stderr only");
                None
            }
        };

        loop {
            // Wait for entries (non-blocking poll)
            if let Some(entry) = consumer.try_pop() {
                match entry {
                    LogEntry::Shutdown => {
                        eprintln!("[LOGGER INFO] Shutdown signal received");
                        break;
                    }
                    _ => {
                        let log_line = Self::format_log_entry(&entry);

                        // Write to SPSC ring buffer
                        if let Some(ref writer) = spsc_writer {
                            let msg = SpscWriter::create_message(&log_line);
                            if let Err(e) = writer.publish(&msg) {
                                eprintln!("[LOGGER ERROR] Failed to publish to SPSC: {}", e);
                            }
                        }

                        // Also write critical events to stderr
                        match entry {
                            LogEntry::Error { .. } | LogEntry::Warning { .. } => {
                                eprintln!("{}", log_line);
                            }
                            _ => {}
                        }
                    }
                }
            } else {
                // No data, sleep briefly to avoid busy-wait
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
        }
    }

    /// Format log entry as a string
    fn format_log_entry(entry: &LogEntry) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        match entry {
            LogEntry::QuoteWritten {
                symbol,
                symbol_id,
                bid,
                ask,
                ts_us,
            } => {
                format!(
                    "[{}] [QUOTE] symbol={} symbol_id={} bid={} ask={} ts_us={}",
                    timestamp, symbol, symbol_id, bid, ask, ts_us
                )
            }
            LogEntry::ConnectionEvent { event, url } => {
                format!("[{}] [CONNECTION] {} url={}", timestamp, event, url)
            }
            LogEntry::Error { message, context } => {
                format!("[{}] [ERROR] {} context={}", timestamp, message, context)
            }
            LogEntry::Warning { message } => {
                format!("[{}] [WARNING] {}", timestamp, message)
            }
            LogEntry::Info { message } => {
                format!("[{}] [INFO] {}", timestamp, message)
            }
            LogEntry::Shutdown => {
                format!("[{}] [SHUTDOWN]", timestamp)
            }
        }
    }
}

impl Clone for Logger {
    fn clone(&self) -> Self {
        Logger {
            producer: Arc::clone(&self.producer),
        }
    }
}

unsafe impl Send for Logger {}
unsafe impl Sync for Logger {}

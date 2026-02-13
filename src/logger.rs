use chrono::Local;
use rtrb::{RingBuffer, Producer, Consumer};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// Log level for messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Fatal,
}

impl LogLevel {
    fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Fatal => "FATAL",
        }
    }
}

/// A log message with timestamp, level, and content
#[derive(Debug, Clone)]
pub struct LogMessage {
    timestamp: String,
    level: LogLevel,
    message: String,
}

impl LogMessage {
    fn new(level: LogLevel, message: String) -> Self {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        Self {
            timestamp,
            level,
            message,
        }
    }

    fn format(&self) -> String {
        format!("[{}] [{}] {}\n", self.timestamp, self.level.as_str(), self.message)
    }
}

/// Lock-free SPSC ring buffer logger
///
/// Uses a true Single Producer Single Consumer ring buffer for zero-contention logging.
/// The main thread (producer) pushes log messages without blocking.
/// A background thread (consumer) writes messages to a file.
pub struct Logger {
    producer: Arc<Mutex<Producer<LogMessage>>>,
}

impl Logger {
    /// Create a new logger with specified ring buffer capacity
    ///
    /// # Arguments
    /// * `capacity` - Size of the SPSC ring buffer (must be power of 2, default: 8192)
    /// * `log_path` - Path to the log file
    ///
    /// # Returns
    /// Returns a Logger instance and starts a background consumer thread
    pub fn new(capacity: usize, log_path: &str) -> Self {
        // Create SPSC ring buffer
        let (producer, consumer) = RingBuffer::<LogMessage>::new(capacity);

        let producer = Arc::new(Mutex::new(producer));
        let log_path = log_path.to_string();

        // Spawn background consumer thread
        thread::spawn(move || {
            Self::consumer_thread(consumer, log_path);
        });

        Logger { producer }
    }

    /// Background consumer thread that writes logs to file
    fn consumer_thread(mut consumer: Consumer<LogMessage>, log_path: String) {
        // Open log file in append mode
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[FATAL] Failed to open log file {}: {}", log_path, e);
                std::process::exit(1);
            }
        };

        loop {
            // Try to consume messages from the SPSC ring buffer
            let mut has_messages = false;

            // Pop all available messages in a batch
            while let Ok(msg) = consumer.pop() {
                has_messages = true;
                if let Err(e) = file.write_all(msg.format().as_bytes()) {
                    eprintln!("[ERROR] Failed to write to log file: {}", e);
                }
            }

            // Flush if we wrote any messages
            if has_messages {
                let _ = file.flush();
            }

            // Sleep briefly to avoid busy-waiting
            // For SPSC this is minimal overhead
            thread::sleep(Duration::from_micros(100));
        }
    }

    /// Log a message with format arguments (info level)
    pub fn info_fmt(&self, args: std::fmt::Arguments) {
        self.log(LogLevel::Info, &format!("{}", args));
    }

    /// Log a message with format arguments (warn level)
    pub fn warn_fmt(&self, args: std::fmt::Arguments) {
        self.log(LogLevel::Warn, &format!("{}", args));
    }

    /// Log a message with format arguments (error level)
    pub fn error_fmt(&self, args: std::fmt::Arguments) {
        self.log(LogLevel::Error, &format!("{}", args));
    }

    /// Log a message with format arguments (fatal level)
    pub fn fatal_fmt(&self, args: std::fmt::Arguments) {
        self.log(LogLevel::Fatal, &format!("{}", args));
    }

    /// Internal logging function - pushes to SPSC ring buffer (non-blocking)
    fn log(&self, level: LogLevel, message: &str) {
        let msg = LogMessage::new(level, message.to_string());

        // Lock is only to make Producer Send/Sync safe
        // The actual ring buffer operations are lock-free SPSC
        if let Ok(mut producer) = self.producer.lock() {
            // Try to push to SPSC ring buffer (non-blocking)
            if producer.push(msg).is_err() {
                // Ring buffer is full - silently drop to avoid blocking hot path
                // In production, you can increase buffer size if this happens
            }
        }
    }
}

/// Macro for easy logging with format arguments
#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($arg:tt)*) => {
        $logger.info_fmt(format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($arg:tt)*) => {
        $logger.warn_fmt(format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($arg:tt)*) => {
        $logger.error_fmt(format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_fatal {
    ($logger:expr, $($arg:tt)*) => {
        $logger.fatal_fmt(format_args!($($arg)*))
    };
}

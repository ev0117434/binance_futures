use chrono::Local;
use crossbeam_queue::ArrayQueue;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
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
/// The main thread (producer) pushes log messages without blocking.
/// A background thread (consumer) writes messages to a file.
pub struct Logger {
    queue: Arc<ArrayQueue<LogMessage>>,
}

impl Logger {
    /// Create a new logger with specified ring buffer capacity
    ///
    /// # Arguments
    /// * `capacity` - Size of the ring buffer (default: 8192 messages)
    /// * `log_path` - Path to the log file
    ///
    /// # Returns
    /// Returns a Logger instance and starts a background consumer thread
    pub fn new(capacity: usize, log_path: &str) -> Self {
        let queue = Arc::new(ArrayQueue::new(capacity));
        let queue_clone = Arc::clone(&queue);
        let log_path = log_path.to_string();

        // Spawn background consumer thread
        thread::spawn(move || {
            Self::consumer_thread(queue_clone, log_path);
        });

        Logger { queue }
    }

    /// Background consumer thread that writes logs to file
    fn consumer_thread(queue: Arc<ArrayQueue<LogMessage>>, log_path: String) {
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
            // Try to consume messages from the queue
            let mut has_messages = false;

            while let Some(msg) = queue.pop() {
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
            thread::sleep(Duration::from_micros(100));
        }
    }

    /// Log an info message (non-blocking)
    pub fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }

    /// Log a warning message (non-blocking)
    pub fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message);
    }

    /// Log an error message (non-blocking)
    pub fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
    }

    /// Log a fatal message (non-blocking)
    pub fn fatal(&self, message: &str) {
        self.log(LogLevel::Fatal, message);
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

    /// Internal logging function
    fn log(&self, level: LogLevel, message: &str) {
        let msg = LogMessage::new(level, message.to_string());

        // Try to push to queue (non-blocking)
        if self.queue.push(msg).is_err() {
            // Queue is full - in production, you might want to handle this
            // For now, we silently drop the message to avoid blocking
            // Alternative: use a larger buffer or implement backpressure
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

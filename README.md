# Binance Futures Writer

Rust-реализация writer'а для записи котировок Binance USDⓈ-M Futures в Shared Memory.

## Описание

Программа подключается к Binance Futures WebSocket API, получает best bid/ask цены по заданным символам и записывает их в shared memory таблицу с использованием seqlock протокола для lock-free чтения.

## Особенности

- Lock-free запись в SHM с использованием seqlock
- SPSC ring buffer для асинхронного логирования в shared memory
- Поддержка до 512 символов на одно WebSocket соединение
- Автоматический reconnect с exponential backoff
- Использование локального времени (UNIX microseconds)
- Точный парсинг decimal чисел без float ошибок
- Минимальная задержка записи

## Требования

- Rust 1.70+
- Shared memory файл `/dev/shm/quotes_v1.dat` должен быть инициализирован
- Файлы конфигурации:
  - `/root/siro/dictionaries/subscribe/binance/binance_futures.txt` - список символов для подписки
  - `/root/siro/dictionaries/configs/symbols.tsv` - маппинг symbol_id <-> SYMBOL

## Сборка

```bash
cargo build --release
```

Бинарный файл будет создан в `target/release/binance_futures_writer`

## Использование

### Базовый запуск
```bash
./target/release/binance_futures_writer
```

### Запуск с CPU affinity (рекомендуется для production)
```bash
# Закрепить на конкретном CPU core (например, CPU 0)
taskset -c 0 ./target/release/binance_futures_writer

# Или на P-core для лучшей производительности
taskset -c 2 ./target/release/binance_futures_writer
```

### Запуск как systemd service

Создайте файл `/etc/systemd/system/binance-futures-writer.service`:

```ini
[Unit]
Description=Binance Futures Writer
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/home/user/binance_futures
ExecStart=/usr/bin/taskset -c 0 /home/user/binance_futures/target/release/binance_futures_writer
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Управление службой:
```bash
sudo systemctl daemon-reload
sudo systemctl enable binance-futures-writer
sudo systemctl start binance-futures-writer
sudo systemctl status binance-futures-writer

# Просмотр логов
sudo journalctl -u binance-futures-writer -f
```

Программа автоматически:
1. Загружает список символов из файлов конфигурации
2. Валидирует заголовок SHM файла
3. Подключается к Binance Futures WebSocket
4. Начинает записывать котировки в SHM

## Логирование

Программа использует **правильный SPSC ring buffer** в shared memory для асинхронного логирования без блокировки hot-path:

- **Ring buffer путь**: `/dev/shm/ring_spsc_binance_f_log`
- **Размер**: 8192 слота (настраивается `SPSC_RING_SLOTS` в `src/logger.rs`)
- **Размер сообщения**: Фиксированно 256 байт
- **Формат**: Бинарный SPSC ring buffer с правильным ABI
- **Типы событий**:
  - `INFO (1)` - информационные сообщения, включая записанные котировки
  - `WARNING (2)` - предупреждения (отключения, parse failures)
  - `ERROR (3)` - ошибки с контекстом

### Спецификация SPSC Ring Buffer

#### Header (128 байт)
```
magic:        0x53505343 ("SPSC")
abi_version:  1
msg_size:     256
ring_slots:   8192 (степень двойки)
data_offset:  128
write_seq:    AtomicU64
read_seq:     AtomicU64
```

#### Сообщение (256 байт)
```
[0-1]   abi_version: 1
[2-3]   msg_type: 1=Info, 2=Warning, 3=Error
[4-5]   msg_size: 256
[6-7]   reserved
[8-15]  timestamp_us
[16-23] seq
[24-27] payload_len (max 208)
[28-47] reserved
[48-255] payload (208 bytes)
```

### Чтение логов из ring buffer

Используйте утилиту `log_reader` для чтения логов в реальном времени:

```bash
# Собрать log_reader
cargo build --release --bin log_reader

# Читать логи
./target/release/log_reader /dev/shm/ring_spsc_binance_f_log
```

Пример вывода:
```
[1234567890123456] [INFO] Starting binance_futures_writer
[1234567890123457] [INFO] Loaded 100 symbols from /root/siro/dictionaries/configs/symbols.tsv
[1234567890123458] [QUOTE] symbol=BTCUSDT symbol_id=1 bid=4500000000000 ask=4500100000000 ts_us=1234567890123458
[1234567890123459] [WARN] Connection lost: wss://fstream.binance.com/...
```

Также критические события (ERROR, WARNING) дублируются в stderr для быстрого обнаружения проблем.

### Преимущества реализации

- **Lock-free**: SPSC ring buffer без мьютексов
- **Правильный memory ordering**: Release/Acquire для корректной видимости
- **Фиксированный размер**: Ровно 256 байт на сообщение (align 8)
- **ABI версионирование**: Совместимость между reader/writer
- **Publish после memcpy**: Никогда не будет частично записанных сообщений
- **Степень двойки**: Быстрое вычисление индекса через битовую маску

## Формат файлов конфигурации

### binance_futures.txt

Один символ на строку (uppercase):

```
BTCUSDT
ETHUSDT
BNBUSDT
```

### symbols.tsv

TSV формат: `symbol_id<TAB>SYMBOL`

```
1	BTCUSDT
2	ETHUSDT
3	BNBUSDT
```

## SHM формат

### Header (4096 байт)

- magic: "QSHM1\0\0\0"
- version: 1
- header_size: 4096
- record_size: 64
- records_offset: 4096
- price_scale: 100000000 (1e8)
- ts_scale: 1000000 (1e6)
- n_sources, n_symbols, n_records, shm_total_size

### Record (64 байта)

```rust
#[repr(C)]
struct Quote64 {
    seq: AtomicU64,  // seqlock counter
    source_id: u64,  // всегда 1 для Binance
    symbol_id: u64,  // из symbols.tsv
    bid: i64,        // bid * 1e8
    ask: i64,        // ask * 1e8
    ts: i64,         // unix_microseconds
    reserved0: u64,
    reserved1: u64,
}
```

## Exit коды

- `1` - SHM header mismatch / mmap fail
- `2/3` - WebSocket соединение умерло
- `10` - Пришёл неизвестный символ
- `11` - Ошибка записи в slot
- `20` - Символ из subscribe file отсутствует в symbols.tsv

## Производительность

- Запись происходит напрямую в SHM без буферизации
- Используется seqlock для lock-free доступа
- Парсинг decimal без float конвертации
- Минимальные аллокации на hot-path
- SPSC ring buffer для логирования не блокирует основной поток
- Рекомендуется использовать CPU affinity для снижения jitter

### Оптимизация для production

1. **CPU Affinity**: Закрепите процесс на одном P-core
2. **CPU Governor**: Установите `performance` mode
   ```bash
   echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
   ```
3. **Transparent Hugepages**: Отключите для меньшего jitter
   ```bash
   echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
   ```

## Примечания

- Время берётся локальное (SystemTime с UNIX_EPOCH)
- ts_scale = 1e6 (микросекунды), а не 1e8
- Поддержка до 512 символов на WebSocket соединение
- Автоматический reconnect при обрыве соединения

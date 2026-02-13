
# ТЗ: `binance_futures_writer` (Rust)

## 1) Цель

Скрипт подключается к Binance USDⓈ-M Futures WebSocket market streams и пишет **best bid / best ask** по заданному списку символов в Shared Memory таблицу в формате проекта SIRO с минимальной задержкой. Дроп сообщений запрещён.

---

## 2) Входные данные и пути (константы)

- `SUBSCRIBE_FILE` = `/root/siro/dictionaries/subscribe/binance/binance_futures.txt`  
    Формат: 1 символ на строку, например:
    
    ```
    BUSDT
    C98USDT
    CAKEUSDT
    ```
    
- `SYMBOLS_TSV` = `/root/siro/dictionaries/configs/symbols.tsv`  
    Формат: `<symbol_id>\t<SYMBOL>` (SYMBOL uppercase), например:
    
    ```
    13  ALLOUSDT
    14  ANIMEUSDT
    15  APEUSDT
    ```
    
- `SHM_PATH` (путь к mmap-файлу SHM): фиксируется в конфиге/константе проекта (например `/dev/shm/quotes_v1.dat`)
    
- `SOURCE_ID` = `1` (константа)
    

---

## 3) Сетевые требования (Binance Futures WS)

- Endpoint base: `wss://fstream.binance.com`
    
- Режим подключения: **combined URL**
    
    ```
    /stream?streams=s1/s2/.../sN
    ```
    
- Stream для каждого символа из списка:
    
    ```
    <symbol_lower>@bookTicker
    ```
    
- Размер чанка: **512 streams на 1 соединение**  
    Кол-во соединений = `ceil(N/512)`.
    

---

## 4) Требования по SHM (таблица уже создана)

### 4.1 Header проверки при старте

Скрипт обязан mmap’нуть SHM и проверить:

- `magic` соответствует проекту
    
- `header_size == 4096`
    
- `record_size == 64`
    
- `records_offset == 4096`
    
- `price_scale == 1e8`
    
- **`ts_scale == 1e6`**
    
- `shm_total_size` соответствует реальному размеру файла
    
- корректность `n_symbols`, `n_sources`, `n_records` (границы)
    

Если любая проверка не проходит → **немедленный exit(1)**.

### 4.2 Record layout (64 bytes, cache line)

Запись (слот) имеет поля:

- `seq: u64 (atomic)` — seqlock
    
- `source_id: u64`
    
- `symbol_id: u64`
    
- `bid: i64` (scale 1e8)
    
- `ask: i64` (scale 1e8)
    
- `ts: i64` (**monotonic_us**, scale 1e6)
    
- `reserved0: u64`
    
- `reserved1: u64`
    

### 4.3 Индексация слота

Индекс слота:

- `idx = source_id * n_symbols + symbol_id`  
    Адрес:
    
- `ptr = records_base + idx * 64`
    

---

## 5) Правила маппинга символов

1. `symbols.tsv` — источник истины.
    
2. Каждый символ из `binance_futures.txt` обязан существовать в `symbols.tsv`.
    
3. Если хотя бы один символ не найден → **скрипт падает** (exit(20)).
    
4. В `binance_futures.txt` нет пустых строк, дублей и комментариев (допущение ТЗ).
    

---

## 6) Правила обработки сообщений WS

### 6.1 Что брать из payload

Из каждого сообщения bookTicker (в `data`) нужны только:

- `s` — symbol (uppercase)
    
- `b` — bid price (string)
    
- `a` — ask price (string)
    

### 6.2 Конвертация цен

- `bid_i64 = round(bid_price * 1e8)`
    
- `ask_i64 = round(ask_price * 1e8)`  
    Правило округления: **round half-up** (≥5 на следующем знаке → вверх).  
    Использовать **десятичный парсер без float** (предпочтительно), чтобы не было ошибок float и непредсказуемых хвостов.
    

### 6.3 Timestamp

- `ts = ` (локальное время системы в мc)
      

---

## 7) Запись в SHM (seqlock)

Writer должен писать запись атомарно для читателей:

### 7.1 Seqlock протокол (обязателен)

- `seq` чётный → запись целостна
    
- `seq` нечётный → запись в процессе, читать нельзя
    

Алгоритм writer:

1. `s0 = load(seq)`
    
2. `store(seq = s0 + 1)` (делаем нечётным)
    
3. записать `bid`, `ask`, `ts` (и при необходимости константы)
    
4. `store(seq = s0 + 2)` **с Release** (commit, чётный)
    

### 7.2 Инициализация констант слота

`source_id` и `symbol_id` разрешено записать **однократно** при старте (инициализация слотов) и далее не трогать, чтобы уменьшить число store на hot-path.

---

## 8) Архитектура выполнения (минимальная задержка)

Цель: минимальные хвосты задержки, без логов на hot-path.

### 8.1 Ограничение по CPU

- Вся программа закрепляется на **одном P-core** (CPU affinity процесса).
    
- Внутри одного потока обслуживаются все соединения (multiplex).
    

### 8.2 Без очередей/каналов

- Нельзя использовать внутренние очереди обработки (mpsc), чтобы не создавать бэклог и дополнительные копии.
    
- На каждый фрейм: parse → convert → write SHM сразу.
    

### 8.3 Fairness между соединениями

При нескольких соединениях (для 1000 символов это 2 conns) нужно избежать голодания:

- ограничение “burst limit”: не более `K` сообщений подряд с одного соединения перед переключением на другое (`K` = 256 по умолчанию).
    

---

## 9) Запреты и ограничения

- **Дроп сообщений запрещён**: скрипт не должен сознательно пропускать сообщения.
    
- Hot-path не должен делать:
    
    - файловый I/O
        
    - stdout/stderr logging
        
    - heap allocations (желательно), особенно per-message
        
- Если соединение упало → reconnect с backoff, но без спама.
    

---

## 10) Надёжность и reconnect

- Автоподдержка ping/pong (если библиотека не делает — реализовать)
    
- Rolling reconnect до лимита 24h (без простоя):
    
    1. открыть новое соединение
        
    2. убедиться что получаем сообщения
        
    3. закрыть старое
        
- Backoff стратегия при ошибках (пример):
    
    - 200ms → 500ms → 1s → 2s → … cap 30s
        

---

## 12) Выходы/коды завершения (строго)

- `exit(1)` — SHM header mismatch / mmap fail
    
- `exit(2/3)` — ws соединение умерло и не восстановилось (фатально)
    
- `exit(10)` — пришёл symbol, которого нет в map (не ожидается)
    
- `exit(11)` — slot_ptr null / symbol_id вне диапазона
    
- `exit(20)` — subscribe symbol отсутствует в symbols.tsv
    
- `exit(30)` — неожиданное число соединений/чанков при фиксированных параметрах
    

---

### 1. WebSocket подключения
- URL: `wss://fstream.binance.com/stream?streams=<symbol1>@bookTicker/<symbol2>@bookTicker/...`
- Формат стрима: `{symbol}@bookTicker` (lowercase)
- Макс. 200 символов на одно соединение
- Разбить на chunks по 100 символов для надежности
- Задержка 1 сек между запуском соединений
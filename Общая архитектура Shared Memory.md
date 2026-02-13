# 1) Общая архитектура Shared Memory

## 1.1. Что это в твоём проекте

У тебя SHM — это **один общий memory-mapped регион** (в `/dev/shm/quotes_v1.dat`), который:
- **инициализируется один раз** init-скриптом,
- затем используется многими процессами:
    
    - **writers**: по одному writer’у на источник (или на поток биржи), пишут котировки в свои слоты,
    - **readers**: любые процессы (агрегатор, стратегия, логгер), читают без блокировок.

## 1.2. Почему фиксированная запись 64 байта

- 64 байта = типичный размер cache line на x86_64.
    
- Каждая котировка занимает **строго одну cache line**, это уменьшает false-sharing и делает доступ предсказуемым.
    

## 1.3. Раскладка памяти (в байтах)

```
[ Header 4096B ]  (offset 0)
[ Records ... ]   (offset 4096)
```

- Header: метаданные формата и размеров (magic, version, record_size, counts).
    
- Records: плотный массив слотов, каждый слот 64 байта.
    

## 1.4. Индексация слота

Слот определяется парой `(source_id, symbol_id)`:

```
idx = source_id * n_symbols + symbol_id
offset = records_offset + idx * record_size
```

Это O(1) и идеально для быстрого доступа.

---

# 2) Формат данных

## 2.1. Header (минимальный)

Хранит только то, что нужно для корректного маппинга и навигации:

- `magic[8] = "QSHM1\0\0\0"`
    
- `version (u64) = 1`
    
- `header_size (u64) = 4096`
    
- `record_size (u64) = 64`
    
- `records_offset (u64) = 4096`
    
- `price_scale (u64) = 1e8`
    
- `ts_scale (u64) = 1e8`
    
- `n_sources (u64)`
    
- `n_symbols (u64)`
    
- `n_records (u64)`
    
- `shm_total_size (u64)`
    

Все little-endian.

## 2.2. Record (64 байта)

8 полей по 8 байт:

|Поле|Тип|Назначение|
|---|---|---|
|seq|u64|seqlock-счётчик|
|source_id|u64|ID источника|
|symbol_id|u64|ID символа|
|bid|i64|цена * 1e8|
|ask|i64|цена * 1e8|
|ts|i64|unix * 1e8|
|reserved0|u64|0|
|reserved1|u64|0|

---

# 3) Протокол консистентности: seqlock (seq чёт/нечёт)

## 3.1. Зачем нужен seq

Без него reader мог бы увидеть “полузаписанную” структуру (например, bid новый, ask старый).

Seqlock даёт:

- writers пишут без mutex,
    
- readers читают без mutex,
    
- консистентность достигается повтором чтения при гонке.
    

## 3.2. Writer: правило записи

1. прочитать `seq` (чётный)
    
2. сделать `seq` нечётным (`seq+1`) → “пишу”
    
3. записать поля
    
4. сделать `seq` чётным (`seq+2`) → “готово”
    

Важно: `seq` должен быть атомарным 64-bit и выровнен по 8 байтам (у тебя так).

## 3.3. Reader: правило чтения

1. прочитать `s1 = seq`
    
2. если `s1` нечётный → retry
    
3. прочитать все поля
    
4. прочитать `s2 = seq`
    
5. если `s1 != s2` → retry
    
6. иначе запись консистентна
    

---

# 4) Гайд для кодера: общие правила

## 4.1. Типы и scale

- `bid/ask` храним как `i64` scaled:
    
    - `bid_i64 = round(bid_float * 1e8)`
        
    - `bid_float = bid_i64 / 1e8`
        
- `ts`:
    
    - `ts_i64 = round(unix_seconds * 1e8)`
        
    - `unix_seconds = ts_i64 / 1e8`
        

## 4.2. Кто что пишет

- Один слот `(source_id, symbol_id)` должен иметь **ровно одного writer’а**.
    
- Разные источники могут писать параллельно в разные области массива — это хорошо.
    
## 4.3. Нельзя менять header

Header неизменяемый после init.

## 4.4. Проверки при старте процесса

Каждый writer/reader при открытии должен:

- проверить `magic`, `version`, `record_size`, `records_offset`
    
- проверить, что файл/сегмент размера `shm_total_size`
    

---

# 5) Python гайд (код-скелет)

## 5.1. Открытие и чтение header

```python
import mmap, os, struct

HDR = struct.Struct("<8s" + "Q"*10)     # magic + 10 u64
REC = struct.Struct("<QQQqqqQQ")        # 64B

path = "/dev/shm/quotes_v1.dat"
fd = os.open(path, os.O_RDONLY)
st = os.fstat(fd)
mm = mmap.mmap(fd, st.st_size, access=mmap.ACCESS_READ)

magic, version, header_size, record_size, records_offset, price_scale, ts_scale, n_sources, n_symbols, n_records, shm_total_size = HDR.unpack_from(mm, 0)
assert magic == b"QSHM1\x00\x00\x00"
```

## 5.2. Чтение одного слота (seqlock)

```python
def offset(source_id, symbol_id):
    idx = source_id * n_symbols + symbol_id
    return records_offset + idx * record_size

def read_quote(source_id, symbol_id, retries=100000):
    off = offset(source_id, symbol_id)
    for _ in range(retries):
        s1 = struct.unpack_from("<Q", mm, off)[0]
        if s1 & 1:
            continue
        seq, sid, sym, bid_i, ask_i, ts_i, r0, r1 = REC.unpack_from(mm, off)
        s2 = struct.unpack_from("<Q", mm, off)[0]
        if s1 != s2:
            continue
        return {
            "seq": seq,
            "source_id": sid,
            "symbol_id": sym,
            "bid": bid_i / price_scale,
            "ask": ask_i / price_scale,
            "ts": ts_i / ts_scale,
        }
    raise RuntimeError("no stable read")
```

## 5.3. Запись слота (важно: один writer на слот)

Python не даёт строгих atomic memory order гарантий, но на x86 обычно работает. Для production лучше writer на Rust/C++.

Тем не менее скелет:

```python
import time

def write_quote(source_id, symbol_id, bid, ask):
    off = offset(source_id, symbol_id)
    seq = struct.unpack_from("<Q", mmw, off)[0]
    mmw[off:off+8] = struct.pack("<Q", seq+1)  # odd

    bid_i = int(round(bid * price_scale))
    ask_i = int(round(ask * price_scale))
    ts_i  = int(round(time.time() * ts_scale))

    struct.pack_into("<QQqqqQQ", mmw, off+8, source_id, symbol_id, bid_i, ask_i, ts_i, 0, 0)

    mmw[off:off+8] = struct.pack("<Q", seq+2)  # even
```

---

# 6) Rust гайд (правильный путь для writer’ов)

## 6.1. Ключевые цели в Rust

- гарантировать **atomic u64** для `seq`
- использовать **Acquire/Release** барьеры
- структура должна быть **repr(C)** и **ровно 64 байта**

## 6.2. Структура записи

```rust
#[repr(C)]
pub struct Quote64 {
    pub seq: std::sync::atomic::AtomicU64, // 8
    pub source_id: u64,                    // 8
    pub symbol_id: u64,                    // 8
    pub bid: i64,                          // 8
    pub ask: i64,                          // 8
    pub ts: i64,                           // 8
    pub reserved0: u64,                    // 8
    pub reserved1: u64,                    // 8
}

const _: () = assert!(std::mem::size_of::<Quote64>() == 64);
```
## 6.3. Write (Release)

```rust
use std::sync::atomic::Ordering;

pub fn write_quote(q: &Quote64, source_id: u64, symbol_id: u64, bid: i64, ask: i64, ts: i64) {
    let seq0 = q.seq.load(Ordering::Relaxed);
    q.seq.store(seq0.wrapping_add(1), Ordering::Release); // odd

    unsafe {
        let ptr = q as *const Quote64 as *mut Quote64;
        (*ptr).source_id = source_id;
        (*ptr).symbol_id = symbol_id;
        (*ptr).bid = bid;
        (*ptr).ask = ask;
        (*ptr).ts = ts;
        (*ptr).reserved0 = 0;
        (*ptr).reserved1 = 0;
    }

    q.seq.store(seq0.wrapping_add(2), Ordering::Release); // even
}
```

## 6.4. Read (Acquire)

```rust
pub fn read_quote(q: &Quote64) -> Option<(u64,u64,i64,i64,i64,u64)> {
    let s1 = q.seq.load(Ordering::Acquire);
    if (s1 & 1) == 1 {
        return None;
    }

    let sid = q.source_id;
    let sym = q.symbol_id;
    let bid = q.bid;
    let ask = q.ask;
    let ts  = q.ts;

    let s2 = q.seq.load(Ordering::Acquire);
    if s1 != s2 {
        return None;
    }

    Some((sid, sym, bid, ask, ts, s2))
}
```

## 6.5. mmap в Rust

Обычно используют `memmap2`:

- открыть файл `/dev/shm/quotes_v1.dat`
    
- `MmapOptions::new().map_mut(&file)`
    
- распарсить header
    
- вычислить offset
    
- получить `&Quote64` через `unsafe` pointer cast
    

---

# 7) Практические советы

## 7.1. Проверка “живости” данных

Reader может считать запись “устаревшей”, если:

```
(now_i64 - ts_i64) > threshold
```

## 7.2. Выбор writer языка

- Python writer: годится для тестов и прототипа
    
- Rust writer: лучше для продакшена (атомики и memory ordering)
    
## 7.3. Совместимость

Любое изменение record layout или размеров = новая `version` + новый файл/имя shm.

---

Если хочешь — следующим сообщением дам готовый минимальный Rust-проект:

- `cargo init`
    
- `memmap2`
    
- `struct Header + Quote64`
    
- команды `read` и `write` как CLI, полностью совместимые с твоими Python-скриптами.
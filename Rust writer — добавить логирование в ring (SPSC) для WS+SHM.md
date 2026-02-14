## 1) Цель

В существующий Rust-скрипт (WS Binance Futures → преобразование → запись в SHM таблицу) добавить логирование состояния и метрик в SPSC ring, сообщения фиксированные 256 байт.

## 2) Что добавить в код (структуры/модули)

### 2.1 RingWriter

Компонент, который:

- открывает shm/mmap ring по `path`
    
- валидирует header:
    
    - `magic == 0x53505343`
        
    - `abi_version == 1`
        
    - `msg_size == 256`
        
    - `ring_slots` степень двойки
        
- хранит:
    
    - `ring_mask`
        
    - `data_off`
        
    - указатель/срез на data region
        
    - указатель на `write_seq` (в header)
        

### 2.2 Функция `publish(msg: [u8;256])`

Действия строго по шагам:

1. `seq = atomic_load(write_seq)`
    
2. `idx = seq & ring_mask`
    
3. `slot_ptr = data_off + idx*256`
    
4. memcpy 256 байт в слот
    
5. `atomic_store(write_seq, seq+1)` (Release)
    
6. без блокировок; при ошибке mmap/segfault — завершение с ошибкой
    

## 3) Встроить “точки измерений” (timestamps)

Везде использовать **unix ms**:

- `now_ms()` возвращает u64 unix ms
    
- На каждое обработанное market data событие:
    
    - `t_rx_ms` — как только payload получен из websocket read
        
    - `t_parsed_ms` — после JSON parse
        
    - `t_mapped_ms` — после маппинга в internal record (поиск symbol_id/source_id и подготовка SHM записи)
        
    - `t_written_ms` — сразу после успешной записи в SHM
        

Latency:

- `rx_to_written = t_written_ms - t_rx_ms`
    
- `rx_to_parsed = t_parsed_ms - t_rx_ms`
    
- `parsed_to_mapped = t_mapped_ms - t_parsed_ms`
    
- `mapped_to_written = t_written_ms - t_mapped_ms`
    

## 4) Метрики: что считать и как обновлять (подробнее)

Все метрики делятся на:

- totals (нарастающие, u64)
    
- deltas за период (рассчитываются при формировании periodic snapshot)
    
- “последние времена” (last_*_ms)
    

### 4.1 WS liveness (обновления)

При каждом recv фрейма:

- `ws_last_frame_rx_ms = now_ms`
    
- `ws_rx_frames_total += 1`
    
- `ws_rx_bytes_total += frame_len`
    

При каждом принятом market data сообщении:

- `ws_last_data_rx_ms = now_ms`
    
- `ws_rx_msgs_total += 1`
    

Состояния:

- `ws_state` обновлять по факту этапа (connecting/handshake/subscribed/running/reconnecting)
    
- `session_id += 1` при каждом новом подключении
    
- `session_start_ms = now_ms` при старте сессии
    

Reconnect/error totals:

- при reconnect: `ws_reconnect_total += 1`
    
- при disconnect: `ws_disconnect_total += 1`
    
- при handshake fail: `ws_handshake_fail_total += 1`
    
- при subscribe ok/fail: соответствующие totals
    
- parse error: `ws_parse_error_total += 1`
    
- protocol error: `ws_protocol_error_total += 1`
    
- last codes:
    
    - `ws_last_error_code = i32`
        
    - `ws_last_error_ms = now_ms`
        
    - `ws_last_close_code = u16`
        

### 4.2 SHM write (обновления)

После попытки записи в SHM:

- при успехе:
    
    - `shm_write_ok_total += 1`
        
    - `shm_last_write_ok_ms = now_ms`
        
- при провале:
    
    - `shm_write_fail_total += 1`
        
    - `shm_last_write_fail_ms = now_ms`
        
    - обновить `ws_last_error_code` (или отдельный `shm_last_error_code`, если хочешь — но в сообщении уже есть last_error_code)
        

### 4.3 Latency агрегатор за период (lat_rx_to_written)

Нужно хранить в runtime:

- `lat_count: u32`
    
- `lat_min_ms: u32` (init = u32::MAX)
    
- `lat_max_ms: u32` (init = 0)
    
- `lat_sum_ms: u64`
    

На каждом сообщении:

- `lat_count += 1`
    
- `lat_min_ms = min(lat_min_ms, value)`
    
- `lat_max_ms = max(lat_max_ms, value)`
    
- `lat_sum_ms += value`
    

При формировании snapshot:

- `avg_ms = (lat_sum_ms / lat_count) as u32` (если count>0)
    
- после публикации snapshot — сбросить агрегатор на новый период
    

## 5) Сообщения, которые писать в ring

Writer обязан публиковать 3 вида сообщений (каждое 256 байт):

1. `SNAPSHOT_START` — один раз при старте, после успешного открытия ring и до RUNNING
    
2. `SNAPSHOT_PERIODIC` — каждые `N_ms` (напр. 1000 или 5000)
    
3. `EVENT_ERROR` — сразу при событиях:
    
    - reconnect
        
    - subscribe_fail
        
    - handshake_fail
        
    - shm_write_fail
        

## 6) Содержание сообщений (строго)

Использовать ABI, который ты уже принял:

- Header: `abi_version=1`, `msg_size=256`, `layout_version` (из ring header)
    
- `ts_ms` — время формирования сообщения
    
- `ws_state`, `session_id`, `pid`, `seq` (счётчик сообщений логгера)
    

### 6.1 SNAPSHOT_START payload

Заполнить поля:

- `start_ms`
    
- `build_version_hash`
    
- `symbols_count`
    
- `stream_type = BOOK_TICKER`
    
- `ring_size`, `msg_size=256`
    
- `ws_url_hash`
    
- `totals` (текущие totals)
    
- `last_frame_rx_ms`, `last_data_rx_ms`
    
- `last_error_code`, `last_close_code`
    

### 6.2 SNAPSHOT_PERIODIC payload

Заполнить:

- `session_start_ms`
    
- `last_frame_rx_ms`, `last_data_rx_ms`
    
- `uptime_ms = ts_ms - session_start_ms`
    
- `silence_ms = ts_ms - last_data_rx_ms` (если last_data=0 → silence=ts_ms)
    
- deltas за период:
    
    - `rx_msgs_delta`
        
    - `reconnect_delta`
        
    - `subscribe_fail_delta`
        
    - `parse_error_delta`
        
    - `protocol_error_delta`
        
    - `shm_write_ok_delta`
        
    - `shm_write_fail_delta`
        
- `shm_last_write_ok_ms`, `shm_last_write_fail_ms`
    
- latency agg:
    
    - `lat_rx_to_written` (count/min/max/avg)
        
- `last_error_code`, `last_close_code`
    

### 6.3 EVENT_ERROR payload

Заполнить:

- `domain` (WS/SHM/SYS)
    
- `code` (i16)
    
- `aux_u32` (например close code / errno)
    
- `aux_u64_0` (например hash(reason))
    
- `aux_u64_1` (например session_id или 0)
    
- `last_frame_rx_ms`, `last_data_rx_ms`
    

## 7) Таймер periodic snapshot

Добавить в main loop:

- `next_snapshot_ms`
    
- если `now_ms >= next_snapshot_ms`:
    
    - сформировать snapshot_periodic
        
    - publish
        
    - `next_snapshot_ms += N_ms`
        

## 8) Логика дельт

Хранить “предыдущие totals на момент прошлого снапшота”:

- `prev_rx_msgs_total`, `prev_reconnect_total`, `prev_subscribe_fail_total`, …  
    На snapshot:
    
- delta = total - prev_total
    
- prev_total = total
    

## 9) Требования к производительности

- Никаких аллокаций на hot path публикации (сообщение формировать в stack `[u8;256]` или `SpscMsg256`)
    
- publish без блокировок
    
- ошибки открытия ring → фатальная ошибка запуска (exit)
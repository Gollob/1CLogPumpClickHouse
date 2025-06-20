# 1CLogPumpClickHouse

Сервис для чтения технологических логов 1С и передачи их в ClickHouse.

---

## 📌 Основные возможности

- **Автоматический мониторинг лог-файлов 1С**
  Следит за папками из `logcfg.xml`, «дожидается» появления новых файлов и строк (использует `hpcloud/tail` и `fsnotify`).

- **Парсинг строк лога**
  Разбирает каждую запись 1С на поля: время, компонент, уровень, процесс, пользователь, SQL-запрос, контекст и пр. Многострочные записи объединяет в единый объект `LogEntry`.

- **Пакетная отправка в ClickHouse**
  Накопление до `BatchSize` или по таймеру `FlushIntervalSec`, затем единым INSERT’ом в ClickHouse (Go-клиент `clickhouse-go/v2`).

- **Гибкая настройка через XML**
  Параметры сервиса, пути к логам и соответствие компонент→таблиц задаются в `config.xml` и `logcfg.xml`.

- **Корректировка временных меток**
  Дата берётся из имени файла (например, `25052607.log` → `2025-05-26`), а время события из содержимого лога.

- **Безопасное завершение работы**
  При SIGINT/SIGTERM дожидается отправки всех накопленных записей и корректно завершает соединение с БД.

---

## 📋 Требования

- **Go** ≥ 1.18  
- **ClickHouse** ≥ 20.x с правами на INSERT  
- **ОС**: Linux или Windows  
- **Зависимости**:  
  - `go.uber.org/zap`  
  - `github.com/fsnotify/fsnotify`  
  - `github.com/hpcloud/tail`  
  - `github.com/ClickHouse/clickhouse-go/v2`

---

## 🛠 Установка

1. Клонировать репозиторий:  
   ```bash
   git clone https://github.com/Gollob/1CLogPumpClickHouse.git
   cd 1CLogPumpClickHouse
   ```
2. Собрать бинарник:  
   ```bash
   go build -o 1c_log_pump_clickhouse main.go
   ```

---

## ⚙️ Настройка

1. Отредактируйте `config.xml` (пример ниже) — пути, параметры ClickHouse, BatchSize, FlushIntervalSec.  
2. Подготовьте `logcfg.xml` с описанием директорий и шаблонов лог-файлов.  

**Пример `config.xml`:**
```xml
<Configuration>
  <LogCfgPath>./logcfg.xml</LogCfgPath>
  <BatchSize>100</BatchSize>
  <FlushIntervalSec>60</FlushIntervalSec>
  <ClickHouse>
    <Address>localhost:9000</Address>
    <Username>default</Username>
    <Password></Password>
    <Database>logs_db</Database>
    <DefaultTable>tech_logs</DefaultTable>
    <TableMap>
      <Map Component="SQL" Table="tech_logs_sql"/>
      <Map Component="EXCEPTION" Table="tech_logs_exceptions"/>
    </TableMap>
  </ClickHouse>
</Configuration>
```

---

## ▶️ Запуск

```bash
./1c_log_pump_clickhouse
```

После старта сервис подключится к ClickHouse, загрузит конфигурацию и начнёт в реальном времени отправлять новые записи из логов 1С.

---

## 🔍 Структура проекта

```
.
├── batch/              # Логика накопления и отправки батчей
├── clickhouseclient/   # Клиент для ClickHouse
├── config/             # Чтение и валидация XML-конфигураций
├── logger/             # Инициализация zap-логгера
├── models/             # Описание структуры LogEntry
├── transform/          # Вспомогательные функции (дата, время)
├── watcher/            # Наблюдатель за файлами (tail + fsnotify)
└── main.go             # Точка входа
```

---

## 🤝 Структура таблицы в ClickHouse
```
CREATE TABLE logs (
    EventDate      Date,
    EventTime      DateTime64(6),
    EventType      LowCardinality(String),
    Duration       UInt32,
    User           String,
    InfoBase       String,
    SessionID      UInt32,
    ClientID       UInt32,
    ConnectionID   UInt32,
    ExceptionType  Nullable(String),
    ErrorText      Nullable(String),
    SQLText        Nullable(String),
    Rows           Nullable(Int32),
    RowsAffected   Nullable(Int32),
    Context        Nullable(String),
    ProcessName    String
)
ENGINE = MergeTree
PARTITION BY EventDate
ORDER BY (EventDate, EventTime);
```

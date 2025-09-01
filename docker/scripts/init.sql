-- Создание базы данных
CREATE DATABASE IF NOT EXISTS JDF_DATABASE;

-- Сырая таблица для JSON данных с дедупликацией по содержимому JSON
CREATE TABLE IF NOT EXISTS JDF_DATABASE.RAW_TABLE
(
    `raw_json` String,
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (raw_json);  

-- Таблица для распарсенных данных с дедупликацией по имени и кораблю
CREATE TABLE IF NOT EXISTS JDF_DATABASE.PARSED_TABLE
(
    `craft` String,
    `name` String,
    `_inserted_at` DateTime
)
ENGINE = ReplacingMergeTree(_inserted_at)
ORDER BY (name, craft);

-- Materialized View для автоматического парсинга JSON и дедупликации
CREATE MATERIALIZED VIEW IF NOT EXISTS JDF_DATABASE.mv
TO JDF_DATABASE.PARSED_TABLE 
AS
SELECT
    JSONExtractString(raw_json, 'craft') AS craft,
    JSONExtractString(raw_json, 'name') AS name,
    inserted_at AS _inserted_at
FROM JDF_DATABASE.RAW_TABLE;

-- Представление для просмотра дедублицированных данных (последняя версия каждого астронавта)
CREATE VIEW IF NOT EXISTS JDF_DATABASE.PARSED_TABLE_latest AS
SELECT *
FROM JDF_DATABASE.PARSED_TABLE
FINAL;  


SELECT 
    'Database and tables created successfully' as status,
    'JDF_DATABASE' as database_name,
    count() as table_count
FROM system.tables 
WHERE database = 'JDF_DATABASE';
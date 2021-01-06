DROP VIEW IF EXISTS system.constant;
CREATE VIEW system.constant AS SELECT 1;

DROP TABLE IF EXISTS system.10k_rows;
CREATE TABLE system.10k_rows
(
  `id`       Int64,
  `name`     String,
  `datetime` DateTime,
  `num`      Nullable(Int64),
  `value`    Nullable(Float32)
) ENGINE = MergeTree() PARTITION BY toYYYYMM(datetime) ORDER BY (id) SETTINGS index_granularity = 8192;
INSERT INTO 10k_rows SELECT * FROM jdbc('mariadb', '10k_rows');
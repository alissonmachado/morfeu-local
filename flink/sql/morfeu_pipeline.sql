-- =====================================================================
--  morfeu — pipeline de ingestão contínua (Flink SQL)
--  Kafka (JSON)  ->  Iceberg (catálogo REST, particionado por dt_particao)
-- =====================================================================

-- Streaming + checkpoint (o Iceberg comita a cada checkpoint)
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30 s';
SET 'pipeline.name' = 'morfeu-ingestao-topologia';

-- ---------------------------------------------------------------------
-- 1) Catálogo Iceberg via REST (aponta para o iceberg-rest + MinIO)
-- ---------------------------------------------------------------------
CREATE CATALOG morfeu_iceberg WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://iceberg-rest:8181',
  'warehouse'            = 's3://warehouse/',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'password',
  's3.path-style-access' = 'true',
  'client.region'        = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS morfeu_iceberg.morfeu;

-- ---------------------------------------------------------------------
-- 2) Tabela analítica de destino (Iceberg v2, particionada)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS morfeu_iceberg.morfeu.topologia_rede (
  id_evento        STRING,
  tipo_operacao    STRING,         -- CREATE | UPDATE | DELETE
  id_no            STRING,
  tipo_no          STRING,
  id_no_pai        STRING,
  uf               STRING,
  municipio        STRING,
  latitude         DOUBLE,
  longitude        DOUBLE,
  status           STRING,
  timestamp_evento TIMESTAMP(3),
  dt_ingestao      TIMESTAMP(3),   -- metadado de rastreabilidade
  dt_particao      STRING          -- coluna de particionamento (yyyy-MM-dd)
) PARTITIONED BY (dt_particao)
WITH (
  'format-version' = '2'
);

-- ---------------------------------------------------------------------
-- 3) Fonte Kafka (catálogo default in-memory do Flink)
-- ---------------------------------------------------------------------
CREATE TABLE default_catalog.default_database.kafka_topologia (
  id_evento        STRING,
  tipo_operacao    STRING,
  id_no            STRING,
  tipo_no          STRING,
  id_no_pai        STRING,
  uf               STRING,
  municipio        STRING,
  latitude         DOUBLE,
  longitude        DOUBLE,
  status           STRING,
  timestamp_evento TIMESTAMP(3)
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'topologia.eventos',
  'properties.bootstrap.servers'  = 'kafka:9092',
  'properties.group.id'           = 'morfeu-flink',
  'scan.startup.mode'             = 'earliest-offset',
  'format'                        = 'json',
  'json.timestamp-format.standard'= 'ISO-8601',
  'json.ignore-parse-errors'      = 'true'
);

-- ---------------------------------------------------------------------
-- 4) Ingestão contínua: transforma e grava no Iceberg
-- ---------------------------------------------------------------------
INSERT INTO morfeu_iceberg.morfeu.topologia_rede
SELECT
  id_evento,
  tipo_operacao,
  id_no,
  tipo_no,
  id_no_pai,
  uf,
  municipio,
  latitude,
  longitude,
  status,
  timestamp_evento,
  CURRENT_TIMESTAMP                            AS dt_ingestao,
  DATE_FORMAT(timestamp_evento, 'yyyy-MM-dd')  AS dt_particao
FROM default_catalog.default_database.kafka_topologia;

"""
morfeu — carga em lote de datasets analíticos da ANATEL direto no Iceberg.

Diferente do dadosgov_producer (Kafka -> Flink -> Iceberg, pensado para
eventos de topologia), estes datasets são estatísticas agregadas mensais
(acessos e velocidade contratada por operadora/UF/município) e chegam em
arquivos de centenas de MB a poucos GB. Não há motivo para publicar cada
linha como mensagem individual no Kafka: aqui a carga é feita em lote,
direto no catálogo REST do Iceberg via pyiceberg, lendo o CSV em chunks
para não estourar a memória.

Config via variáveis de ambiente:
  LOADER_ZIP_PATH      caminho do .zip (ou .csv direto) montado no container
  LOADER_CSV_MEMBER    nome do .csv dentro do .zip (ignorado se não for zip)
  LOADER_SCHEMA_FILE    JSON [[coluna_origem, coluna_destino, tipo], ...]
  LOADER_TABLE         tabela destino, ex.: morfeu.acessos_banda_larga_fixa
  LOADER_CSV_SEP       separador de campo (padrão ;)
  LOADER_CSV_ENCODING  encoding do CSV (padrão utf-8-sig — ANATEL usa BOM)
  LOADER_CHUNK_SIZE    linhas por lote lido/gravado (padrão 200000)
  LOADER_MAX_ROWS      corta a carga após N linhas (0 = sem limite; usar
                       para testar antes de rodar o arquivo inteiro)
  LOADER_ARCHIVE_ONLY  "true" = só arquiva o raw no MinIO e sai, sem tocar na
                       tabela Iceberg (para arquivar o raw de uma carga que
                       já rodou antes, sem duplicar dados por reprocessar)

Antes de processar, o arquivo original (.zip ou .csv, do jeito que veio da
ANATEL) é copiado para s3://warehouse/raw/<tabela>/<nome_do_arquivo> — é a
auditoria equivalente ao MongoDB da trilha de streaming (audit-consumer),
já que esta trilha em lote nunca passa pelo Kafka. Idempotente: se o objeto
já existir no MinIO, o upload é pulado.
"""
import io
import json
import os
import re
import sys
import zipfile
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, IntegerType, LongType, NestedField, StringType, TimestampType

TYPE_ICEBERG = {
    "int": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "string": StringType(),
}
TYPE_PANDAS = {
    "int": "Int64",
    "long": "Int64",
    "double": None,  # deixa o pandas inferir como float64 (respeita decimal=',')
    "string": "string",
}
TYPE_PYARROW = {
    "int": pa.int32(),
    "long": pa.int64(),
    "double": pa.float64(),
    "string": pa.string(),
}

ZIP_PATH = os.environ["LOADER_ZIP_PATH"]
CSV_MEMBER = os.getenv("LOADER_CSV_MEMBER", "")
SCHEMA_FILE = os.environ["LOADER_SCHEMA_FILE"]
TABLE_NAME = os.environ["LOADER_TABLE"]
CSV_SEP = os.getenv("LOADER_CSV_SEP", ";")
CSV_ENCODING = os.getenv("LOADER_CSV_ENCODING", "utf-8-sig")
CHUNK_SIZE = int(os.getenv("LOADER_CHUNK_SIZE", "200000"))
MAX_ROWS = int(os.getenv("LOADER_MAX_ROWS", "0"))

with open(SCHEMA_FILE, encoding="utf-8") as f:
    COLUMNS = json.load(f)  # [[origem, destino, tipo], ...]

CATALOG_PROPS = {
    "type": "rest",
    "uri": os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
    "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
    "s3.endpoint": os.getenv("S3_ENDPOINT", "http://minio:9000"),
    "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "admin"),
    "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "password"),
    "s3.path-style-access": "true",
    "s3.region": os.getenv("S3_REGION", "us-east-1"),
}


def montar_s3_filesystem() -> pafs.S3FileSystem:
    m = re.match(r"^(https?)://(.+)$", CATALOG_PROPS["s3.endpoint"])
    scheme, host = (m.group(1), m.group(2)) if m else ("http", CATALOG_PROPS["s3.endpoint"])
    return pafs.S3FileSystem(
        access_key=CATALOG_PROPS["s3.access-key-id"],
        secret_key=CATALOG_PROPS["s3.secret-access-key"],
        endpoint_override=host,
        scheme=scheme,
        region=CATALOG_PROPS["s3.region"],
    )


def arquivar_raw():
    """Copia só o CSV efetivamente lido (não o .zip inteiro, que pode ter
    outros anos/semestres nunca tocados por esta carga)."""
    with open(ZIP_PATH, "rb") as f:
        eh_zip = f.read(4) == b"PK\x03\x04"
    nome_arquivo = CSV_MEMBER if (eh_zip and CSV_MEMBER) else os.path.basename(ZIP_PATH)

    fs = montar_s3_filesystem()
    bucket = CATALOG_PROPS["warehouse"].replace("s3://", "").rstrip("/")
    tabela_curta = TABLE_NAME.split(".", 1)[1]
    destino = f"{bucket}/raw/{tabela_curta}/{nome_arquivo}"

    info = fs.get_file_info(destino)
    if info.type != pafs.FileType.NotFound:
        print(f"[loader] Raw já arquivado em s3://{destino}, pulando upload.")
        return

    print(f"[loader] Arquivando original em s3://{destino}...")
    if eh_zip:
        origem_ctx = zipfile.ZipFile(ZIP_PATH).open(CSV_MEMBER, "r")
    else:
        origem_ctx = open(ZIP_PATH, "rb")
    with origem_ctx as origem, fs.open_output_stream(destino) as saida:
        while True:
            bloco = origem.read(1 << 20)
            if not bloco:
                break
            saida.write(bloco)
    print("[loader] Raw arquivado.")


def abrir_csv():
    with open(ZIP_PATH, "rb") as f:
        eh_zip = f.read(4) == b"PK\x03\x04"
    if eh_zip:
        if not CSV_MEMBER:
            sys.exit("[loader] LOADER_ZIP_PATH aponta pra um .zip: defina LOADER_CSV_MEMBER.")
        zf = zipfile.ZipFile(ZIP_PATH)
        fh = zf.open(CSV_MEMBER, "r")
        return io.TextIOWrapper(fh, encoding=CSV_ENCODING, errors="replace", newline="")
    return open(ZIP_PATH, encoding=CSV_ENCODING, errors="replace", newline="")


def montar_schema_iceberg() -> Schema:
    campos = [
        NestedField(field_id=i, name=destino, field_type=TYPE_ICEBERG[tipo], required=False)
        for i, (_origem, destino, tipo) in enumerate(COLUMNS, start=1)
    ]
    campos.append(
        NestedField(field_id=len(campos) + 1, name="dt_ingestao", field_type=TimestampType(), required=False)
    )
    return Schema(*campos)


def garantir_tabela(catalog):
    namespace, _nome = TABLE_NAME.split(".")
    existentes = {n[0] for n in catalog.list_namespaces()}
    if namespace not in existentes:
        catalog.create_namespace(namespace)
    if catalog.table_exists(TABLE_NAME):
        print(f"[loader] Tabela {TABLE_NAME} já existe, anexando dados.")
        tabela = catalog.load_table(TABLE_NAME)
        if "dt_ingestao" not in tabela.schema().column_names:
            print("[loader] Evoluindo schema: adicionando dt_ingestao (linhas antigas ficam NULL)...")
            with tabela.update_schema() as update:
                update.add_column("dt_ingestao", TimestampType())
            tabela = catalog.load_table(TABLE_NAME)
        return tabela
    print(f"[loader] Criando tabela {TABLE_NAME}...")
    return catalog.create_table(TABLE_NAME, schema=montar_schema_iceberg())


def main():
    arquivar_raw()
    if os.getenv("LOADER_ARCHIVE_ONLY", "").lower() in ("1", "true"):
        print("[loader] LOADER_ARCHIVE_ONLY definido — não vou tocar na tabela.")
        return

    catalog = load_catalog("morfeu", **CATALOG_PROPS)
    tabela = garantir_tabela(catalog)

    agora = datetime.now(timezone.utc).replace(tzinfo=None)
    origem_cols = [c[0] for c in COLUMNS]
    dtype = {origem: TYPE_PANDAS[tipo] for origem, _destino, tipo in COLUMNS if TYPE_PANDAS[tipo]}
    arrow_schema = pa.schema(
        [(destino, TYPE_PYARROW[tipo]) for _o, destino, tipo in COLUMNS]
        + [("dt_ingestao", pa.timestamp("us"))]
    )
    renomeia = {origem: destino for origem, destino, _tipo in COLUMNS}
    ordem_final = [destino for _o, destino, _t in COLUMNS]

    total = 0
    with abrir_csv() as texto:
        leitor = pd.read_csv(
            texto,
            sep=CSV_SEP,
            usecols=origem_cols,
            dtype=dtype,
            decimal=",",
            chunksize=CHUNK_SIZE,
            on_bad_lines="warn",
        )
        for chunk in leitor:
            if MAX_ROWS and total >= MAX_ROWS:
                break
            if MAX_ROWS and total + len(chunk) > MAX_ROWS:
                chunk = chunk.iloc[: MAX_ROWS - total]
            chunk = chunk.rename(columns=renomeia)[ordem_final]
            chunk["dt_ingestao"] = agora
            tabela_arrow = pa.Table.from_pandas(chunk, schema=arrow_schema, preserve_index=False)
            tabela.append(tabela_arrow)
            total += len(chunk)
            print(f"[loader] {total} linhas gravadas em {TABLE_NAME}...", flush=True)

    print(f"[loader] Concluído. {total} linhas gravadas em {TABLE_NAME}.")


if __name__ == "__main__":
    main()

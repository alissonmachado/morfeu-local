"""
morfeu — ingestor de dados abertos da ANATEL (via dados.gov.br / PDA da Anatel).

Alimenta o tópico Kafka com dados REAIS de estações de telecomunicações.
Cada linha do CSV vira um evento de topologia (um nó da rede).

Fontes suportadas (em ordem de prioridade):
  1. DADOSGOV_RESOURCE_URL  -> baixa o arquivo direto (CSV ou ZIP). Sem token.
                              Ex.: um link em https://www.anatel.gov.br/dadosabertos/PDA/...
                              ou o link "Baixar" de um recurso no dados.gov.br.
  2. DADOSGOV_DATASET_ID    -> usa a API do dados.gov.br (endpoint 'detalhar'
                              GET /conjuntos-dados/{id}) para achar o recurso.
                              A API de consumidor exige token (DADOSGOV_API_TOKEN).

Modos (DADOSGOV_MODE):
  ingest   (padrão) baixa e publica os eventos no Kafka.
  headers           só mostra as COLUNAS do CSV e uma linha de amostra, e sai.
                    Use antes de ingerir para ajustar o DADOSGOV_FIELD_MAP.
  dump              só imprime o JSON do detalhamento (fluxo via API).
"""
import csv
import io
import json
import os
import sys
import tempfile
import time
import uuid
import zipfile
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --------------------------------------------------------------------- config
BASE_URL = os.getenv("DADOSGOV_BASE_URL", "https://dados.gov.br/dados/api/publico")
API_TOKEN = os.getenv("DADOSGOV_API_TOKEN", "")
TOKEN_HEADER = os.getenv("DADOSGOV_TOKEN_HEADER", "chave-api-dados-abertos")
RESOURCE_URL = os.getenv("DADOSGOV_RESOURCE_URL", "")
DATASET_ID = os.getenv("DADOSGOV_DATASET_ID", "")
RESOURCE_FORMAT = os.getenv("DADOSGOV_RESOURCE_FORMAT", "CSV").upper()
ZIP_MEMBER = os.getenv("DADOSGOV_ZIP_MEMBER", "")     # substring do CSV dentro do zip
CSV_SEP = os.getenv("DADOSGOV_CSV_SEP", ";")
CSV_ENCODING = os.getenv("DADOSGOV_CSV_ENCODING", "latin-1")  # ANATEL costuma ser latin-1
MAX_ROWS = int(os.getenv("DADOSGOV_MAX_ROWS", "500"))
TIPO_NO = os.getenv("DADOSGOV_TIPO_NO", "ESTACAO")
MODE = os.getenv("DADOSGOV_MODE", "ingest").lower()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topologia.eventos")

# Mapa coluna_do_CSV -> campo do evento. AJUSTE conforme o 'make headers'.
DEFAULT_FIELD_MAP = {
    "id_no": "NumEstacao",
    "id_no_pai": "",
    "uf": "SiglaUf",
    "municipio": "CodMunicipio",
    "latitude": "Latitude",
    "longitude": "Longitude",
    "status": "Status",
}
FIELD_MAP = json.loads(os.getenv("DADOSGOV_FIELD_MAP", json.dumps(DEFAULT_FIELD_MAP)))


def headers() -> dict:
    return {TOKEN_HEADER: API_TOKEN} if API_TOKEN else {}


def http_get_json(path: str, **params):
    resp = requests.get(f"{BASE_URL}{path}", headers=headers(), params=params or None, timeout=60)
    resp.raise_for_status()
    return resp.json()


def resolver_url_via_api() -> str:
    """Usa o endpoint 'detalhar' para achar a URL do primeiro recurso CSV/ZIP."""
    if not DATASET_ID:
        sys.exit("[ingest] Defina DADOSGOV_RESOURCE_URL (recomendado) ou DADOSGOV_DATASET_ID.")
    print(f"[ingest] Detalhando conjunto {DATASET_ID} via API...")
    detalhe = http_get_json(f"/conjuntos-dados/{DATASET_ID}")
    recursos = detalhe.get("recursos") or detalhe.get("resources") or []
    for r in recursos:
        fmt = str(r.get("formato") or r.get("format") or "").upper()
        link = r.get("link") or r.get("url") or r.get("downloadUrl")
        if link and (RESOURCE_FORMAT in fmt or "ZIP" in fmt or link.upper().endswith((".CSV", ".ZIP"))):
            return link
    sys.exit("[ingest] Nenhum recurso CSV/ZIP encontrado no conjunto.")


def baixar_arquivo(url: str) -> str:
    """Baixa em disco (streaming) e devolve o caminho — evita estourar a memória."""
    print(f"[ingest] Baixando: {url}")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    with requests.get(url, headers=headers(), timeout=600, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1 << 20):
            tmp.write(chunk)
    tmp.close()
    return tmp.name


def abrir_reader(path: str):
    """DictReader lazy a partir de um arquivo .csv ou .zip (lê sob demanda)."""
    with open(path, "rb") as f:
        eh_zip = f.read(4) == b"PK\x03\x04"
    if eh_zip:
        zf = zipfile.ZipFile(path)
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            sys.exit("[ingest] ZIP sem arquivos .csv dentro.")
        alvo = next((n for n in csvs if ZIP_MEMBER.lower() in n.lower()), csvs[0]) if ZIP_MEMBER else csvs[0]
        print(f"[ingest] CSV dentro do ZIP: {alvo}  (disponíveis: {csvs})")
        fh = zf.open(alvo, "r")
        texto = io.TextIOWrapper(fh, encoding=CSV_ENCODING, errors="replace", newline="")
    else:
        texto = open(path, encoding=CSV_ENCODING, errors="replace", newline="")
    return csv.DictReader(texto, delimiter=CSV_SEP)


def obter_reader():
    url = RESOURCE_URL or resolver_url_via_api()
    return abrir_reader(baixar_arquivo(url))


def to_float(v):
    if v in (None, ""):
        return None
    try:
        return float(str(v).strip().replace(",", "."))
    except ValueError:
        return None


def linha_para_evento(row: dict) -> dict:
    def col(campo):
        nome = FIELD_MAP.get(campo)
        return row.get(nome) if nome else None

    return {
        "id_evento": str(uuid.uuid4()),
        "tipo_operacao": "CREATE",
        "id_no": (col("id_no") or str(uuid.uuid4())),
        "tipo_no": TIPO_NO,
        "id_no_pai": col("id_no_pai"),
        "uf": col("uf"),
        "municipio": (str(col("municipio")) if col("municipio") is not None else None),
        "latitude": to_float(col("latitude")),
        "longitude": to_float(col("longitude")),
        "status": (col("status") or "ATIVO"),
        "timestamp_evento": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "_origem": "ANATEL/dados.gov.br",
        "_raw_row": row,
    }


def build_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except NoBrokersAvailable:
            print("[ingest] Kafka indisponível, retry em 3s...")
            time.sleep(3)


def modo_headers():
    reader = obter_reader()
    print("\n=== COLUNAS DO CSV ===")
    for c in (reader.fieldnames or []):
        print(f"  - {c}")
    primeira = next(reader, None)
    if primeira:
        print("\n=== PRIMEIRA LINHA (amostra) ===")
        print(json.dumps(primeira, ensure_ascii=False, indent=2)[:4000])
    print("\nAjuste o DADOSGOV_FIELD_MAP no .env com base nesses nomes e rode 'make ingest'.")


def modo_dump():
    print(json.dumps(http_get_json(f"/conjuntos-dados/{DATASET_ID}"), ensure_ascii=False, indent=2)[:8000])


def modo_ingest():
    reader = obter_reader()
    producer = build_producer()
    enviados = 0
    for row in reader:
        if enviados >= MAX_ROWS:
            break
        evt = linha_para_evento(row)
        producer.send(KAFKA_TOPIC, key=evt["id_no"], value=evt)
        enviados += 1
        if enviados % 100 == 0:
            print(f"[ingest] {enviados} eventos publicados...", flush=True)
    producer.flush()
    print(f"[ingest] Concluído. {enviados} eventos publicados em '{KAFKA_TOPIC}'.")


if __name__ == "__main__":
    if MODE == "ingest" and not RESOURCE_URL and not API_TOKEN:
        print("[aviso] Sem DADOSGOV_RESOURCE_URL e sem token: se usar a API, pode dar 401.",
              file=sys.stderr)
    if MODE == "dump":
        modo_dump()
    elif MODE == "headers":
        modo_headers()
    else:
        modo_ingest()

"""
morfeu — consumidor de auditoria.

Lê o tópico de eventos de topologia e persiste a MENSAGEM BRUTA no MongoDB,
para rastreabilidade, auditoria e apoio a reprocessamentos.

Cada documento gravado guarda o payload original mais metadados de controle
(partição, offset, timestamp do Kafka e o instante da ingestão de auditoria).
"""
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "topologia.eventos")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "morfeu-audit")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "morfeu")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "topologia_raw")


def build_consumer() -> KafkaConsumer:
    """Tenta conectar ao Kafka com retry (o broker pode subir depois)."""
    while True:
        try:
            return KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
            )
        except NoBrokersAvailable:
            print("[audit] Kafka indisponível, tentando novamente em 3s...")
            time.sleep(3)


def main() -> None:
    mongo = MongoClient(MONGO_URI)
    collection = mongo[MONGO_DB][MONGO_COLLECTION]
    consumer = build_consumer()

    print(
        f"[audit] Consumindo '{KAFKA_TOPIC}' em {KAFKA_BOOTSTRAP} "
        f"-> {MONGO_DB}.{MONGO_COLLECTION}"
    )

    for msg in consumer:
        # Tenta preservar o JSON estruturado; se falhar, guarda como texto puro.
        try:
            payload = json.loads(msg.value)
        except (json.JSONDecodeError, TypeError):
            payload = None

        documento = {
            "raw": msg.value,           # mensagem bruta exatamente como recebida
            "payload": payload,         # versão parseada (quando JSON válido)
            "kafka": {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "timestamp_ms": msg.timestamp,
                "key": msg.key.decode("utf-8") if msg.key else None,
            },
            "dt_ingestao_auditoria": datetime.now(timezone.utc),
        }
        collection.insert_one(documento)
        print(
            f"[audit] gravado offset={msg.offset} particao={msg.partition}",
            flush=True,
        )


if __name__ == "__main__":
    main()

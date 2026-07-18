#!/usr/bin/env bash
# Publica os eventos de exemplo (data/sample_events.json) no tópico topologia.eventos.
set -euo pipefail

TOPIC="${1:-topologia.eventos}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo ">> Publicando eventos em '${TOPIC}'..."
${DOCKER:-docker} exec -i morfeu-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${TOPIC}" < "${DIR}/data/sample_events.json"

echo ">> Concluído."

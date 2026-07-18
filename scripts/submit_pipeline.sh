#!/usr/bin/env bash
# Submete o pipeline de ingestão (Flink SQL) ao cluster Flink.
set -euo pipefail

echo ">> Submetendo o pipeline morfeu ao Flink..."
${DOCKER:-docker} exec -i morfeu-flink-jobmanager \
  ./bin/sql-client.sh -f /opt/sql/morfeu_pipeline.sql

echo ">> Job submetido. Acompanhe em http://localhost:8081"

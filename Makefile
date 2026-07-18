# Backend-agnóstico: funciona com Docker Desktop OU Rancher Desktop.
#   - Rancher Desktop / backend dockerd (moby): use os padrões abaixo.
#   - Rancher Desktop / backend containerd:     make up COMPOSE="nerdctl compose" DOCKER=nerdctl
COMPOSE ?= docker compose
DOCKER  ?= docker

.PHONY: help up down build logs pipeline produce ingest dump trino mongo ps clean load-acessos load-velocidade

help:
	@echo "morfeu — ambiente local"
	@echo ""
	@echo "  make up        Sobe todo o stack (build + up -d)"
	@echo "  make pipeline  Submete o pipeline Flink SQL (Kafka -> Iceberg)"
	@echo "  make produce   Publica os eventos SINTÉTICOS de exemplo no Kafka"
	@echo "  make ingest    Ingere dados REAIS do dados.gov.br (usa .env)"
	@echo "  make headers   Mostra as colunas do CSV da ANATEL (p/ ajustar o mapa)"
	@echo "  make dump      Inspeciona o detalhamento do conjunto (dados.gov.br)"
	@echo "  make load-acessos     Carga em lote: acessos banda larga fixa (Iceberg)"
	@echo "  make load-velocidade  Carga em lote: velocidade contratada SCM (Iceberg)"
	@echo "  make trino     Abre o cliente SQL do Trino"
	@echo "  make mongo     Abre o mongosh na coleção de auditoria"
	@echo "  make logs      Segue os logs de todos os serviços"
	@echo "  make ps        Lista o status dos serviços"
	@echo "  make down      Para e remove os containers"
	@echo "  make clean     down + remove volumes (zera MinIO e Mongo)"
	@echo ""
	@echo "  Rancher Desktop (containerd): acrescente COMPOSE=\"nerdctl compose\" DOCKER=nerdctl"

up:
	$(COMPOSE) up -d --build

build:
	$(COMPOSE) build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

pipeline:
	DOCKER="$(DOCKER)" ./scripts/submit_pipeline.sh

produce:
	DOCKER="$(DOCKER)" ./scripts/produce_sample.sh

# 'run <serviço>' inclui o serviço mesmo com profile definido — portável entre docker/nerdctl.
ingest:
	$(COMPOSE) run --rm --build dadosgov-producer

headers:
	$(COMPOSE) run --rm --build -e DADOSGOV_MODE=headers dadosgov-producer

dump:
	$(COMPOSE) run --rm --build -e DADOSGOV_MODE=dump dadosgov-producer

trino:
	$(DOCKER) exec -it morfeu-trino trino

mongo:
	$(DOCKER) exec -it morfeu-mongodb mongosh morfeu --eval "db.topologia_raw.find().limit(5).pretty()"

load-acessos:
	$(COMPOSE) run --rm --build \
	  -e LOADER_SCHEMA_FILE=/app/schemas/acessos_banda_larga_fixa.json \
	  -e LOADER_ZIP_PATH=/raw/acessos_banda_larga_fixa.zip \
	  -e LOADER_CSV_MEMBER=Acessos_Banda_Larga_Fixa_2026.csv \
	  -e LOADER_TABLE=morfeu.acessos_banda_larga_fixa \
	  loader

load-velocidade:
	$(COMPOSE) run --rm --build \
	  -e LOADER_SCHEMA_FILE=/app/schemas/velocidade_contratada_scm.json \
	  -e LOADER_ZIP_PATH=/raw/velocidade_contratada_scm.zip \
	  -e LOADER_CSV_MEMBER=Velocidade_Contratada_SCM.csv \
	  -e LOADER_TABLE=morfeu.velocidade_contratada_scm \
	  loader

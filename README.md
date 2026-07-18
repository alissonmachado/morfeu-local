# morfeu

> Pipeline de ingestão contínua de eventos de topologia de rede, com persistência
> analítica em **Apache Iceberg** (catálogo **REST**), auditoria em **MongoDB** e
> consulta via **Trino**. Fontes de dados reais: **Portal Brasileiro de Dados
> Abertos (dados.gov.br)** e **painéis da ANATEL** — estações de
> telecomunicações, acessos de banda larga fixa e velocidade contratada.

Projeto acadêmico. Ambiente 100% local via Docker Compose.

> **Nota de transparência.** O código, a infraestrutura (Docker Compose,
> Dockerfiles, pipeline Flink SQL, loader `pyiceberg`) e esta documentação
> foram desenvolvidos com auxílio de IA — [Claude](https://claude.ai)
> (Anthropic), via Claude Code — em sessões supervisionadas pelo autor, que
> tomou as decisões de arquitetura, validou os resultados nas fontes de dados
> reais e é responsável pelo conteúdo final.

## Proposta de negócio

O pipeline combina duas visões que normalmente ficam em sistemas separados:
**topologia de rede** (onde estão os nós de telecomunicações) e **indicadores
de mercado** (quantos acessos existem, a que velocidade, por operadora,
UF e município). Cruzar as duas é o que dá valor de negócio — nenhuma delas
sozinha responde à pergunta que interessa a quem planeja investimento ou
regulação: *onde a infraestrutura já existe, onde o acesso está crescendo, e
onde as duas coisas não se encontram.*

Casos de uso que essa base de dados unificada permite, hoje:

- **Priorização de expansão de rede.** Cruzar `topologia_rede` (nós físicos)
  com `acessos_banda_larga_fixa` (acessos por município) para identificar
  municípios com demanda/acesso crescente e baixa densidade de infraestrutura
  — candidatos naturais a investimento.
- **Inteligência competitiva.** `acessos_banda_larga_fixa` traz `empresa`,
  `grupo_econômico`, `tecnologia` e `faixa_velocidade` por município — dá para
  responder "quem domina onde" e "com qual tecnologia", sem depender de
  relatórios trimestrais das próprias operadoras.
- **Acompanhamento da qualidade de serviço regulatória.** `velocidade_contratada_scm`
  permite comparar a velocidade efetivamente contratada, por região e ao
  longo do tempo, contra metas de universalização — um insumo direto para
  quem monitora política pública de conectividade (ANATEL, prefeituras,
  ONGs de inclusão digital).
- **Due diligence de M&A e investimento em infraestrutura.** Uma base
  histórica e auditável (Iceberg com *time travel*, MongoDB com a mensagem
  bruta original) sustenta due diligence de aquisição de provedores regionais
  ou avaliação de ativos de rede.

Este repositório é a prova de conceito da arquitetura, não o produto: os
volumes e o recorte temporal (um ano de banda larga fixa, um semestre de
telefonia móvel, 4 anos de velocidade contratada, o histórico integral de TV
por assinatura, uma amostra de estações) foram escolhidos para caber num
ambiente Docker local (ver [Rancher Desktop](#rancher-desktop)). A mesma stack
— Kafka, Flink, Iceberg, Trino — é o desenho de referência usado em
lakehouses de streaming em produção; escalar significa trocar o ambiente de
execução (Kubernetes gerenciado, warehouse Iceberg com armazenamento de
objetos real, catálogo com persistência em Postgres) sem reescrever o
pipeline.

## Arquitetura

Duas trilhas de ingestão convivem no mesmo lakehouse, cada uma para o tipo de
dado a que se aplica — não existe uma "única forma certa" de trazer dado pra
dentro do Iceberg aqui:

```
  TRILHA 1 — streaming (eventos de topologia)
  ─────────────────────────────────────────────────────────────────
  dados.gov.br (API)                      eventos sintéticos
   estações ANATEL                         (data/*.json)
        │                                        │
        ▼                                        ▼
   dadosgov-producer  ───────►  Kafka  ◄──────  produce_sample
                                  │  (topologia.eventos)
                   ┌──────────────┴───────────────┐
                   ▼                              ▼
             Flink (streaming)            audit-consumer
                   │                              │
                   ▼                              ▼
   Iceberg (REST + MinIO/S3)             MongoDB (mensagem bruta)
        morfeu.topologia_rede
                   │
                   ▼
                Trino  ◄── consulta analítica (SQL)


  TRILHA 2 — carga em lote (indicadores agregados da ANATEL)
  ─────────────────────────────────────────────────────────────────
  .zip local (painéis ANATEL)
   acessos banda larga fixa
   velocidade contratada SCM
        │
        ▼
      loader (pyiceberg, lê CSV em chunks, grava direto no catálogo REST)
        │
        ▼
   Iceberg (REST + MinIO/S3)  ──► Trino
        morfeu.acessos_banda_larga_fixa
        morfeu.velocidade_contratada_scm
```

Cada estação da ANATEL (com UF, município, latitude/longitude) é modelada como um
**nó** da topologia. O Flink transforma o JSON, deriva `dt_particao` e grava na
tabela Iceberg `morfeu.topologia_rede`. Em paralelo, a mensagem bruta é persistida
no MongoDB para auditoria e reprocessamento.

Os datasets de acessos e velocidade contratada **não** passam por Kafka/Flink:
são estatísticas agregadas mensais (sem coordenadas, sem conceito de "nó"),
distribuídas em arquivos de centenas de MB a poucos GB — histórico, não
streaming. Forçá-los pela mesma esteira de eventos seria a ferramenta errada
para o problema (ver [Decisões de projeto](#decisões-de-projeto-e-aprendizados)).

## Stack

| Camada            | Tecnologia                            |
|-------------------|---------------------------------------|
| Barramento        | Apache Kafka 3.9 (KRaft)              |
| Processamento     | Apache Flink 1.20 (SQL)              |
| Formato de tabela | Apache Iceberg 1.10 (catálogo REST)  |
| Object store      | MinIO (S3-compatível)                |
| Auditoria         | MongoDB 7                            |
| Consulta          | Trino 476                            |
| Carga em lote     | Python 3.11 + pyiceberg + pandas     |
| Ingestão real     | API pública dados.gov.br / ANATEL    |

## Resultados-chave

- **`topologia_rede`**: **1.006 eventos** de nós de rede (estações reais da
  ANATEL + eventos sintéticos de teste), streaming via Flink.
- **`acessos_banda_larga_fixa`**: **3.413.808 linhas** (ano 2026, recorte mais
  recente de uma série que a ANATEL disponibiliza desde 2007).
- **`velocidade_contratada_scm`**: **5.789.372 linhas** (2017-2020, arquivo
  único integral).
- **`acessos_telefonia_movel`**: **11.871.067 linhas** (2026, 1º semestre —
  recorte mais recente de uma série que vai de 2005 a 2026).
- **`acessos_tv_assinatura`**: **4.584.455 linhas** (arquivo único integral,
  todos os anos disponíveis).
- ~25,6 milhões de linhas analíticas carregadas em lote, direto no Iceberg via
  `pyiceberg`, sem passar por Kafka — ver por quê na seção de decisões.

## Pré-requisitos

- Docker + Docker Compose v2
- **~10-12 GB de RAM livres na VM do container engine** se for rodar a carga em
  lote dos datasets ANATEL junto com o resto do stack (Trino sozinho já usa
  ~2,3 GB sob carga; ver nota de memória abaixo). Para só o streaming de
  topologia, ~6 GB bastam.
- Um **token de consumidor** do dados.gov.br (opcional — só para a variante via
  API; a variante por URL direta não precisa)
- Os `.zip` dos painéis da ANATEL, baixados manualmente (ver
  [Carga em lote](#carga-em-lote-indicadores-agregados-da-anatel))

## Rancher Desktop

Funciona com Rancher Desktop. Pontos de atenção:

1. **Memória da VM.** Por padrão a VM do Rancher Desktop vem com **7,6 GB**, o
   que é justo demais: rodar a carga em lote de um CSV de ~500 MB com Trino e
   Flink já em pé estourou memória (`Cannot allocate memory`) durante o
   desenvolvimento deste projeto. Em *Preferences → Virtual Machine →
   Hardware*, reserve pelo menos **10-12 GB de RAM** e 4 CPUs. Se não puder
   aumentar, pare `trino` e os dois containers do `flink` antes de rodar
   `make load-acessos` / `make load-velocidade` (eles não são necessários
   durante a carga, só para consultar depois):
   ```bash
   docker compose stop trino flink-taskmanager flink-jobmanager
   make load-acessos
   docker compose start trino flink-jobmanager flink-taskmanager
   make pipeline   # o job do Flink não sobrevive ao restart do container
   ```

2. **Backend do container engine** (*Preferences → Container Engine*):
   - **`dockerd (moby)`** — recomendado. `docker` e `docker compose` funcionam
     normalmente; use os comandos `make` como estão.
   - **`containerd`** — use `nerdctl`. Passe as variáveis para o make:
     ```bash
     make up      COMPOSE="nerdctl compose" DOCKER=nerdctl
     make ingest  COMPOSE="nerdctl compose" DOCKER=nerdctl
     make trino   DOCKER=nerdctl
     ```

As portas publicadas são encaminhadas para o `localhost` automaticamente nos dois
backends.

## Início rápido

```bash
git clone <seu-repo> && cd morfeu
cp .env.example .env          # ajuste ANATEL_RAW_DIR e, se for usar a API, o token
make up                       # sobe todo o stack
make pipeline                 # submete o job Flink (Kafka -> Iceberg)
```

Depois, escolha a fonte dos eventos de topologia:

```bash
make produce   # (A) eventos sintéticos de exemplo — funciona sem token
make ingest    # (B) dados REAIS do dados.gov.br — requer .env configurado
```

Consulte no Trino:

```bash
make trino
```
```sql
SELECT * FROM iceberg.morfeu.topologia_rede LIMIT 20;
SELECT uf, count(*) FROM iceberg.morfeu.topologia_rede GROUP BY uf ORDER BY 2 DESC;
```

## Ingestão de dados reais da ANATEL (streaming)

O tópico Kafka pode ser alimentado com dados reais de estações de
telecomunicações da ANATEL. O caminho mais simples é por **URL direta** do
recurso (não precisa de token):

1. Escolha um conjunto da ANATEL com UF, município, latitude e longitude — por
   exemplo o *Plano Básico de Radiodifusão* (FM/TV) ou as *estações licenciadas*
   (SMP/SCM) do Mosaico. Fontes:
   - Portal ANATEL: `https://www.anatel.gov.br/dadosabertos/PDA/...`
   - dados.gov.br: conjuntos com a tag ANATEL (no recurso, use o link "Baixar").

2. No `.env`, cole o link em `DADOSGOV_RESOURCE_URL`. O ingestor baixa CSV **ou
   ZIP** (extrai o CSV de dentro; use `DADOSGOV_ZIP_MEMBER` para escolher qual).

3. Descubra os nomes reais das colunas e o encoding correto — **não confie no
   que a documentação do dataset promete, confira sempre**:
   ```bash
   make headers
   ```
   Isso imprime as colunas do CSV e uma linha de amostra. Ajuste
   `DADOSGOV_FIELD_MAP` e `DADOSGOV_CSV_ENCODING` no `.env` de acordo (ver
   [o bug do encoding](#decisões-de-projeto-e-aprendizados) abaixo).

4. Publique no Kafka:
   ```bash
   make ingest
   ```

Os eventos entram no mesmo tópico `topologia.eventos` e seguem pelo pipeline
Flink → Iceberg, e são auditados no MongoDB (o CSV original de cada linha fica
guardado em `_raw_row`).

**Alternativa via API** (endpoint *detalhar* `GET /conjuntos-dados/{id}`): exige
um token de consumidor do dados.gov.br. Preencha `DADOSGOV_API_TOKEN` e
`DADOSGOV_DATASET_ID` no `.env` e deixe `DADOSGOV_RESOURCE_URL` vazio. Instruções
do token: https://dados.gov.br/dados/conteudo/como-acessar-a-api-do-portal-de-dados-abertos-com-o-perfil-de-consumidor

## Carga em lote: indicadores agregados da ANATEL

Os painéis de **acessos de banda larga fixa**, **velocidade contratada**,
**telefonia móvel** e **TV por assinatura** da ANATEL não são eventos de
topologia — são estatísticas agregadas mensais por operadora/UF/município,
distribuídas em arquivos grandes (a série de telefonia móvel, por exemplo,
tem um `.csv` por semestre desde 2005; só o 1º semestre de 2026 tem ~1,5 GB e
quase 12M linhas). Por isso entram por uma trilha própria, em lote, sem Kafka.

1. Baixe manualmente os `.zip` nos painéis da ANATEL (busque por "acessos" no
   portal https://informacoes.anatel.gov.br/paineis/ — os dois primeiros
   links abaixo foram confirmados nesta sessão; os de telefonia móvel e TV
   por assinatura seguem o mesmo padrão de portal, mas confirme o link exato
   antes de usar):
   - https://informacoes.anatel.gov.br/paineis/acessos/banda-larga-fixa
   - https://informacoes.anatel.gov.br/paineis/acessos/velocidade-contratada-banda-larga-fixa
   - Telefonia móvel (`acessos_telefonia_movel.zip`)
   - TV por assinatura (`acessos_tv_por_assinatura.zip`)

2. No `.env`, aponte `ANATEL_RAW_DIR` para a pasta onde os `.zip` ficaram (ex.:
   sua pasta de Downloads). É montada como somente-leitura em `/raw` dentro do
   container — os arquivos **não** são copiados para dentro do repositório.

3. Rode a carga:
   ```bash
   make load-acessos       # acessos_banda_larga_fixa (ano mais recente do zip)
   make load-velocidade    # velocidade_contratada_scm (arquivo único)
   make load-movel         # acessos_telefonia_movel (semestre mais recente)
   make load-tv            # acessos_tv_assinatura (arquivo único)
   ```
   Cada linha do CSV é escrita direto na tabela Iceberg correspondente via
   `pyiceberg`, em chunks (não carrega o arquivo inteiro em memória). O schema
   de cada dataset (mapeamento coluna → campo → tipo) fica versionado em
   `loader/schemas/*.json` — se a ANATEL mudar o layout do CSV, é só editar o
   JSON, não o código Python. Toda tabela ganha uma coluna `dt_ingestao`
   (timestamp de quando a linha foi carregada — mesmo metadado de
   rastreabilidade que o Flink grava em `topologia_rede`; o loader evolui o
   schema automaticamente se a tabela já existir sem essa coluna, e linhas
   carregadas antes dessa mudança ficam `NULL`, não um valor inventado). Para
   os datasets maiores (telefonia móvel em especial), pare `trino` e o
   `flink` antes de rodar a carga — ver [Rancher Desktop](#rancher-desktop).

4. **Auditoria do raw.** Antes de processar, o loader copia o CSV
   efetivamente lido (não o `.zip` inteiro — só o membro do ano/semestre
   usado) para `s3://warehouse/raw/<tabela>/<arquivo>` no MinIO. É o
   equivalente, para a trilha em lote, do que o `audit-consumer` +
   `topologia_raw` fazem para a trilha de streaming: se a ANATEL atualizar o
   dataset ou o `.zip` local for perdido, ainda existe uma cópia exata do que
   foi carregado. Upload é idempotente (pula se o objeto já existir).

5. Para testar com poucas linhas antes de rodar o arquivo inteiro (recomendado
   ao trocar de dataset ou schema):
   ```bash
   docker compose run --rm -e LOADER_MAX_ROWS=5000 \
     -e LOADER_SCHEMA_FILE=/app/schemas/velocidade_contratada_scm.json \
     -e LOADER_ZIP_PATH=/raw/velocidade_contratada_scm.zip \
     -e LOADER_CSV_MEMBER=Velocidade_Contratada_SCM.csv \
     -e LOADER_TABLE=morfeu.velocidade_contratada_scm_teste \
     loader
   ```
   Para só arquivar o raw de uma carga que já rodou (sem duplicar dados
   reprocessando a tabela), use `LOADER_ARCHIVE_ONLY=true`.

## Consultas úteis (Trino)

```sql
-- Estado atual de cada nó de topologia (última operação por id_no)
SELECT id_no, tipo_operacao, status, uf, municipio
FROM (
  SELECT *, row_number() OVER (PARTITION BY id_no ORDER BY timestamp_evento DESC) rn
  FROM iceberg.morfeu.topologia_rede
) WHERE rn = 1;

-- Distribuição por partição
SELECT dt_particao, count(*) FROM iceberg.morfeu.topologia_rede GROUP BY dt_particao;

-- Acessos de banda larga fixa por UF (2026)
SELECT uf, sum(acessos) AS total
FROM iceberg.morfeu.acessos_banda_larga_fixa
GROUP BY uf ORDER BY total DESC LIMIT 10;

-- Velocidade contratada média por ano
SELECT ano, avg(velocidade_contratada_mbps) AS media_mbps
FROM iceberg.morfeu.velocidade_contratada_scm
GROUP BY ano ORDER BY ano;
```

## Auditoria (MongoDB)

```bash
make mongo
docker exec -it morfeu-mongodb mongosh morfeu --eval "db.topologia_raw.countDocuments()"
```

Ou pela UI web (**mongo-express**, sem autenticação — mesmo padrão das demais
UIs deste ambiente local): http://localhost:8091

## Comandos (Makefile)

| Comando               | Ação                                             |
|-----------------------|---------------------------------------------------|
| `make up`             | build + sobe o stack                             |
| `make pipeline`       | submete o pipeline Flink SQL                     |
| `make produce`        | publica eventos sintéticos de topologia          |
| `make ingest`         | ingere dados reais de estações (dados.gov.br)     |
| `make headers`        | mostra as colunas do CSV de estações              |
| `make dump`           | inspeciona o detalhamento de um conjunto          |
| `make load-acessos`   | carga em lote: acessos banda larga fixa          |
| `make load-velocidade`| carga em lote: velocidade contratada SCM         |
| `make load-movel`     | carga em lote: acessos telefonia móvel           |
| `make load-tv`        | carga em lote: acessos TV por assinatura         |
| `make trino`          | abre o cliente SQL do Trino                      |
| `make mongo`          | consulta a coleção de auditoria                  |
| `make ps`             | status dos containers                            |
| `make down`           | para os containers                               |
| `make clean`          | para e remove os volumes                         |

## Portas

| Serviço      | URL / porta                    |
|--------------|--------------------------------|
| MinIO console| http://localhost:9001 (admin/password) |
| Iceberg REST | http://localhost:8181          |
| Flink UI     | http://localhost:8081          |
| Trino        | http://localhost:8080          |
| MongoDB      | localhost:27017                |
| Mongo Express| http://localhost:8091          |
| Kafka (host) | localhost:29092                |

## Estrutura do repositório

```
morfeu/
├── docker-compose.yml         # orquestração do stack
├── .env.example               # configuração (copie para .env)
├── Makefile                   # atalhos
├── flink/
│   ├── Dockerfile             # Flink + jars Iceberg/Kafka/Hadoop (Java 11)
│   └── sql/morfeu_pipeline.sql
├── producer/                  # ingestor dados.gov.br -> Kafka (streaming)
│   ├── Dockerfile
│   └── dadosgov_producer.py
├── audit/                     # Kafka -> MongoDB (mensagem bruta)
│   ├── Dockerfile
│   └── audit_consumer.py
├── loader/                    # carga em lote ANATEL -> Iceberg (sem Kafka)
│   ├── Dockerfile
│   ├── carga_lote.py
│   └── schemas/                # schema declarativo por dataset (JSON)
│       ├── acessos_banda_larga_fixa.json
│       ├── velocidade_contratada_scm.json
│       ├── acessos_telefonia_movel.json
│       └── acessos_tv_assinatura.json
├── trino/catalog/iceberg.properties
├── scripts/                   # submit_pipeline.sh, produce_sample.sh
└── data/sample_events.json
```

## Notas de projeto

- **Catálogo REST em memória.** O `iceberg-rest-fixture` guarda os metadados em
  SQLite in-memory: `make clean` (ou reiniciar o `iceberg-rest`) zera o catálogo.
  Para persistência entre reinícios, aponte-o para um Postgres (`CATALOG_URI`,
  `CATALOG_JDBC_USER`, `CATALOG_JDBC_PASSWORD`).
- **create/update/delete lógico.** Modelo *append-only*: cada evento é uma linha
  com `tipo_operacao`; o estado atual sai da consulta com `row_number()`. Para
  *upsert* nativo (Iceberg v2 + chave `id_no`), configure o sink do Flink em modo
  `upsert`.
- **Kafka.** Na rede Docker use `kafka:9092`; do host, `localhost:29092`.
- **Job do Flink não sobrevive a restart do container.** Não há savepoint
  configurado; se `flink-jobmanager` reiniciar, rode `make pipeline` de novo.
  A fonte Kafka usa `scan.startup.mode = group-offsets` com
  `properties.group.id = morfeu-flink-v2`: um resubmit resume de onde o
  consumer group parou, em vez de reprocessar o tópico inteiro (ver o bug de
  duplicação nas [Decisões de projeto](#decisões-de-projeto-e-aprendizados)).
- **Auditoria do raw nas duas trilhas.** A trilha de streaming audita a
  mensagem bruta no MongoDB (`topologia_raw`, via `audit-consumer`); a trilha
  em lote audita o CSV original no MinIO (`s3://warehouse/raw/<tabela>/...`,
  via `loader`). Mecanismos diferentes, mesmo propósito — replicar milhões
  de linhas no Mongo só para auditoria não valeria o custo.

## Decisões de projeto e aprendizados

Registro aqui, em primeira pessoa, decisões que moldaram o que este pipeline
faz e não faz — e os bugs reais que encontrei no caminho, porque cada um deles
mudou alguma coisa concreta no código ou na arquitetura.

**Por que os indicadores agregados da ANATEL não entram pelo Kafka/Flink.**
Quando fui incluir os datasets de acessos de banda larga fixa e velocidade
contratada, minha primeira reação foi tentar encaixá-los na mesma esteira de
eventos de topologia — é o pipeline que já existia. Mas esses dados não têm
latitude/longitude, não têm um ID de estação físico, são contagens agregadas
por operadora/UF/mês. Forçar isso em `topologia_rede` seria um encaixe
semanticamente errado só para reaproveitar infraestrutura. Optei por uma
trilha de carga em lote separada (`loader/`, via `pyiceberg`), escrevendo
direto no catálogo REST do Iceberg. Isso também resolve um problema de escala:
publicar 3,4 milhões de linhas como mensagens individuais do Kafka, para dados
que são histórico mensal e não eventos em tempo real, seria a ferramenta
errada pelo motivo errado.

**O bug do encoding: `latin-1` por suposição, não por verificação.** O
producer original assumia `latin-1` para os CSVs da ANATEL, com um comentário
dizendo "ANATEL costuma ser latin-1". Ao rodar `make headers` no recurso
padrão, os cabeçalhos vieram como `Ã§Ã£o` em vez de `ção`, e um `ï»¿` sujando o
nome da primeira coluna — sinal clássico de um arquivo UTF-8 (com BOM) sendo
decodificado como latin-1. A correção foi trocar para `utf-8-sig` (que também
descarta o BOM automaticamente). O aprendizado que fica: "costuma ser" não é
verificação — o modo `headers` existe exatamente para checar antes de assumir,
e eu quase pulei essa etapa por confiar no comentário do código.

**O bug silencioso do `kafka-python` no Python 3.12.** O `audit-consumer` e o
`dadosgov-producer` usavam `python:3.12-slim` com `kafka-python==2.0.2`. Essa
combinação quebra em import (`ModuleNotFoundError:
kafka.vendor.six.moves`) — um bug conhecido da biblioteca com versões novas do
Python. O `audit-consumer` tem `restart: on-failure`, então ele ficou
crash-looping silenciosamente desde que a stack subiu, sem eu perceber, porque
o comando `docker compose up` reporta "Started" mesmo para um container que
vai cair um segundo depois. Só apareceu quando fui checar `docker logs`
por outro motivo. A correção foi trivial (`python:3.11-slim`), mas o
aprendizado importante foi outro: "container Started" não é o mesmo que
"container funcionando" — depois disso, sempre que subo ou rebuido um serviço
com `restart: on-failure`, confiro o log antes de seguir em frente.

**O bug do `client.region` no catálogo Iceberg do Flink.** Mesmo usando
`S3FileIO` apontando só para o MinIO (sem AWS de verdade), o writer do
Iceberg no Flink falhava com `Unable to load region from any of the
providers in the chain` — o AWS SDK v2 exige resolver uma região para
construir o client S3, independente de o endpoint ser MinIO. A correção foi
declarar `client.region = us-east-1` explicitamente no `CREATE CATALOG`.
Esse fix **desapareceu do arquivo** em algum momento da sessão (provavelmente
perdido entre um restart de container e uma edição concorrente) e o job
voltou a cair semanas — perdão, minutos — depois; tive que resubmeter com a
correção de novo. Isso reforça por que vale conferir o SQL antes de resubmeter
um job depois de qualquer reinício, em vez de assumir que o arquivo em disco
ainda tem o último fix aplicado.

**O teto de memória da VM do Rancher Desktop.** A carga em lote do dataset de
acessos (CSV de ~500 MB, 3,4M linhas) falhou com `OSError: Cannot allocate
memory` na primeira tentativa. A VM do Rancher Desktop vinha com só 7,6 GB
alocados, e Trino sozinho já consome ~2,3 GB quando ativo, mais ~1,2 GB de
Flink ocioso — sobrava pouco para os buffers do `pandas`/`pyarrow` durante a
leitura em chunks. A correção prática foi parar `trino` e os containers do
`flink` durante a carga (não são necessários nesse momento) e reduzir o
tamanho do chunk; a correção durável é aumentar a memória alocada à VM. Fica
documentado em [Rancher Desktop](#rancher-desktop) para não repetir a mesma
investigação.

**A duplicação silenciosa causada por `earliest-offset`.** Toda vez que
cancelei e resubmeti o job do Flink nesta sessão (por causa do bug do
`client.region`, duas vezes), a fonte Kafka usava
`scan.startup.mode = earliest-offset` — ou seja, cada resubmit reprocessava o
tópico `topologia.eventos` inteiro desde o começo, sem saber que aquelas
mensagens já tinham sido consumidas antes. Resultado: a tabela
`topologia_rede`, que devia ter ~500 eventos únicos, acumulou 1.512 linhas
com metade duplicada. Só percebi porque comparei a contagem antes/depois de
mexer em outra coisa e o número não batia com o que eu esperava — de novo,
"o número está diferente do que eu esperava" foi o sinal de alerta, não um
erro explícito. A correção foi trocar para `scan.startup.mode = group-offsets`
com um novo `group.id` (`morfeu-flink-v2`): resubmits agora resumem de onde o
consumer group parou, em vez de reler tudo. O aprendizado: num pipeline que já
foi cancelado/resubmetido manualmente mais de uma vez sem savepoint, é sensato
conferir contagens e duplicatas antes de considerar os dados confiáveis —
"o job está RUNNING" não é o mesmo que "os dados estão corretos".

**Um problema de rede que não era nem do Docker nem do WSL.** Antes de
qualquer um desses bugs, o `docker compose build` do Flink começou a falhar
com timeout de `i/o timeout` puxando a imagem base do Docker Hub — depois de
descartar DNS, WSL2 e firewall como causa, um `tracert` até o IP da AWS usado
pelo Docker Hub mostrou o pacote morrendo dentro da própria rede do provedor
de internet, não em nenhuma camada que eu controlo. Fica como lembrete de que
nem todo erro de infraestrutura tem uma correção de configuração — às vezes o
diagnóstico correto é "não é aqui", e a ação certa é trocar de rede ou
esperar, não continuar mexendo em Docker/WSL à toa.

## Fonte de dados e base legal

Dados do **Portal Brasileiro de Dados Abertos** (dados.gov.br) e dos
**painéis públicos da ANATEL** (estações licenciadas, acessos de banda larga
fixa, velocidade contratada, telefonia móvel, TV por assinatura), mantidos
pela CGU/SGD e pela própria ANATEL. Disponibilização amparada pela Lei nº
12.527/2011 (LAI) e pelo Decreto nº 8.777/2016 (Política de Dados Abertos).

## Licença

MIT — veja [LICENSE](LICENSE).

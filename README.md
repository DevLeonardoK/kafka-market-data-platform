# 📈 Kafka Market Data Platform

Pipeline de dados de mercado (bolsa de valores) que simula a ingestão, o processamento e
a análise de negociações em tempo real, usando **Apache Kafka** como espinha dorsal de
streaming e **Apache Spark Structured Streaming** como motor de processamento em camadas
(arquitetura *medallion*), com destino final na **Google Cloud Platform**.

Um producer Python gera ordens de compra/venda de ações simuladas e publica no Kafka.
A partir daí, três jobs Spark encadeados levam esse dado cru até uma tabela consultável
no BigQuery:

```
Kafka (market_data_raw_topic)
   │
   ▼
spark_bronze  →  GCS (Parquet cru, 1:1 com a mensagem do Kafka)
   │
   ▼
spark_silver  →  GCS (Parquet tipado e limpo: parse do JSON, dedup de espaços, arredondamento de preço)
   │
   ▼
spark_gold    →  BigQuery (tabela final, escrita via Storage Write API)
```

Cada camada roda como um job Spark Structured Streaming próprio, em containers
separados, lendo continuamente o que a camada anterior escreveu.

---

## 🏗️ Arquitetura

- **Kafka em modo KRaft** (sem ZooKeeper): 1 nó *controller* dedicado + 3 *brokers*.
- **9 partições** no tópico `market_data_raw_topic`, criado de forma idempotente por um
  admin client no boot do cluster (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).
- **Spark roda local** (cluster Standalone: 1 master + 3 workers). Decisão de custo:
  Dataproc/GKE cobram por hora rodando; o valor de portfólio está em usar os serviços GCP
  de *free tier* generoso como **destino** do dado, não em pagar por compute na nuvem.
- **Arquitetura Medallion** (bronze → silver → gold): bronze e silver ficam em Parquet no
  GCS; gold é a tabela final no BigQuery, pronta pra consulta/BI.
- O **producer** publica uma mensagem a cada 35s, propositalmente — o objetivo aqui é
  validar o pipeline de ponta a ponta dentro do free tier, não medir throughput.

---

## 🧩 Componentes

| Componente | Papel |
|---|---|
| `kafka_controller` | Controller KRaft (quórum/metadados) |
| `kafka_broker_1/2/3` | Brokers (armazenam as partições) |
| `kafka_admin_client` | Cria o tópico de forma idempotente no boot |
| `market_data_raw_producer_1` | Gera e publica market data no Kafka |
| `apache-spark-master` / `apache-spark-worker-1/2/3` | Cluster Spark Standalone |
| `market_data_spark_bronze` | Kafka → Parquet cru no GCS |
| `market_data_spark_silver` | Parquet cru → Parquet tipado/limpo no GCS |
| `market_data_spark_gold` | Parquet do silver → tabela no BigQuery |

---

## 📊 Modelo de dados (mensagem crua)

Cada mensagem publicada no Kafka representa uma negociação simulada:

```json
{
  "id": 4821337,
  "symbol": "NVDA",
  "price": 187.42,
  "broker": "J.P. Morgan",
  "quantity": 350,
  "currency": "USD",
  "exchange": "NASDAQ",
  "timestamp": "2026-05-14 11:23:07"
}
```

- **Chave da mensagem:** `symbol` → garante que negociações do mesmo ativo caiam na mesma
  partição (ordem preservada por símbolo).

---

## 🚀 Como rodar

**Pré-requisitos:** Docker + Docker Compose, uma service account key do GCP com acesso ao
GCS e BigQuery (`credential/projeto_data_credential.json`, fora do controle de versão) e
os jars do conector Spark-BigQuery/GCS em `jars/` (também fora do controle de versão).

```bash
cd docker

# subir todo o cluster (Kafka + Spark + producer + pipeline bronze/silver/gold)
docker compose up -d --build

# acompanhar os logs de um job específico
docker compose logs -f market_data_spark_gold

# derrubar tudo
docker compose down
```

---

## 🛠️ Stack

`Apache Kafka (KRaft)` · `Apache Spark (Structured Streaming)` · `Python` · `Docker Compose`
· `Google Cloud Platform (GCS · BigQuery)`

---

## 📁 Estrutura do repositório

```
.
├── docker/
│   ├── docker-compose.yml                  # orquestra todo o cluster
│   ├── Dockerfile.kafka_admin_client
│   ├── Dockerfile.market_data_raw_producer_1
│   ├── Dockerfile.apache_spark_bronze
│   ├── Dockerfile.apache_spark_silver
│   └── Dockerfile.apache_spark_gold
├── python/
│   ├── kafka_admin_client.py               # cria o tópico (idempotente)
│   ├── kafka_market_data_raw_producer.py   # gera e publica market data
│   ├── spark_bronze.py                     # Kafka -> Parquet cru (GCS)
│   ├── spark_silver.py                     # Parquet cru -> Parquet tipado/limpo (GCS)
│   └── spark_gold.py                       # Parquet do silver -> BigQuery
├── credential/                             # service account key (gitignored)
├── jars/                                   # conectores Spark-BigQuery/GCS (gitignored)
└── README.md
```

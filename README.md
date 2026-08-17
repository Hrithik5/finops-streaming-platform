# FinOps Real-Time Streaming Platform

A production-style real-time data platform for processing FinOps events from Kafka into analytics-ready Delta tables and exposing business insights through Databricks AI/BI.

The project focuses on the **data engineering and data platform layer** rather than BI development.

---

## Overview

The platform ingests financial events from Kafka and processes them through a layered Delta Lake architecture:

```text
Kafka
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Databricks AI/BI
```

The pipeline supports:

- Incremental Kafka ingestion
- Structured Streaming
- Delta Lake
- Medallion architecture
- Business-key deduplication
- Idempotent Delta MERGE operations
- Checkpoint-based recovery
- Incremental Gold processing
- Scheduled Databricks orchestration
- Business-ready analytics

---

## Architecture

Replace the placeholder below with the final architecture diagram once it has been created.

```md
![FinOps Streaming Platform Architecture](docs/images/architecture.png)
```

---

## Pipeline Flow

The platform processes data through the following stages.

### 1. Event Generation

A controlled event generator produces realistic FinOps lifecycle events.

Supported event types include:

- Merchant
- Customer
- Invoice
- Payment
- Refund
- Chargeback
- Settlement

The generator supports controlled batch creation through configuration:

```python
EVENT_MODE = "NEW_BATCH"
NEW_LIFECYCLE_COUNT = 5
```

Generated events use clean UUID-style identifiers without semantic prefixes.

The generator also randomizes:

- Payment amounts
- Payment statuses
- Event timestamps
- Source/template records

The generated events are published to Confluent Kafka.

---

### 2. Kafka

Kafka acts as the real-time event ingestion layer.

Topics:

```text
chargeback-events
customer-events
invoice-events
merchant-events
payment-events
refund-events
settlement-events
```

Each event is published to its corresponding Kafka topic.

---

### 3. Bronze Layer

Bronze stores raw Kafka records in Delta.

Table:

```text
dev.bronze.raw_events
```

Bronze preserves Kafka metadata such as:

- topic
- partition
- offset
- Kafka timestamp
- raw payload

Bronze is intentionally close to the source data and does not perform business transformations.

---

### 4. Silver Layer

Silver converts raw events into validated business entities.

Tables include:

```text
dev.silver.merchants
dev.silver.customers
dev.silver.invoices
dev.silver.payments
dev.silver.refunds
dev.silver.chargebacks
dev.silver.settlements
```

The Silver pipeline performs:

```text
Bronze
  ↓
Parse
  ↓
Flatten
  ↓
Transform
  ↓
Data Quality Validation
  ↓
Business-Key Deduplication
  ↓
Delta MERGE
```

Validation includes:

- Duplicate detection
- Required-field validation
- Negative-value validation

Duplicate records are handled through business-key deduplication before MERGE so Kafka replay/reprocessing does not create duplicate business entities.

---

### 5. Gold Layer

Gold contains business-ready analytical datasets.

```text
dev.gold.payment_performance
dev.gold.merchant_performance
dev.gold.gateway_performance
dev.gold.financial_operations
```

Gold is incrementally maintained using affected business keys rather than rebuilding the entire dataset on every run.

Examples:

```text
payment_performance
→ one row per payment

merchant_performance
→ one row per merchant

gateway_performance
→ one row per gateway

financial_operations
→ one row per operation date
```

---

## Data Model

### Payment Performance

Grain:

```text
1 row = 1 payment
```

Primary business key:

```text
payment_id
```

Used for payment-level operational and financial analysis.

---

### Merchant Performance

Grain:

```text
1 row = 1 merchant
```

Business key:

```text
merchant_id
```

Contains merchant-level payment and financial performance metrics.

---

### Gateway Performance

Grain:

```text
1 row = 1 gateway
```

Business key:

```text
gateway_id
```

Used to compare payment gateway activity.

---

### Financial Operations

Grain:

```text
1 row = 1 operation date
```

Business key:

```text
operation_date
```

Used for financial trend analysis and operational exposure such as refunds and chargebacks.

---

## Incremental Processing

The platform is designed around incremental processing.

### Bronze

Kafka is read through Structured Streaming.

```python
spark.readStream
```

The Bronze writer uses:

```python
.trigger(availableNow=True)
```

Each execution processes the Kafka data currently available and then stops.

The Kafka source position is persisted through the streaming checkpoint.

---

### Silver

Silver reads Bronze incrementally:

```python
spark.readStream.table(
    "dev.bronze.raw_events"
)
```

Each microbatch is processed using `foreachBatch`.

Only the newly available Bronze records are handled.

Business-key deduplication is performed before the Delta MERGE.

---

### Gold

Gold also processes incremental changes.

The Gold pipeline identifies affected entities such as:

```text
payment_id
merchant_id
gateway_id
operation_date
```

Only the affected Gold records are rebuilt and merged.

This avoids unnecessary full-table recomputation.

---

## Checkpoints

Structured Streaming checkpoints preserve processing state and progress.

Examples:

```text
/Volumes/dev/stream/streaming_checkpoints/bronze_raw_events_v2
/Volumes/dev/stream/streaming_checkpoints/silver
/Volumes/dev/stream/streaming_checkpoints/gold_payment
```

Checkpoints allow the streaming jobs to remember which data has already been processed.

For Kafka, this means the pipeline does not need to start from the beginning of the topic on every scheduled run.

### Recovery Behavior

If a task fails and is retried, the checkpoint allows Spark to resume from the previously committed state.

The project was also tested against Kafka topic recreation and checkpoint/offset mismatches. In those situations, checkpoints must be reset deliberately when the Kafka topic history no longer matches the stored offsets.

---

## Deduplication and Idempotency

Two different problems are handled separately.

### Duplicate events inside a microbatch

A single microbatch can contain multiple records with the same business key.

Before MERGE, Silver keeps the latest record using Kafka metadata such as:

```text
kafka_timestamp
partition
offset
```

This guarantees at most one source record per merge key.

---

### Replayed events

Kafka replay may contain the same business entity again with a new Kafka offset.

Bronze keeps the replayed event because Bronze is a raw ingestion layer.

Silver prevents the replay from creating another business row by merging on the business key.

Example:

```text
payment_id = ABC123
```

The same payment can appear again with a different Kafka offset.

Silver performs:

```text
MERGE ON payment_id
```

instead of using Kafka offset as the business identity.

This makes the Silver layer idempotent.

---

## Data Quality

The Silver layer performs validation before writing curated business data.

Checks include:

### Duplicate events

Duplicate business records are detected and handled through business-key deduplication.

### Required columns

Required identifiers are validated for null values.

Examples:

```text
payment_id
merchant_id
customer_id
invoice_id
```

### Negative values

Financial amount fields are checked to prevent invalid negative values.

Critical validation failures fail the pipeline instead of silently producing invalid Gold data.

---

## Orchestration

The pipeline is orchestrated with a Databricks Job.

Current task structure:

```text
Kafka_Ingestion
        ↓
Silver_Processing_Kafka ───┐
                            ↓
Static_Ingestion → Silver_Processing_Static
                            ↓
                      Gold_Processing
```

The Job is configured to execute on a recurring interval.

Current configuration:

```text
Every 5 minutes
```

Because the individual streaming queries use checkpointed `availableNow` processing, each scheduled execution processes the newly available data and then completes.

---

## How to Run

### 1. Configure Kafka credentials

Set the Kafka connection configuration for the event generator.

The generator reads:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_API_KEY
KAFKA_API_SECRET
```

Do not commit credentials to GitHub.

---

### 2. Configure event generation

Edit:

```text
event_generator/config/settings.py
```

Example:

```python
EVENT_MODE = "NEW_BATCH"
NEW_LIFECYCLE_COUNT = 5
```

---

### 3. Run the event generator

From the project root:

```bash
python event_generator/main.py
```

The generator prints:

- event counts by topic
- total events
- publishing duration
- throughput

---

### 4. Run the Databricks pipeline

The Databricks Job processes:

```text
Kafka
→ Bronze
→ Silver
→ Gold
```

The Job is designed to run incrementally using checkpoints.

For a normal run, do not reset checkpoints.

---

## Validation Queries

### Bronze counts

```sql
SELECT
    topic,
    COUNT(*) AS event_count
FROM dev.bronze.raw_events
GROUP BY topic
ORDER BY topic;
```

### Silver payment grain

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT payment_id) AS distinct_payments
FROM dev.silver.payments;
```

Expected:

```text
rows = distinct_payments
```

### Gold payment grain

```sql
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT payment_id) AS distinct_payments
FROM dev.gold.payment_performance;
```

Expected:

```text
rows = distinct_payments
```

### Prefixed ID validation

```sql
SELECT COUNT(*) AS bad_ids
FROM dev.gold.payment_performance
WHERE payment_id LIKE '%-batch-%';
```

Expected:

```text
0
```

---

## Analytics Layer

The analytics layer uses Databricks AI/BI on top of the Gold tables.

The dashboard includes:

- Payment Count
- Total Payment Amount
- Successful Payment Count
- Successful Payment Rate
- Payment Status Distribution
- Weekly Payment Volume
- Top Merchants by Payment Volume
- Gateway Performance
- Net Payment Trend
- Refund Amount
- Chargeback Amount

The purpose of the analytics layer is to demonstrate how engineered Gold data can be consumed as business insights.

---

## Screenshots

### Databricks Job

Replace the placeholder path below with the final screenshot location.

```md
![Databricks FinOps Data Pipeline Job](docs/images/databricks-job.png)
```

### Databricks AI/BI Dashboard

Replace the placeholder path below with the final screenshot location.

```md
![FinOps AI/BI Dashboard](docs/images/finops-dashboard.png)
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Event Generation | Python |
| Event Streaming | Apache Kafka / Confluent Cloud |
| Streaming Processing | PySpark Structured Streaming |
| Lakehouse | Databricks + Delta Lake |
| Catalog | Unity Catalog |
| Orchestration | Databricks Jobs |
| Data Quality | PySpark validation |
| Analytics | Databricks AI/BI |
| Storage Architecture | Medallion Architecture |

---

## Project Structure

```text
finops-streaming-platform/
│
├── config/
│
├── data/
│
├── databricks/
│   └── notebooks/
│       ├── 01_bronze_ingestion.py
│       ├── 01_static_ingestion.py
│       ├── 01_static_silver_processing.py
│       ├── 02_silver_processing.py
│       └── 03_gold_processing.py
│
├── event_generator/
│   ├── config/
│   ├── builder.py
│   ├── batch_generator.py
│   ├── loader.py
│   ├── main.py
│   └── producer.py
│
├── streaming/
│   ├── bronze/
│   │   └── ingestion.py
│   │
│   ├── silver/
│   │   ├── parser.py
│   │   ├── quality.py
│   │   ├── schemas.py
│   │   ├── transformer.py
│   │   └── writer.py
│   │
│   └── gold/
│       ├── incremental_metrics.py
│       ├── metrics.py
│       └── writer.py
│
├── tests/
│
├── docs/
│
├── pyproject.toml
└── README.md
```

---

## Key Engineering Decisions

### Bronze is raw

Bronze preserves the Kafka event and Kafka metadata rather than applying business transformations.

### Silver owns business correctness

Parsing, validation, normalization, business-key deduplication, and idempotent MERGE operations happen in Silver.

### Gold is business-oriented

Gold exposes datasets at business-friendly grains for analytics.

### Checkpoints provide incremental state

The platform relies on Structured Streaming checkpoints instead of repeatedly re-reading all historical Kafka data.

### Business keys define identity

Kafka offsets identify transport records. Business keys identify business entities.

This distinction is fundamental to making the Silver layer replay-safe.

---

## Project Validation

The pipeline was tested with:

- Controlled lifecycle batches
- Incremental event ingestion
- Replayed Kafka data
- Kafka topic recreation and checkpoint reset scenarios
- Same-batch duplicate keys
- Large event batches
- Scheduled five-minute executions
- Gold grain validation
- Data quality failures and recovery

The final pipeline completed scheduled end-to-end runs successfully without errors.

---

## Future Improvements

Potential future extensions include:

- Apache Flink for alternative streaming workloads
- Advanced Kafka partitioning strategies
- Schema Registry integration
- Dead-letter queues
- More sophisticated data quality quarantine
- Additional Gold analytical domains
- Production monitoring and alerting
- CI/CD deployment for Databricks resources

---

## Author

**Hrithik**

Cloud & Data Engineer

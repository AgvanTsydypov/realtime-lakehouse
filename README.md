# realtime-lakehouse

Learning project: a real-time lakehouse built on streaming data.
Stack — Apache Spark, Databricks, and Kubernetes.

## Architecture

A live data feed lands in Kafka and Spark Structured Streaming on
Kubernetes, is written to Delta (bronze), processed in Databricks
(silver → gold + ML), and served to a dashboard.

<!-- add the architecture diagram from docs/ here -->

## Stack

- **Apache Spark** — distributed processing (batch + streaming)
- **Databricks** — lakehouse, Delta Lake, ML with MLflow
- **Kubernetes** — orchestration and the streaming layer

## Data

<!-- data source: NYC Taxi / Wikipedia / crypto -->

## Structure

- `notebooks/`  — Databricks notebooks (Phases 1–2)
- `spark-jobs/` — local Spark jobs (Phase 3)
- `k8s/`        — Kubernetes manifests (Phase 4)
- `airflow/`    — orchestration DAGs (Phase 4)
- `docs/`       — diagrams and notes

## Roadmap

- [x] Phase 1 — Spark on Databricks: reads, aggregations, query plan
- [x] Phase 2 — Delta Lake, bronze/silver/gold medallion, MLflow
- [x] Phase 3 — the same Spark locally (OSS PySpark)
- [ ] Phase 4 — Spark on Kubernetes + Airflow

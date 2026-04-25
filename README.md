# 🎵 AzureSpotify — End-to-End Data Engineering Pipeline

A production-grade data pipeline that incrementally ingests Spotify streaming data from Azure SQL into a **Bronze → Silver → Gold** Medallion Architecture using **Azure Data Factory** and **Databricks Delta Live Tables**.

---

## 🏗️ Architecture Overview

```
Azure SQL Database
       │
       ▼ (Incremental CDC via ADF)
Azure Data Lake Gen2 — Bronze Layer (Parquet / Snappy)
       │
       ▼ (Auto Loader + Structured Streaming — Databricks)
Azure Data Lake Gen2 — Silver Layer (Delta Tables)
       │
       ▼ (DLT apply_changes — SCD Type 1 / 2)
Azure Data Lake Gen2 — Gold Layer (Delta Live Tables)
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Databricks (PySpark, Structured Streaming) |
| Gold Layer | Databricks Delta Live Tables (DLT) |
| Data Format | Parquet (Snappy), Delta |
| CDC Tracking | JSON watermark files on Data Lake |
| Alerting | Azure Logic Apps (HTTP webhook) |
| Security | ADF System-Assigned Managed Identity |
| Version Control | GitHub (ADF Git integration) |

---

## 📂 Project Structure

```
AzureSpotify/
├── dataset/
│   ├── AzureSql.json           # ADF dataset — Azure SQL source
│   ├── Json_dynamic.json       # Parameterised JSON dataset (CDC watermark files)
│   └── Parquet_dynamic.json    # Parameterised Parquet dataset (Bronze sink)
├── factory/
│   └── df-spotifyy.json        # ADF factory config (East US, System Identity)
├── linkedService/
│   ├── azure_sql.json          # Linked service — Azure SQL Database
│   └── datalake.json           # Linked service — ADLS Gen2
├── pipeline/
│   ├── incremental_ingestion.json       # Core CDC pipeline (single table)
│   └── incremental_ingestion_loop.json  # Orchestrator — loops all 5 tables + alerting
├── utils/
│   └── transformations.py      # Reusable PySpark transformation utilities
└── src/
    ├── silver/
    │   └── silver_Dim.ipynb    # Databricks notebook — Bronze → Silver transformation
    └── gold/
        └── dlt/transformations/
            ├── DimDate.py      # DLT SCD Type 2 — Date dimension
            ├── DimTrack.py     # DLT SCD Type 2 — Track dimension
            ├── DimUser.py      # DLT SCD Type 2 — User dimension
            └── FactStream.py   # DLT SCD Type 1 — Fact table (streaming events)
```

---

## 🔄 How the Pipeline Works

### 1. Incremental Ingestion (Bronze Layer) — ADF

The `incremental_ingestion_loop` pipeline orchestrates incremental loads for **5 tables** in sequence:

- `DimUser`, `DimTrack`, `DimDate`, `DimArtist`, `FactStream`

For **each table**, the inner `incremental_ingestion` pipeline:

1. **Reads the last CDC watermark** from a JSON file stored in ADLS (`{table}_cdc/cdc.json`)
2. **Queries only new/updated rows** from Azure SQL using a dynamic SQL expression:
   ```sql
   SELECT * FROM {schema}.{table}
   WHERE {cdc_col} > '{last_watermark}'
   ```
3. **Writes results as Parquet** (Snappy-compressed) into the Bronze container, partitioned by table and timestamped filename
4. **Checks if any data was read** via an `IfCondition` activity:
   - **No data** → deletes the empty Parquet file to keep the lake clean
   - **Data found** → queries `MAX({cdc_col})` from SQL and updates the CDC watermark JSON for the next run
5. **Sends an alert** via Azure Logic Apps webhook after all tables complete (success, failure, or skip)

The pipeline supports **backfill** via an optional `from_date` parameter — if provided, it overrides the stored watermark and re-ingests data from that date forward. This is useful for:
- Re-processing historical data after a bug fix or schema change
- Onboarding a new table for the first time with a custom start date
- Recovering from a failed pipeline run that corrupted the watermark

The logic is expressed in ADF as:
```
if(empty(pipeline().parameters.from_date), activity('last-cdc').output.value[0].cdc, pipeline().parameters.from_date)
```
If `from_date` is empty, the stored CDC watermark is used. If it's set, it takes precedence — giving full control over the ingestion window without modifying any pipeline code.

### 2. Silver Layer — Databricks Structured Streaming

The `silver_Dim` notebook uses **Auto Loader** (`cloudFiles`) to stream new Parquet files arriving in Bronze into Silver Delta tables:

- Handles **schema evolution** automatically (`addNewColumns`)
- Checkpointing ensures **exactly-once processing**
- `trigger(once=True)` runs as a batch for cost efficiency
- Applied transformations via the shared `reusable` utility class (`utils/transformations.py`):
  - Drops `_rescued_data` system column (via `reusable.dropColumns()`)
  - Deduplicates by primary key (`user_id`, `track_id`, `artist_id`)
  - Adds `durationFlag` derived column on tracks (`low` < 150s / `medium` < 300s / `high` ≥ 300s)
  - Cleans `track_name` by replacing hyphens with spaces

The `reusable` class in `utils/transformations.py` acts as a shared transformation library, making it easy to apply consistent cleaning operations across all dimension and fact notebooks without code duplication.

### 3. Gold Layer — Databricks Delta Live Tables (DLT)

DLT pipelines read from Silver and apply **CDC flows** to maintain Gold dimension and fact tables:

| Table | Primary Key | Sequence Column | SCD Type |
|---|---|---|---|
| `DimDate` | `date_key` | `date` | **Type 2** (full history) |
| `DimTrack` | `track_id` | `updated_at` | **Type 2** (full history) |
| `DimUser` | `user_id` | `updated_at` | **Type 2** (full history) |
| `FactStream` | `stream_id` | `stream_timestamp` | **Type 1** (overwrite) |

SCD Type 2 on dimensions preserves historical records with `__START_AT` and `__END_AT` columns, enabling point-in-time analysis of user and track attributes.

---

## 🗃️ Data Model

The Gold layer follows a **Star Schema**:

```
         DimDate
            │
DimUser ──► FactStream ◄── DimTrack
                              │
                           DimArtist
```

---

## 🚀 Key Features

- ✅ **Incremental CDC** — only processes changed rows using timestamp watermarking
- ✅ **Backfill support** — `from_date` parameter overrides the stored watermark for re-processing or recovery
- ✅ **Parameterised, reusable ADF datasets** — one dataset definition handles all tables
- ✅ **Empty file cleanup** — automatically deletes zero-row Parquet files
- ✅ **Shared transformation utilities** — `reusable` class in `utils/transformations.py` standardises cleaning across all notebooks
- ✅ **Auto Loader with schema evolution** — handles upstream schema changes gracefully
- ✅ **SCD Type 2 on dimensions** — full historical tracking via DLT `apply_changes`
- ✅ **Managed Identity authentication** — no secrets or passwords in code
- ✅ **Pipeline alerting** — Logic Apps webhook notifies on pipeline completion/failure
- ✅ **ADF Git integration** — full CI/CD pipeline version control

---

## 🔧 Setup & Configuration

### Prerequisites
- Azure Subscription
- Azure Data Factory instance
- Azure Data Lake Storage Gen2 account with containers: `bronze`, `silver`, `gold`
- Azure SQL Database with Spotify schema tables
- Databricks workspace with Unity Catalog (`spotify` catalog)
- Azure Logic Apps for alerting (optional)

### ADF Configuration
1. Import the `linkedService/`, `dataset/`, and `pipeline/` JSON files into your ADF instance
2. Update connection strings in `linkedService/azure_sql.json` and `linkedService/datalake.json`
3. Initialise CDC watermark files: create `{table}_cdc/cdc.json` in the Bronze container for each table with an initial timestamp

### Databricks Configuration
1. Attach `src/silver/silver_Dim.ipynb` to a cluster with access to ADLS Gen2
2. Deploy `src/gold/dlt/transformations/` as a DLT pipeline targeting the `spotify.gold` schema

---

## 📊 Tables Ingested

| Table | CDC Column | Description |
|---|---|---|
| `DimUser` | `updated_at` | Spotify user profiles |
| `DimTrack` | `updated_at` | Track metadata |
| `DimDate` | `date` | Date dimension |
| `DimArtist` | `updated_at` | Artist metadata |
| `FactStream` | `stream_timestamp` | Streaming events / play history |

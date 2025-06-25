
# Contoso Ingestion PoC  
**Author: Claudia Benítez Cuadra**

## Overview

This repository contains a proof of concept (PoC) for a reusable, metadata-driven ingestion framework designed for the company **Contoso**. It demonstrates the ingestion of structured data into a modern data platform using **Azure Data Factory (ADF)** for orchestration and **Databricks (PySpark)** for data transformation and ingestion into Delta Lake.

The core objective of this solution is to enable scalable, configurable, and auditable ingestion from diverse data sources into a layered architecture with a **Raw zone (ADLS)** and a **Data Hub zone (Delta Lake)**.

---

## Solution Architecture

The base flow consists of:

1. An **Azure Data Factory pipeline** (not included in this repo, described below).
2. Two **Databricks notebooks**:
   - `Contoso_pipeline_notebook`: notebook triggered by ADF with parameterized logic.
   - `Contoso_demo`: notebook with a local demo simulating the transformation logic.

### Pipeline Parameters

| Parameter | Description |
|----------|-------------|
| `RESTART` | Controls whether to reprocess (`0`) start the cycle from the beginning extracting from the source or (`1`) start the cycle when the file is already in Data Lake Raw and we want to transform and upload it to Delta Lake |
| `JOBID`   | Ingestion identifier, example: `DING0001` |
| `ODATE`   | Ingestion date, example. `20250622` (used for partitioning) |

---

## Flow Description

1. **Metadata Retrieval**  
   Metadata is stored in a `.csv` file in an ADLS container (`metadata`). It defines ingestion logic with the following fields:
   - `jobid`: identifier for the ingestion.
   - `origin`: data source type (file, DB, Kafka or OLAP Cube).
   - `origin_details`: configuration (origin connection info).
   - `country`: organizational parameter.
   - `filename`: source filename.

2. **Parameter Filtering & Preparation**  
   The pipeline filters metadata by `jobid` and sets key variables (timestamp (used for partitioning) and raw path ).

3. **Copy to Raw (Conditional)**  
   If `RESTART = 0`, the pipeline copies the file from `landing` to `raw` in `.parquet` format, adding a timestamp-based partition.
   Why Parquet in Raw? 
   - Parquet is a **columnar, compressed, and schema-aware** format, ideal for big data processing.
   - It ensures **fast read performance**, **efficient storage**, and compatibility with the **Delta Lake** layer.
   - By converting raw ingested filesto .parquet we **standardize the ingestion format** across diverse origins.
   - This simplifies the transformation logic, facilitates schema enforcement, and improves traceability—**the raw content is preserved**, but wrapped in a uniform structure.

4. **Databricks Processing**  
   The Databricks notebook is triggered to:
   - Read the `.parquet` file from raw container.
   - Apply transformations (example: deduplication, anonymization).
   - Store the result as **Delta format** in the Data Hub.

---

## Metadata-Driven Features

- Ingestion sources are configured via metadata.
- Easily extensible to new sources, formats, or transformation rules.
- Designed for reusability and low-code operation.

---

## Why This Approach?

- **Modular & Scalable**: Separate logic from configuration via metadata.
- **Efficient Storage**: Use of `.parquet` in Raw and Delta in Hub.
- **Auditability**: Preserves original data in raw and tracks ingestion steps.
- **ADF + Databricks Combo**: Combines graphical orchestration with powerful PySpark transformations.
- **Supports Reprocessing**: Via `RESTART` parameter.

---

## Future Roadmap

- Move metadata to a centralized database (SQL Server or Cosmos DB).
- Add business APIs for source registration without technical support.
- Support advanced ingestion types (OLAP, streaming, query-based).
- Include per-file schemas for structured ingestion and data type management.

---

## Notebooks Included

- `Contoso_pipeline_notebook`: 
  - Parameterized and integrated with ADF.
  - Connects to ADLS and processes real data.
- `Contoso_demo`: 
  - Simplified version for local execution.
  - Contains hardcoded paths for testing without Azure setup.

---

## Notes

- The ADF pipeline is included in this repository under pipelines/ingestcontoso_metadataDriven.json. Although the pipeline cannot be executed directly from GitHub, the JSON can be imported into any Azure Data Factory environment to reproduce the orchestration.
- This PoC focuses on a `file` origin but is designed to support others like:
  - **Databases** (via JDBC or ADF Copy Activity)
  - **Kafka** (via streaming ingestion)
  - **OLAP cubes** (via MDX queries with Azure Function or PyADOMD)

---

## Contact

For more information or questions, contact:  
**Claudia Benítez Cuadra**


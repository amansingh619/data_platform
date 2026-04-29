# Data Platform Assignment

## Overview

This project implements a simple end-to-end data platform with **Bronze → Silver → Gold** layers.
It ingests data from source files, applies transformations, and serves aggregated datasets for downstream use cases.

---

## How to Run

### 1. Manual Trigger (Python Script)

Use this if you want to run the pipeline directly without orchestration:

```bash
python data_platform/main.py
```

---

### 2. Using Dagster

Run the orchestration layer via Dagster:

```bash
dagster dev -f dagster_app/definitions.py
```

Then open:

```
http://localhost:3000
```

Go to **Assets → Materialize All** to execute the pipeline.

---

## Data Layers

* **Bronze**
  Raw ingestion from source (CSV/Oracle mock) with:

  * `row_hash` for change detection
  * `ingestion_ts` for tracking

* **Silver**
  Cleaned and deduplicated data:

  * latest records only
  * normalized fields
  * basic data quality checks

* **Gold**
  Final datasets:

  * Risk dataset 
  * Monthly analytics dataset

---

## Key Features

* Incremental loading using **hash-based CDC (change data capture)**
* No full table reloads
* Modular structure (connectors + processing + orchestration)
* Dagster-based orchestration with asset dependencies

---

## Future Improvements

* Improve error handling (currently basic try/except)
* Enhance logging (more structured + better visibility)
* Remove hardcoded `LOAD_FILE` list (make it dynamic/config-driven)
* Add proper function docstrings across the codebase

---

## Notes

* CSVs are used to simulate Oracle query outputs
* Postgres is used as the storage layer
* Designed to be easily extendable for real DB ingestion

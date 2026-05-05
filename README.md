# DataVow + Dremio Integration Guide
## Data Contract Enforcement for the Lakehouse

> **CitiBike Reference Implementation**  
> DataVow v0.4.0 · Dremio 26.x · dbt-dremio 1.10.0 · Python 3.12 · EC2 Amazon Linux

---

## Table of Contents
1. [Introduction to DataVow](#1-introduction-to-datavow)
2. [How Dremio Benefits from DataVow](#2-how-dremio-benefits-from-datavow)
3. [Architecture Overview](#3-architecture-overview)
4. [Prerequisites and Installation](#4-prerequisites-and-installation)
5. [Project Directory Structure](#5-project-directory-structure)
6. [Complete File Configurations](#6-complete-file-configurations)
7. [DataVow Contract Files](#7-datavow-contract-files)
8. [Running the Complete Pipeline](#8-running-the-complete-pipeline)
9. [Final Test Results](#9-final-test-results)
10. [Quick Reference](#10-quick-reference)

---

## 1. Introduction to DataVow

DataVow is an open-source data contract enforcement tool that brings structured quality assurance to modern data pipelines. Built on the philosophy of *"a solemn vow on your data"*, it allows data teams to define what their data should look like — and automatically verify that it actually does.

### What DataVow Is

DataVow (v0.4.0) is a Python CLI tool that:
- Reads data contracts defined in YAML files following the ODCS v3.1 standard
- Validates data against those contracts using DuckDB as its local query engine
- Generates dbt-native tests (SQL files) from contracts via `datavow dbt sync`
- Runs those dbt tests against any warehouse that dbt supports — including Dremio
- Produces a **Vow Score** (0–100) and a human-readable verdict
- Integrates with CI/CD pipelines via exit codes

### Vow Score and Verdicts

| Verdict | Score | Meaning |
|---|---|---|
| **Vow Kept** | 100/100 | All rules passed — data is fully compliant |
| **Vow Strained** | 80–99 | Minor warnings — acceptable but worth reviewing |
| **Vow Broken** | 50–79 | Significant failures — action required |
| **Vow Shattered** | 0–49 or any CRITICAL fail | Critical violations — pipeline should be blocked |

### Where DataVow Actually Helps

**Data Contract Enforcement** — Makes informal data agreements explicit, versioned, and testable. If a field type changes upstream, the downstream contract immediately flags the violation.

**Pipeline Quality Gates** — `datavow dbt ci --mode dbt-test` blocks a pipeline if data quality rules fail. Prevents bad data from flowing to dashboards and reports.

**Medallion Architecture Validation** — Bronze tolerates raw data issues; silver must resolve them; gold must be analytically correct. DataVow contracts encode exactly these expectations per layer.

**Cross-Team Data Agreements** — In data mesh architectures, DataVow contracts are the formal expression of data product quality guarantees — readable by engineers and analysts, versioned in git, automatically enforced.

---

## 2. How Dremio Benefits from DataVow

### Why DataVow Requires dbt to Connect to Dremio

> DataVow's direct database validation supports only **PostgreSQL and DuckDB**.  
> For cloud warehouses including Dremio, DataVow generates dbt-native test files and lets dbt execute them against the warehouse using your existing dbt adapter and credentials.  
>
> **dbt-dremio connects to Dremio via REST / Arrow Flight SQL on port 443. No JDBC, no Java required.**
>
> - DataVow owns the contract definition and scoring  
> - dbt owns the warehouse execution

The pipeline works in two steps:
1. `datavow dbt sync` reads YAML contracts and generates SQL test files under `tests/datavow/` using dbt's `{{ ref() }}` Jinja syntax
2. dbt executes those tests against Dremio; DataVow parses the results to calculate the Vow Score

---

## 3. Architecture Overview

### Complete Pipeline

```
1. YAML contracts in contracts/ define quality rules per layer
2. 'datavow dbt ci --mode dbt-test' triggers the full pipeline
3. DataVow syncs contracts → SQL test files in tests/datavow/
4. DataVow invokes 'dbt test --select tag:datavow' as a subprocess
5. dbt connects to Dremio via REST / Arrow Flight SQL using profiles.yml
6. dbt executes each SQL test file against the live Dremio view
7. DataVow parses dbt output → calculates Vow Score and verdict
```

### Data Layers in dremio_catalog.datavow

| Layer | View Name | Source | Purpose |
|---|---|---|---|
| Source | `Samples.samples.dremio.com.citibikes` | External | Raw NYC CitiBike data, 2M rows |
| Bronze | `dremio_catalog.datavow.bronze.brz_citibikes` | dbt view | Snake_case rename only, no type casting |
| Silver | `dremio_catalog.datavow.silver.slv_citibikes` | dbt view | Typed, cleaned, derived fields added |
| Gold | `dremio_catalog.datavow.gold.gld_station_usage` | dbt view | Per-station aggregation |
| Gold | `dremio_catalog.datavow.gold.gld_hourly_demand` | dbt view | Hourly demand by user type |
| Gold | `dremio_catalog.datavow.gold.gld_top_routes` | dbt view | Top A-to-B station pairs |

### Data Quality Facts Discovered from Source

| Metric | Value | Contract Impact |
|---|---|---|
| Total source rows | 2,000,000 | `minimum_row_count` threshold: 1,900,000 |
| Negative duration rows | 355 | WARNING tolerance: 500 in bronze |
| Empty coordinate rows (empty string `''`) | 4,727 | WARNING tolerance: 6,000 in bronze; `NULLIF` fix in silver |
| Trips > 24 hours | 3,294 | Known outlier — not a quality failure |
| Unique start stations | 1,750 | Gold `station_count_minimum`: 1,700 |
| Station IDs with multiple names | 14 | Requires `MAX(start_station_name)` in gold model |
| Blank `start_station_id` rows | 1 | Filtered with `WHERE start_station_id != ''` |
| Timestamp format variants | 2 formats | HH:MM (16 chars) and HH:MM:SS (19 chars) — handled in silver |

---

## 4. Prerequisites and Installation

### System Requirements
- Python 3.12+
- Virtual environment (strongly recommended)
- Access to a Dremio deployment (EKS-hosted or cloud)
- Dremio Personal Access Token (PAT)

### requirements.txt

```
# DataVow -- data contract enforcement
datavow==0.4.0

# dbt -- model execution and test runner
dbt-core==1.11.8
dbt-dremio==1.10.0

# dbt-utils is NOT a pip package -- installed via packages.yml + dbt deps
```

### Installation Steps

Follow these steps in **exact order**. Create all directories and files before running dbt commands.

```bash
# Step 1 -- Create project directory and virtual environment
mkdir datavow && cd datavow
python -m venv .venv
source .venv/bin/activate

# Step 2 -- Install all dependencies
pip install datavow==0.4.0 dbt-core==1.11.8 dbt-dremio==1.10.0
# Note: dbt-utils is NOT a pip package -- installed by dbt deps in Step 6

# Step 3 -- Initialise DataVow project
# This auto-creates contracts/ and datavow.yaml
datavow init citibikes --force
rm contracts/example.yaml

# Step 4 -- Create all remaining directories
mkdir -p dbt
mkdir -p models/bronze models/silver models/gold
mkdir -p tests

# Step 5 -- Create all files (Sections 6 and 7)
# Then verify all files exist before proceeding:
ls -ltr dbt_project.yml
ls -ltr packages.yml
ls -ltr datavow.yaml
ls -ltr dbt/profiles.yml
ls -ltr models/sources.yml
ls -ltr models/schema.yml
ls -ltr models/bronze/brz_citibikes.sql
ls -ltr models/silver/slv_citibikes.sql
ls -ltr models/gold/gld_station_usage.sql
ls -ltr models/gold/gld_hourly_demand.sql
ls -ltr models/gold/gld_top_routes.sql
ls -ltr contracts/citibikes_bronze.yaml
ls -ltr contracts/citibikes_silver.yaml
ls -ltr contracts/citibikes_gold.yaml
# All files must show a timestamp. Any 'No such file' means a file is missing.

# Step 6 -- Install dbt packages (dbt_project.yml and packages.yml must exist)
dbt deps --profiles-dir dbt/

# Step 7 -- Symlink profiles to home directory
# Symlink means edits to dbt/profiles.yml are reflected automatically
mkdir -p ~/.dbt
ln -sf $(pwd)/dbt/profiles.yml ~/.dbt/profiles.yml

# Step 8 -- Suppress SSL warnings (Dremio self-signed cert)
echo 'export PYTHONWARNINGS="ignore:Unverified HTTPS request"' >> ~/.bashrc
source ~/.bashrc
```

---

## 5. Project Directory Structure

```
datavow/
├── dbt_project.yml              # dbt project configuration
├── packages.yml                 # dbt package dependencies (dbt_utils)
├── datavow.yaml                 # DataVow project configuration
├── requirements.txt             # Python package requirements
├── .venv/                       # Python virtual environment
├── dbt/
│   └── profiles.yml             # dbt connection to Dremio
├── models/
│   ├── sources.yml              # defines Samples source for dbt
│   ├── schema.yml               # dbt generic tests
│   ├── bronze/
│   │   └── brz_citibikes.sql
│   ├── silver/
│   │   └── slv_citibikes.sql
│   └── gold/
│       ├── gld_station_usage.sql
│       ├── gld_hourly_demand.sql
│       └── gld_top_routes.sql
├── contracts/
│   ├── citibikes_bronze.yaml    # DataVow contract: bronze layer
│   ├── citibikes_silver.yaml    # DataVow contract: silver layer
│   └── citibikes_gold.yaml      # DataVow contract: gold layer
├── tests/
│   └── datavow/                 # generated by datavow dbt sync (do not edit)
│       ├── brz_citibikes__valid_rideable_type.sql
│       ├── brz_citibikes__minimum_row_count.sql
│       └── ... (12 SQL test files total)
└── target/                      # dbt compilation output (auto-generated)
```

---

## 6. Complete File Configurations

### 6.1 dbt_project.yml

> **Important:** Each layer must have `+dremio_space_folder` defined exactly **once**. Defining it twice causes a `DuplicateYAMLKeysDeprecation` warning and incorrect routing.

```yaml
name: 'dbt_bikes_project'
version: '1.0.0'
profile: 'dbt_bikes_project'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

on-run-start:
  - 'CREATE FOLDER IF NOT EXISTS "dremio_catalog"."datavow"'
  - 'CREATE FOLDER IF NOT EXISTS "dremio_catalog"."datavow"."bronze"'
  - 'CREATE FOLDER IF NOT EXISTS "dremio_catalog"."datavow"."silver"'
  - 'CREATE FOLDER IF NOT EXISTS "dremio_catalog"."datavow"."gold"'

models:
  dbt_bikes_project:
    +materialized: view
    bronze:
      +dremio_space_folder: bronze
    silver:
      +dremio_space_folder: silver
    gold:
      +dremio_space_folder: gold
```

### 6.2 packages.yml

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

### 6.3 datavow.yaml

```yaml
project: citibikes
contracts_dir: contracts/
```

### 6.4 dbt/profiles.yml

```yaml
dbt_bikes_project:
  outputs:
    dev:
      enterprise_catalog_folder: datavow
      enterprise_catalog_namespace: dremio_catalog
      pat: <your-personal-access-token>
      port: <port>
      software_host: <your-dremio-elb-or-hostname>
      threads: 1
      type: dremio
      use_ssl: true
      user: dremio
      verify_ssl: false
  target: dev
```

### 6.5 models/sources.yml

```yaml
version: 2

sources:
  - name: samples_citibike
    database: Samples
    schema: '"samples.dremio.com"'   # inner quotes force dbt to treat it as a single identifier
    quoting:
      database: true
      schema: false                  # false because we're manually quoting above
      identifier: true
    tables:
      - name: citibikes
```

### 6.6 models/schema.yml

```yaml
version: 2

models:
  - name: brz_citibikes
    description: "Raw CitiBike source with snake_case column names only"
    columns:
      - name: tripduration
        tests:
          - not_null
      - name: rideable_type
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['classic_bike', 'electric_bike', 'docked_bike']
      - name: usertype
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['Subscriber', 'Customer']
      - name: start_station_id
        tests: [not_null]
      - name: end_station_id
        tests: [not_null]
      - name: starttime
        tests: [not_null]
      - name: stoptime
        tests: [not_null]

  - name: slv_citibikes
    description: "Cleaned and typed CitiBike data. Negative durations and nulls removed."
    columns:
      - name: start_ts
        tests: [not_null]
      - name: stop_ts
        tests: [not_null]
      - name: trip_duration_seconds
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
      - name: trip_duration_minutes
        tests: [not_null]
      - name: rideable_type
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['classic_bike', 'electric_bike', 'docked_bike']
      - name: usertype
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['Subscriber', 'Customer']
      - name: start_station_id
        tests: [not_null]
      - name: end_station_id
        tests: [not_null]
      - name: trip_hour
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0
                max_value: 23
      - name: trip_month
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
                max_value: 12

  - name: gld_station_usage
    description: "Per-station departure counts and average trip duration"
    columns:
      - name: start_station_id
        tests: [not_null, unique]
      - name: start_station_name
        tests: [not_null]
      - name: trip_count
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
      - name: avg_trip_duration_minutes
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0

  - name: gld_hourly_demand
    description: "Trip counts by month/day/hour/usertype"
    columns:
      - name: trip_month
        tests: [not_null]
      - name: trip_day
        tests: [not_null]
      - name: trip_hour
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0
                max_value: 23
      - name: usertype
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['Subscriber', 'Customer']
      - name: trip_count
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1

  - name: gld_top_routes
    description: "All A to B station pairs ranked by volume"
    columns:
      - name: start_station_id
        tests: [not_null]
      - name: end_station_id
        tests: [not_null]
      - name: trip_count
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 1
      - name: avg_trip_duration_minutes
        tests:
          - not_null
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0
```

### 6.7 models/bronze/brz_citibikes.sql

```sql
select
    tripduration,
    rideable_type,
    starttime,
    stoptime,

    "start station id"        as start_station_id,
    "start station name"      as start_station_name,
    "start station latitude"  as start_station_latitude,
    "start station longitude" as start_station_longitude,

    "end station id"          as end_station_id,
    "end station name"        as end_station_name,
    "end station latitude"    as end_station_latitude,
    "end station longitude"   as end_station_longitude,

    usertype

from {{ source('samples_citibike', 'citibikes') }}
```

### 6.8 models/silver/slv_citibikes.sql

```sql
select
    rideable_type,

    -- station identifiers
    cast(start_station_id as varchar)                       as start_station_id,
    start_station_name,
    cast(nullif(start_station_latitude,  '') as double)     as start_station_latitude,
    cast(nullif(start_station_longitude, '') as double)     as start_station_longitude,

    cast(end_station_id as varchar)                         as end_station_id,
    end_station_name,
    cast(nullif(end_station_latitude,  '') as double)       as end_station_latitude,
    cast(nullif(end_station_longitude, '') as double)       as end_station_longitude,

    -- timestamps: source has mixed precision (HH:MM vs HH:MM:SS)
    case
        when starttime is null then null
        when length(starttime) = 16
            then to_timestamp(replace(starttime,'T',' ')||':00','YYYY-MM-DD HH24:MI:SS',1)
        when length(starttime) = 19
            then to_timestamp(replace(starttime,'T',' '),'YYYY-MM-DD HH24:MI:SS',1)
        else null
    end as start_ts,

    case
        when stoptime is null then null
        when length(stoptime) = 16
            then to_timestamp(replace(stoptime,'T',' ')||':00','YYYY-MM-DD HH24:MI:SS',1)
        when length(stoptime) = 19
            then to_timestamp(replace(stoptime,'T',' '),'YYYY-MM-DD HH24:MI:SS',1)
        else null
    end as stop_ts,

    -- duration
    cast(tripduration as double)          as trip_duration_seconds,
    cast(tripduration as double) / 60.0   as trip_duration_minutes,

    usertype,

    -- derived time parts
    extract(hour from
        case
            when starttime is null then null
            when length(starttime) = 16
                then to_timestamp(replace(starttime,'T',' ')||':00','YYYY-MM-DD HH24:MI:SS',1)
            when length(starttime) = 19
                then to_timestamp(replace(starttime,'T',' '),'YYYY-MM-DD HH24:MI:SS',1)
            else null
        end
    ) as trip_hour,

    extract(day from
        case
            when starttime is null then null
            when length(starttime) = 16
                then to_timestamp(replace(starttime,'T',' ')||':00','YYYY-MM-DD HH24:MI:SS',1)
            when length(starttime) = 19
                then to_timestamp(replace(starttime,'T',' '),'YYYY-MM-DD HH24:MI:SS',1)
            else null
        end
    ) as trip_day,

    extract(month from
        case
            when starttime is null then null
            when length(starttime) = 16
                then to_timestamp(replace(starttime,'T',' ')||':00','YYYY-MM-DD HH24:MI:SS',1)
            when length(starttime) = 19
                then to_timestamp(replace(starttime,'T',' '),'YYYY-MM-DD HH24:MI:SS',1)
            else null
        end
    ) as trip_month

from {{ ref('brz_citibikes') }}
where starttime    is not null
  and stoptime     is not null
  and tripduration is not null
  and cast(tripduration as double) > 0  -- ← remove this line to test failure
```

### 6.9 models/gold/gld_station_usage.sql

```sql
select
    start_station_id,
    MAX(start_station_name)         as start_station_name,
    count(*)                        as trip_count,
    avg(trip_duration_minutes)      as avg_trip_duration_minutes
from {{ ref('slv_citibikes') }}
where start_station_id != ''
group by
    start_station_id
```

### 6.10 models/gold/gld_hourly_demand.sql

```sql
select
    trip_month,
    trip_day,
    trip_hour,
    usertype,
    count(*)                        as trip_count,
    avg(trip_duration_minutes)      as avg_trip_duration_minutes
from {{ ref('slv_citibikes') }}
group by
    trip_month,
    trip_day,
    trip_hour,
    usertype
```

### 6.11 models/gold/gld_top_routes.sql

```sql
select
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    count(*)                        as trip_count,
    avg(trip_duration_minutes)      as avg_trip_duration_minutes
from {{ ref('slv_citibikes') }}
where start_station_id != end_station_id
group by
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name
```

---

## 7. DataVow Contract Files

### Contract Format Rules

> DataVow v0.4.0 has strict format requirements. Violations cause **silent failures** — contracts are skipped with no error message.

- `apiVersion` must be exactly `datavow/v1`
- `kind` must be exactly `DataContract` (capital D and C)
- `schema.fields` types must use DataVow enum values only: `string`, `integer`, `float`, `decimal`, `boolean`, `date`, `timestamp`
- **Do NOT use** warehouse types: `varchar`, `double`, `bigint`
- Field-level rules (`range`, `regex`) require a `field` key
- SQL rules require both `query` and `threshold` keys
- **Do NOT add** `required`, `unique`, or `allowed_values` in `schema.fields` — these generate yml files that conflict with `models/schema.yml`

### 7.1 contracts/citibikes_bronze.yaml

```yaml
apiVersion: datavow/v1
kind: DataContract
metadata:
  name: brz_citibikes
  domain: mobility
  owner: data-team@company.com
  description: "Raw CitiBike source with snake_case rename only."
schema:
  fields:
    - name: tripduration
      type: string
    - name: rideable_type
      type: string
    - name: starttime
      type: string
    - name: stoptime
      type: string
    - name: start_station_id
      type: string
    - name: end_station_id
      type: string
    - name: usertype
      type: string
quality:
  rules:
    - name: valid_rideable_type
      type: sql
      severity: CRITICAL
      query: "SELECT COUNT(*) FROM {table} WHERE rideable_type NOT IN ('classic_bike','electric_bike','docked_bike')"
      threshold: 0
    - name: negative_tripduration_tolerance
      type: sql
      severity: WARNING
      query: "SELECT COUNT(*) FROM {table} WHERE CAST(tripduration AS BIGINT) < 0"
      threshold: 500
    - name: empty_end_coords_tolerance
      type: sql
      severity: WARNING
      query: "SELECT COUNT(*) FROM {table} WHERE end_station_latitude = '' OR end_station_longitude = ''"
      threshold: 6000
    - name: minimum_row_count
      type: row_count
      severity: CRITICAL
      min: 1900000
```

### 7.2 contracts/citibikes_silver.yaml

```yaml
apiVersion: datavow/v1
kind: DataContract
metadata:
  name: slv_citibikes
  domain: mobility
  owner: data-team@company.com
  description: "Cleaned and typed CitiBike data."
schema:
  fields:
    - name: rideable_type
      type: string
    - name: start_station_id
      type: string
    - name: end_station_id
      type: string
    - name: start_ts
      type: string
    - name: stop_ts
      type: string
    - name: trip_duration_seconds
      type: float
    - name: trip_duration_minutes
      type: float
    - name: usertype
      type: string
    - name: trip_hour
      type: integer
    - name: trip_day
      type: integer
    - name: trip_month
      type: integer
quality:
  rules:
    - name: no_negative_duration
      type: sql
      severity: CRITICAL
      query: "SELECT COUNT(*) FROM {table} WHERE trip_duration_seconds <= 0"
      threshold: 0
    - name: start_before_stop
      type: sql
      severity: CRITICAL
      query: "SELECT COUNT(*) FROM {table} WHERE start_ts >= stop_ts"
      threshold: 0
    - name: valid_trip_hour
      type: range
      severity: CRITICAL
      field: trip_hour
      min_value: 0
      max_value: 23
    - name: valid_trip_month
      type: range
      severity: CRITICAL
      field: trip_month
      min_value: 1
      max_value: 12
    - name: minimum_row_count
      type: row_count
      severity: CRITICAL
      min: 1990000
```

### 7.3 contracts/citibikes_gold.yaml

```yaml
apiVersion: datavow/v1
kind: DataContract
metadata:
  name: gld_station_usage
  domain: mobility
  owner: data-team@company.com
  description: "Per-station aggregation. One row per station_id."
schema:
  fields:
    - name: start_station_id
      type: string
    - name: start_station_name
      type: string
    - name: trip_count
      type: integer
    - name: avg_trip_duration_minutes
      type: float
quality:
  rules:
    - name: no_zero_trip_count
      type: sql
      severity: CRITICAL
      query: "SELECT COUNT(*) FROM {table} WHERE trip_count < 1"
      threshold: 0
    - name: positive_avg_duration
      type: sql
      severity: CRITICAL
      query: "SELECT COUNT(*) FROM {table} WHERE avg_trip_duration_minutes <= 0"
      threshold: 0
    - name: station_count_minimum
      type: row_count
      severity: WARNING
      min: 1700
```

---

## 8. Running the Complete Pipeline

### 8.1 Pre-flight Check

```bash
# Verify virtual environment is active
which python                   # should point to .venv/bin/python

# Verify dbt can find profiles and project
dbt debug --profiles-dir dbt/

# Create softlink to ~/.dbt/profiles.yml
mkdir -p ~/.dbt
ln -s dbt/profiles.yml ~/.dbt/profiles.yml

# Confirm profiles symlinked to home directory
ls -l ~/.dbt/profiles.yml
```

### 8.2 Step 1 — Install dbt Packages

```bash
dbt deps --profiles-dir dbt/

# Expected output:
# Installing dbt-labs/dbt_utils
# Installed from version 1.3.3
# Up to date!
```

### 8.3 Step 2 — Build dbt Models

> **Note:** On the first run you will see `"Unable to do partial parsing because saved manifest not found. Starting full parse."` This is normal — dbt is building its manifest for the first time. It will not appear on subsequent runs.

```bash
dbt run --profiles-dir dbt/

# Expected output:
# 1 of 5 OK  brz_citibikes
# 2 of 5 OK  slv_citibikes
# 3 of 5 OK  gld_hourly_demand
# 4 of 5 OK  gld_station_usage
# 5 of 5 OK  gld_top_routes
# Done. PASS=9 WARN=0 ERROR=0
```

### 8.4 Step 3 — Run dbt Schema Tests

```bash
dbt test --profiles-dir dbt/
# Expected: ~45 tests, all PASS
```

### 8.5 Step 4 — Run DataVow Full Pipeline

```bash
datavow dbt ci \
  --contracts contracts/ \
  --dbt-project . \
  --mode dbt-test

# Expected output:
# Step 1/2: Syncing contracts -> dbt tests
#   3 contract(s) -> 12 dbt test(s)
#
# Step 2/2: Running dbt test --select tag:datavow
#   1 of 12 PASS brz_citibikes__valid_rideable_type
#   ...
#   12 of 12 PASS slv_citibikes__valid_trip_month
#
#   ✅ Vow Kept -- Vow Score: 100/100
#   Passed: 12 | Failed: 0 | Warned: 0 | Total: 12
#
# CI PASSED
```

---

## 9. Final Test Results

### dbt Schema Tests
45 tests — all PASS.

### DataVow Tests

| Test Name | Layer | Rule Type | Result |
|---|---|---|---|
| `brz_citibikes__valid_rideable_type` | Bronze | sql | ✅ PASS |
| `brz_citibikes__negative_tripduration_tolerance` | Bronze | sql (WARN) | ✅ PASS |
| `brz_citibikes__empty_end_coords_tolerance` | Bronze | sql (WARN) | ✅ PASS |
| `brz_citibikes__minimum_row_count` | Bronze | row_count | ✅ PASS |
| `slv_citibikes__no_negative_duration` | Silver | sql | ✅ PASS |
| `slv_citibikes__start_before_stop` | Silver | sql | ✅ PASS |
| `slv_citibikes__valid_trip_hour` | Silver | range | ✅ PASS |
| `slv_citibikes__valid_trip_month` | Silver | range | ✅ PASS |
| `slv_citibikes__minimum_row_count` | Silver | row_count | ✅ PASS |
| `gld_station_usage__no_zero_trip_count` | Gold | sql | ✅ PASS |
| `gld_station_usage__positive_avg_duration` | Gold | sql | ✅ PASS |
| `gld_station_usage__station_count_minimum` | Gold | row_count | ✅ PASS |

### ✅ Vow Kept — Vow Score: 100/100 — CI PASSED

---

## 10. Quick Reference

### Daily Commands

```bash
# Rebuild all views after model changes
dbt run --profiles-dir dbt/

# Run dbt schema tests
dbt test --profiles-dir dbt/

# Run DataVow contracts against live Dremio (primary command)
datavow dbt ci --contracts contracts/ --dbt-project . --mode dbt-test

# Rebuild and retest a single model
dbt run  --profiles-dir dbt/ --select gld_station_usage
dbt test --profiles-dir dbt/ --select gld_station_usage

# Manually regenerate DataVow test SQL files
datavow dbt sync --contracts contracts/ --dbt-project . --clean
rm -f tests/datavow/*.yml    # prevent schema.yml conflict
```

### DataVow FieldType Reference

| DataVow Type | Maps to Dremio Type | Notes |
|---|---|---|
| `string` | VARCHAR, TEXT | Use for all text, IDs, and timestamps stored as strings |
| `integer` | INTEGER, BIGINT, SMALLINT | Use for whole numbers including EXTRACT() results |
| `float` | FLOAT, DOUBLE, REAL | Use for decimals including AVG() results |
| `decimal` | DECIMAL, NUMERIC | Use for precise decimal types |
| `boolean` | BOOLEAN | |
| `date` | DATE | |
| `timestamp` | TIMESTAMP, TIMESTAMPTZ | |

### DataVow Rule Type Reference

| Rule Type | Required Fields | Example Use Case |
|---|---|---|
| `sql` | `query`, `threshold` | Any custom SQL check |
| `row_count` | `min` and/or `max` | Minimum row count SLA |
| `range` | `field`, `min_value` or `max_value` | `trip_hour` between 0 and 23 |
| `not_null` | `field` | Better placed in `schema.yml` |
| `unique` | `field` | Better placed in `schema.yml` |
| `accepted_values` | `field`, `values` | Better placed in `schema.yml` |
| `regex` | `field`, `pattern` | Pattern matching on string columns |

---

*Author: Boban Jayan, Dremio Field Engineering — April 2026*

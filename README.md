# IMDb Lakehouse to OLAP Pipeline

Local data pipeline that ingests the IMDb dataset with PySpark, stores it as a partitioned Parquet data lake, loads it into ClickHouse for sub-second analytics, and benchmarks Spark vs OLAP query performance.

## Architecture

```
Kaggle IMDb TSV  →  PySpark ETL  →  Partitioned Parquet Lake  →  ClickHouse OLAP  →  Analytics
   (data/raw/)       (etl_job.py)      (data/lake/)              (load_to_olap.py)    (benchmark.py)
```

**Components:**
- **Spark cluster** (Docker): Master + Worker for distributed ETL and Spark SQL benchmarks
- **ClickHouse** (Docker): Columnar OLAP engine for high-speed aggregations
- **Parquet lake**: Snappy-compressed, Hive-partitioned storage

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for load and benchmark scripts on host)
- ~4 GB disk space for raw + lake data

## Step 1: Place IMDb Data in `data/raw/`

Download the [IMDb dataset from Kaggle](https://www.kaggle.com/datasets/ashirwadsangwan/imdb-dataset) and place files in `data/raw/`.

The ETL **only reads files it recognizes** — anything else in the folder is ignored. Both `.tsv` and `.tsv.gz` work.

| File | Used when present |
|------|-------------------|
| `title.basics.tsv` | **Required** — titles, types, years, runtime, genres |
| `title.ratings.tsv` | Optional — joins ratings onto titles |
| `title.episode.tsv` | Optional — episode hierarchy with season/episode numbers |
| `name.basics.tsv` | Optional — actors, directors, writers (people dimension) |
| `title.akas.tsv` | Optional — alternate / localized titles |
| `title.principals.tsv` | Optional — cast & crew (joined with names + titles) |

With your current files (`title.basics`, `name.basics`, `title.akas`, `title.principals`), the ETL builds **titles, episodes, names, akas, and principals** lakes.

```bash
ls -lh data/raw/
```

## Step 2: Start Infrastructure

```bash
cd /home/surajmaurya/test
docker compose up -d
```

Services:
| Service | URL | Purpose |
|---------|-----|---------|
| Spark Master UI | http://localhost:8081 | Cluster monitoring |
| ClickHouse HTTP | http://localhost:8123 | OLAP queries |
| Spark Master RPC | spark://localhost:7077 | Job submission |

> **Note:** Uses official `apache/spark:3.5.1` (Bitnami Spark images are no longer free on Docker Hub). Spark UI is mapped to host port **8081** to avoid conflicts with other services on 8080.

## Step 3: Run PySpark ETL

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/etl_job.py
```

**What it extracts:**

| Lake dataset | Source file(s) | Key fields |
|--------------|----------------|------------|
| `titles/` | title.basics (+ ratings) | title, type, year, genre, runtime, rating |
| `episodes/` | title.episode or title.basics | episode title, series, season, runtime |
| `names/` | name.basics | person name, profession, birth year |
| `akas/` | title.akas | localized titles, region, language |
| `principals/` | title.principals + names + titles | cast/crew, character, job, title context |

Partitioning:

| Dataset | Path | Partitions |
|---------|------|------------|
| Titles | `data/lake/titles/` | `start_year`, `title_type` |
| Episodes | `data/lake/episodes/` | `start_year`, `primary_genre` |
| Names | `data/lake/names/` | `primary_role` |
| Akas | `data/lake/akas/` | `region` |
| Principals | `data/lake/principals/` | `category` |

**Partitioning rationale:** Year partitions enable time-series analysis (decade trends, release windows). Type/genre partitions enable category pruning for movie vs TV and genre breakdowns.

## Step 4: Load into ClickHouse

From the project folder, create a virtual environment and install dependencies (required on Ubuntu/WSL — do not use system `pip` directly):

```bash
cd /home/surajmaurya/test

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

Make sure ClickHouse is running:

```bash
docker compose ps
curl http://localhost:8123/ping   # should return: Ok.
```

Run the load script (from the same activated venv):

```bash
python load_to_olap.py
```

This applies DDL from `ddl/`, loads Parquet from `data/lake/`, and prints row counts. Expect several minutes for ~135M total rows.

## Step 5: Run Analytics Benchmark

Compare Spark (Parquet scan) vs ClickHouse (OLAP) on five analytics queries.

```bash
source .venv/bin/activate
python benchmark.py
```

No Java needed on the host — Spark benchmarks run automatically inside the Docker cluster. ClickHouse benchmarks run locally via `clickhouse-connect`.

Or run each side separately:

```bash
# ClickHouse only (host)
python benchmark.py --clickhouse-only

# Spark only (Docker — has Java built in)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/benchmark.py --spark-only
```

Sample ClickHouse queries (including extras not in the benchmark) are in `analytics/sample_queries.sql`.

## Benchmark Queries

`benchmark.py` runs the same five analytics queries on **Spark** (scanning `data/lake/` Parquet) and **ClickHouse** (querying `imdb.*` MergeTree tables), then prints side-by-side timings.

### Q1: Top rated movies by decade

Highest-rated movies with at least 10k votes, grouped by decade.

```sql
SELECT
    toUInt32(floor(start_year / 10) * 10) AS decade,
    primary_title,
    start_year,
    average_rating,
    num_votes
FROM imdb.titles
WHERE title_type = 'movie'
  AND num_votes >= 10000
  AND average_rating IS NOT NULL
  AND start_year > 0
ORDER BY decade DESC, average_rating DESC
LIMIT 20;
```

Spark equivalent: filter `titles` Parquet on `title_type`, `num_votes`, compute `decade`, order and limit 20.

### Q2: Average rating by title type

Compare average ratings across movies, TV series, episodes, etc.

```sql
SELECT
    title_type,
    round(avg(average_rating), 2) AS avg_rating,
    count() AS title_count
FROM imdb.titles
WHERE average_rating IS NOT NULL
GROUP BY title_type
ORDER BY title_count DESC;
```

Spark equivalent: groupBy `title_type`, avg/count on Parquet `titles`.

### Q3: Top TV series by episode count

Series with the most episodes and average episode runtime.

```sql
SELECT
    series_title,
    count() AS episode_count,
    round(avg(runtime_minutes), 1) AS avg_runtime_minutes
FROM imdb.episodes
WHERE series_title IS NOT NULL AND series_title != ''
GROUP BY series_title
ORDER BY episode_count DESC
LIMIT 10;
```

Spark equivalent: groupBy `series_title` on Parquet `episodes`.

> **Note:** Without `title.episode.tsv`, `series_title` may be empty and this query returns few or no rows.

### Q4: Genre popularity (last 20 years)

Title count and average rating by genre for recent releases.

```sql
SELECT
    primary_genre,
    count() AS title_count,
    round(avg(average_rating), 2) AS avg_rating
FROM imdb.titles
WHERE start_year >= (toYear(today()) - 20)
  AND start_year > 0
  AND average_rating IS NOT NULL
GROUP BY primary_genre
ORDER BY title_count DESC
LIMIT 20;
```

Spark equivalent: filter `start_year >= 2006`, groupBy `primary_genre`.

### Q5: Average runtime by genre (movies)

Average and median movie runtime per genre (genres with 100+ movies).

```sql
SELECT
    primary_genre,
    round(avg(runtime_minutes), 1) AS avg_runtime,
    quantile(0.5)(runtime_minutes) AS median_runtime,
    count() AS movie_count
FROM imdb.titles
WHERE title_type = 'movie'
  AND runtime_minutes IS NOT NULL
  AND runtime_minutes > 0
GROUP BY primary_genre
HAVING movie_count >= 100
ORDER BY avg_runtime DESC
LIMIT 20;
```

Spark equivalent: filter movies, groupBy `primary_genre`, percentile on `runtime_minutes`.

## Performance Results

Measured locally on the full IMDb dataset (~12.2M titles, ~9.5M episodes). Spark reads Snappy Parquet from the lake; ClickHouse serves the same queries from MergeTree tables. Re-run `python benchmark.py` to reproduce.

| Query | Spark (ms) | ClickHouse (ms) | Speedup |
|-------|-----------|-----------------|---------|
| Q1: Top rated movies by decade | 2611.2 | 74.6 | 35.0x |
| Q2: Avg rating by title type | 2315.4 | 80.1 | 28.9x |
| Q3: Top TV series by episode count | 3063.0 | 96.3 | 31.8x |
| Q4: Genre popularity (last 20 years) | 456.7 | 38.9 | 11.7x |
| Q5: Avg runtime by genre (movies) | 1810.8 | 73.7 | 24.6x |

**Average: Spark 2051 ms vs ClickHouse 73 ms — ClickHouse is ~28x faster.** Exact numbers vary by hardware.

Example benchmark output:

```
========================================================================
Query                                      Spark (ms)  ClickHouse (ms)  Speedup
========================================================================
Q1: Top rated movies by decade                 2611.2             74.6    35.0x
Q2: Avg rating by title type                   2315.4             80.1    28.9x
Q3: Top TV series by episode count             3063.0             96.3    31.8x
Q4: Genre popularity (last 20 years)            456.7             38.9    11.7x
Q5: Avg runtime by genre (movies)              1810.8             73.7    24.6x
========================================================================

Average: Spark 2051.4 ms | ClickHouse 72.7 ms
ClickHouse is 28.2x faster on average
```

## Schema Design

### Lake (Parquet)

**titles:** `tconst`, `title_type`, `primary_title`, `original_title`, `start_year`, `end_year`, `runtime_minutes`, `genres`, `primary_genre`, `average_rating`, `num_votes`

**episodes:** `episode_tconst`, `parent_tconst`, `season_number`, `episode_number`, `episode_title`, `series_title`, `start_year`, `primary_genre`, `average_rating`, `num_votes`, `runtime_minutes`

**names:** `nconst`, `primary_name`, `birth_year`, `death_year`, `primary_profession`, `primary_role`, `known_for_titles`

**akas:** `title_id`, `ordering`, `title`, `region`, `language`, `types`, `is_original_title`

**principals:** `tconst`, `ordering`, `nconst`, `primary_name`, `category`, `job`, `characters`, `title_type`, `primary_title`, `primary_genre`, `birth_year`, `primary_role`

### ClickHouse (OLAP)

- **Engine:** MergeTree — columnar storage with sorted primary index
- **Partition keys:** `start_year` (titles/episodes), `primary_role` (names), `region` (akas), `category` (principals)
- **LowCardinality** on repeated string columns

DDL files: `ddl/01_create_database.sql` through `ddl/06_principals.sql`

## Why ClickHouse?

ClickHouse was chosen as the OLAP engine for this pipeline:

1. **Columnar storage** — reads only columns needed for aggregations, not full rows
2. **MergeTree engine** — data sorted on disk by `(title_type, primary_genre, tconst)`, enabling index-based pruning
3. **Partition elimination** — `PARTITION BY start_year` skips irrelevant year ranges
4. **Sub-second aggregations** — designed for analytical queries on hundreds of millions of rows
5. **Native Parquet support** — straightforward load path from the data lake
6. **Lightweight Docker deployment** — single container, no cluster coordination overhead for local dev

Spark excels at distributed ETL and batch processing but incurs JVM startup, Catalyst planning, and full-file scan overhead for interactive queries. ClickHouse keeps data pre-indexed and columnar, making repeated aggregations 5–50x faster for the query patterns in this exercise.

## Project Structure

```
test/
├── docker-compose.yml      # Spark Master/Worker + ClickHouse
├── etl_job.py              # PySpark ETL → Parquet lake
├── load_to_olap.py         # Parquet → ClickHouse loader
├── benchmark.py            # Spark vs ClickHouse timing
├── requirements.txt
├── ddl/                    # ClickHouse DDL (01–06)
├── analytics/              # Sample SQL queries
├── clickhouse/
│   └── users.d/            # Local ClickHouse user config (default, no password)
└── data/
    ├── raw/                # Manual Kaggle download
    └── lake/               # ETL output (gitignored)
```

## Teardown

```bash
docker compose down -v   # removes containers and ClickHouse volume
```

## Troubleshooting

**ETL fails with "Missing raw files"** — ensure `title.basics.tsv` is in `data/raw/`.

**`Mkdirs failed to create file:/data/lake/...`** — Spark runs as a non-root user inside the container and couldn't write to the mounted `data/lake/` folder. Recreate Spark services after pulling the latest `docker-compose.yml` (runs as root and fixes lake permissions on startup):

```bash
docker compose up -d --force-recreate spark-master spark-worker
```

**`TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'`** — fixed in `etl_job.py` (Spark container uses Python 3.8).

**Load fails with connection error** — verify ClickHouse is running: `docker compose ps` and `curl http://localhost:8123/ping`.

**Spark OOM on ETL** — increase worker memory in `docker-compose.yml` (`--memory 8g`).

**Benchmark shows 0 rows** — run ETL and load steps first; verify with `python load_to_olap.py` row counts.

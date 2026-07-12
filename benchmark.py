"""
Benchmark Spark (Parquet scan) vs ClickHouse (OLAP) on identical analytics queries.

Run from host (no Java required — Spark runs in Docker automatically):
  source .venv/bin/activate
  python benchmark.py

Run individually:
  python benchmark.py --clickhouse-only
  docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 /opt/spark/work-dir/benchmark.py --spark-only
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
DEFAULT_LAKE = (
    Path("/data/lake")
    if Path("/data/lake").exists()
    else PROJECT_DIR / "data" / "lake"
)
SPARK_TIMES_FILE = (
    Path("/data/benchmark_spark_times.txt")
    if Path("/data").exists()
    else PROJECT_DIR / "data" / "benchmark_spark_times.txt"
)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
LAKE_DIR = os.environ.get("IMDB_LAKE_DIR", str(DEFAULT_LAKE))
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")

QUERIES = [
    {
        "name": "Q1: Top rated movies by decade",
        "clickhouse": """
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
            LIMIT 20
        """,
    },
    {
        "name": "Q2: Avg rating by title type",
        "clickhouse": """
            SELECT
                title_type,
                round(avg(average_rating), 2) AS avg_rating,
                count() AS title_count
            FROM imdb.titles
            WHERE average_rating IS NOT NULL
            GROUP BY title_type
            ORDER BY title_count DESC
        """,
    },
    {
        "name": "Q3: Top TV series by episode count",
        "clickhouse": """
            SELECT
                series_title,
                count() AS episode_count,
                round(avg(runtime_minutes), 1) AS avg_runtime_minutes
            FROM imdb.episodes
            WHERE series_title IS NOT NULL AND series_title != ''
            GROUP BY series_title
            ORDER BY episode_count DESC
            LIMIT 10
        """,
    },
    {
        "name": "Q4: Genre popularity (last 20 years)",
        "clickhouse": """
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
            LIMIT 20
        """,
    },
    {
        "name": "Q5: Avg runtime by genre (movies)",
        "clickhouse": """
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
            LIMIT 20
        """,
    },
]


def spark_available_locally() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and Path(java_home).exists():
        return True
    return subprocess.run(
        ["bash", "-lc", "command -v java"],
        capture_output=True,
        text=True,
    ).returncode == 0


def should_run_spark_locally() -> bool:
    """Use local PySpark when Java is available or we're already inside Docker."""
    if Path("/.dockerenv").exists():
        return True
    return spark_available_locally()


def time_clickhouse(client, sql: str) -> float:
    start = time.perf_counter()
    client.query(sql)
    return (time.perf_counter() - start) * 1000


def run_spark_benchmarks(spark):
    from pyspark.sql import functions as F

    titles = spark.read.parquet(os.path.join(LAKE_DIR, "titles"))
    episodes = spark.read.parquet(os.path.join(LAKE_DIR, "episodes"))

    results = []

    start = time.perf_counter()
    (
        titles.filter(
            (F.col("title_type") == "movie")
            & (F.col("num_votes") >= 10000)
            & F.col("average_rating").isNotNull()
            & (F.col("start_year") > 0)
        )
        .withColumn("decade", (F.floor(F.col("start_year") / 10) * 10).cast("int"))
        .orderBy(F.desc("decade"), F.desc("average_rating"))
        .limit(20)
        .collect()
    )
    results.append((time.perf_counter() - start) * 1000)

    start = time.perf_counter()
    (
        titles.filter(F.col("average_rating").isNotNull())
        .groupBy("title_type")
        .agg(
            F.round(F.avg("average_rating"), 2).alias("avg_rating"),
            F.count("*").alias("title_count"),
        )
        .orderBy(F.desc("title_count"))
        .collect()
    )
    results.append((time.perf_counter() - start) * 1000)

    start = time.perf_counter()
    (
        episodes.filter(
            F.col("series_title").isNotNull() & (F.col("series_title") != "")
        )
        .groupBy("series_title")
        .agg(
            F.count("*").alias("episode_count"),
            F.round(F.avg("runtime_minutes"), 1).alias("avg_runtime_minutes"),
        )
        .orderBy(F.desc("episode_count"))
        .limit(10)
        .collect()
    )
    results.append((time.perf_counter() - start) * 1000)

    current_year = 2026
    start = time.perf_counter()
    (
        titles.filter(
            (F.col("start_year") >= (current_year - 20))
            & (F.col("start_year") > 0)
            & F.col("average_rating").isNotNull()
        )
        .groupBy("primary_genre")
        .agg(
            F.count("*").alias("title_count"),
            F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        )
        .orderBy(F.desc("title_count"))
        .limit(20)
        .collect()
    )
    results.append((time.perf_counter() - start) * 1000)

    start = time.perf_counter()
    (
        titles.filter(
            (F.col("title_type") == "movie")
            & F.col("runtime_minutes").isNotNull()
            & (F.col("runtime_minutes") > 0)
        )
        .groupBy("primary_genre")
        .agg(
            F.round(F.avg("runtime_minutes"), 1).alias("avg_runtime"),
            F.expr("percentile(runtime_minutes, 0.5)").alias("median_runtime"),
            F.count("*").alias("movie_count"),
        )
        .filter(F.col("movie_count") >= 100)
        .orderBy(F.desc("avg_runtime"))
        .limit(20)
        .collect()
    )
    results.append((time.perf_counter() - start) * 1000)

    return results


def run_spark_benchmarks_local():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("imdb-benchmark")
        .master(SPARK_MASTER)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        return run_spark_benchmarks(spark)
    finally:
        spark.stop()


def run_spark_benchmarks_docker():
    print("Running Spark benchmarks in Docker (Java not required on host)...")
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master",
        SPARK_MASTER,
        "/opt/spark/work-dir/benchmark.py",
        "--spark-only",
    ]
    result = subprocess.run(
        cmd,
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError("Spark benchmark failed inside Docker")

    if not SPARK_TIMES_FILE.exists():
        raise RuntimeError(f"Spark times file not found: {SPARK_TIMES_FILE}")

    times = [float(line.strip()) for line in SPARK_TIMES_FILE.read_text().splitlines() if line.strip()]
    if len(times) != len(QUERIES):
        raise RuntimeError(
            f"Expected {len(QUERIES)} Spark timings, got {len(times)}"
        )
    return times


def run_clickhouse_benchmarks(client):
    return [time_clickhouse(client, q["clickhouse"]) for q in QUERIES]


def print_results(spark_times, ch_times):
    print("\n" + "=" * 72)
    print(f"{'Query':<40} {'Spark (ms)':>12} {'ClickHouse (ms)':>16} {'Speedup':>8}")
    print("=" * 72)
    for q, st, ct in zip(QUERIES, spark_times, ch_times):
        speedup = f"{st / ct:.1f}x" if ct > 0 else "N/A"
        print(f"{q['name']:<40} {st:>12.1f} {ct:>16.1f} {speedup:>8}")
    print("=" * 72)

    avg_spark = sum(spark_times) / len(spark_times)
    avg_ch = sum(ch_times) / len(ch_times)
    print(f"\nAverage: Spark {avg_spark:.1f} ms | ClickHouse {avg_ch:.1f} ms")
    if avg_ch > 0:
        print(f"ClickHouse is {avg_spark / avg_ch:.1f}x faster on average")


def write_spark_times(times):
    SPARK_TIMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    SPARK_TIMES_FILE.write_text("\n".join(str(t) for t in times) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--spark-only", action="store_true", help="Run only Spark benchmarks"
    )
    parser.add_argument(
        "--clickhouse-only", action="store_true", help="Run only ClickHouse benchmarks"
    )
    args = parser.parse_args()

    spark_times = []
    ch_times = []

    if not args.clickhouse_only:
        print(f"Running Spark benchmarks (lake: {LAKE_DIR}) ...")
        if should_run_spark_locally():
            spark_times = run_spark_benchmarks_local()
        else:
            spark_times = run_spark_benchmarks_docker()

        if args.spark_only:
            write_spark_times(spark_times)
            for q, t in zip(QUERIES, spark_times):
                print(f"{q['name']}: {t:.1f} ms")
            print(f"Wrote timings to {SPARK_TIMES_FILE}")
            return

    if not args.spark_only:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT
        )
        print("Running ClickHouse benchmarks...")
        ch_times = run_clickhouse_benchmarks(client)

        if args.clickhouse_only:
            for q, t in zip(QUERIES, ch_times):
                print(f"{q['name']}: {t:.1f} ms")
            return

    if spark_times and ch_times:
        print_results(spark_times, ch_times)
    else:
        print("Run without --spark-only or --clickhouse-only for full comparison.")


if __name__ == "__main__":
    main()

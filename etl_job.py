"""
PySpark ETL: IMDb raw TSV -> cleaned, partitioned Snappy Parquet lake.

Reads whichever pipeline files exist in /data/raw/ (.tsv or .tsv.gz).

Lake outputs:
  - titles/      movie & TV titles with optional ratings
  - episodes/    TV episodes (from title.episode or tvEpisode rows in basics)
  - principals/  cast & crew joined to names and titles
  - akas/        alternate / localized titles
  - names/       people dimension (actors, directors, etc.)
"""

import os
import sys
from typing import List, Optional

from pyspark.sql import SparkSession, functions as F, types as T

RAW_DIR = os.environ.get("IMDB_RAW_DIR", "/data/raw")
LAKE_DIR = os.environ.get("IMDB_LAKE_DIR", "/data/lake")


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("imdb-lakehouse-etl")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def resolve_raw_path(base_name: str) -> Optional[str]:
    for suffix in (".tsv", ".tsv.gz"):
        path = os.path.join(RAW_DIR, f"{base_name}{suffix}")
        if os.path.exists(path):
            return path
    return None


def read_tsv(spark: SparkSession, base_name: str, required: bool = True):
    path = resolve_raw_path(base_name)
    if path is None:
        if required:
            raise FileNotFoundError(
                f"Missing required file: {base_name}.tsv or {base_name}.tsv.gz in {RAW_DIR}/"
            )
        return None
    print(f"  Reading {os.path.basename(path)} ...")
    return (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("nullValue", "\\N")
        .csv(path)
    )


def first_token(col):
    return F.when(col.isNull() | (col == ""), F.lit("Unknown")).otherwise(
        F.split(col, ",").getItem(0)
    )


def clean_basics(df):
    return (
        df.filter(F.col("isAdult").isin("0", 0))
        .withColumn("start_year", F.col("startYear").cast(T.IntegerType()))
        .withColumn("end_year", F.col("endYear").cast(T.IntegerType()))
        .withColumn("runtime_minutes", F.col("runtimeMinutes").cast(T.IntegerType()))
        .withColumn("primary_genre", first_token(F.col("genres")))
        .withColumn(
            "start_year",
            F.when(F.col("start_year").isNull(), F.lit(0)).otherwise(
                F.col("start_year")
            ),
        )
        .select(
            F.col("tconst"),
            F.col("titleType").alias("title_type"),
            F.col("primaryTitle").alias("primary_title"),
            F.col("originalTitle").alias("original_title"),
            F.col("start_year"),
            F.col("end_year"),
            F.col("runtime_minutes"),
            F.col("genres"),
            F.col("primary_genre"),
        )
    )


def build_titles(basics, ratings=None):
    if ratings is None:
        return basics.select(
            "tconst",
            "title_type",
            "primary_title",
            "original_title",
            "start_year",
            "end_year",
            "runtime_minutes",
            "genres",
            "primary_genre",
            F.lit(None).cast(T.FloatType()).alias("average_rating"),
            F.lit(None).cast(T.LongType()).alias("num_votes"),
        )
    return (
        basics.join(ratings, on="tconst", how="left")
        .select(
            "tconst",
            "title_type",
            "primary_title",
            "original_title",
            "start_year",
            "end_year",
            "runtime_minutes",
            "genres",
            "primary_genre",
            F.col("averageRating").cast(T.FloatType()).alias("average_rating"),
            F.col("numVotes").cast(T.LongType()).alias("num_votes"),
        )
    )


def build_episodes(episode_raw, titles):
    episodes = episode_raw.select(
        F.col("tconst").alias("episode_tconst"),
        F.col("parentTconst").alias("parent_tconst"),
        F.col("seasonNumber").cast(T.IntegerType()).alias("season_number"),
        F.col("episodeNumber").cast(T.IntegerType()).alias("episode_number"),
    )
    episode_titles = titles.select(
        F.col("tconst").alias("episode_tconst"),
        F.col("primary_title").alias("episode_title"),
        F.col("start_year"),
        F.col("runtime_minutes"),
        F.col("average_rating"),
        F.col("num_votes"),
    )
    series_info = titles.filter(F.col("title_type") == "tvSeries").select(
        F.col("tconst").alias("parent_tconst"),
        F.col("primary_title").alias("series_title"),
        F.col("primary_genre"),
    )
    return (
        episodes.join(episode_titles, on="episode_tconst", how="left")
        .join(series_info, on="parent_tconst", how="left")
        .withColumn(
            "start_year",
            F.when(F.col("start_year").isNull(), F.lit(0)).otherwise(
                F.col("start_year")
            ),
        )
        .withColumn(
            "primary_genre",
            F.when(
                F.col("primary_genre").isNull(), F.lit("Unknown")
            ).otherwise(F.col("primary_genre")),
        )
        .select(
            "episode_tconst",
            "parent_tconst",
            "season_number",
            "episode_number",
            "episode_title",
            "series_title",
            "start_year",
            "primary_genre",
            "average_rating",
            "num_votes",
            "runtime_minutes",
        )
    )


def build_episodes_from_basics(titles):
    return (
        titles.filter(F.col("title_type") == "tvEpisode")
        .withColumn("episode_tconst", F.col("tconst"))
        .withColumn("parent_tconst", F.lit(None).cast(T.StringType()))
        .withColumn("season_number", F.lit(None).cast(T.IntegerType()))
        .withColumn("episode_number", F.lit(None).cast(T.IntegerType()))
        .withColumn("episode_title", F.col("primary_title"))
        .withColumn("series_title", F.lit(None).cast(T.StringType()))
        .select(
            "episode_tconst",
            "parent_tconst",
            "season_number",
            "episode_number",
            "episode_title",
            "series_title",
            "start_year",
            "primary_genre",
            "average_rating",
            "num_votes",
            "runtime_minutes",
        )
    )


def build_names(name_raw):
    return (
        name_raw.withColumn("birth_year", F.col("birthYear").cast(T.IntegerType()))
        .withColumn("death_year", F.col("deathYear").cast(T.IntegerType()))
        .withColumn("primary_role", first_token(F.col("primaryProfession")))
        .select(
            F.col("nconst"),
            F.col("primaryName").alias("primary_name"),
            F.col("birth_year"),
            F.col("death_year"),
            F.col("primaryProfession").alias("primary_profession"),
            F.col("primary_role"),
            F.col("knownForTitles").alias("known_for_titles"),
        )
    )


def build_akas(akas_raw):
    return (
        akas_raw.withColumn(
            "region",
            F.when(
                F.col("region").isNull() | (F.col("region") == "\\N"),
                F.lit("Unknown"),
            ).otherwise(F.col("region")),
        )
        .withColumn(
            "language",
            F.when(
                F.col("language").isNull() | (F.col("language") == "\\N"),
                F.lit("Unknown"),
            ).otherwise(F.col("language")),
        )
        .withColumn("ordering", F.col("ordering").cast(T.IntegerType()))
        .withColumn(
            "is_original_title",
            F.col("isOriginalTitle").cast(T.IntegerType()),
        )
        .select(
            F.col("titleId").alias("title_id"),
            F.col("ordering"),
            F.col("title"),
            F.col("region"),
            F.col("language"),
            F.col("types"),
            F.col("is_original_title"),
        )
    )


def build_principals(principals_raw, names, titles):
    name_lookup = names.select(
        F.col("nconst"),
        F.col("primary_name"),
        F.col("birth_year"),
        F.col("primary_role"),
    )
    title_lookup = titles.select(
        F.col("tconst"),
        F.col("title_type"),
        F.col("primary_title"),
        F.col("primary_genre"),
    )
    return (
        principals_raw.withColumn("ordering", F.col("ordering").cast(T.IntegerType()))
        .withColumn(
            "category",
            F.when(F.col("category").isNull(), F.lit("Unknown")).otherwise(
                F.col("category")
            ),
        )
        .join(name_lookup, on="nconst", how="left")
        .join(title_lookup, on="tconst", how="left")
        .select(
            "tconst",
            "ordering",
            "nconst",
            "primary_name",
            "category",
            F.col("job"),
            F.col("characters"),
            F.col("title_type"),
            F.col("primary_title"),
            F.col("primary_genre"),
            F.col("birth_year"),
            F.col("primary_role"),
        )
    )


def write_partitioned(df, path: str, partition_cols: List[str]):
    (
        df.repartition(8, *partition_cols)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(*partition_cols)
        .parquet(path)
    )


def maybe_write(df, path: str, partition_cols: List[str], label: str) -> int:
    if df.head(1):
        print(f"Writing {label} lake to {path} ...")
        write_partitioned(df, path, partition_cols)
        return df.count()
    return 0


def main():
    if resolve_raw_path("title.basics") is None:
        print(
            f"ERROR: title.basics.tsv (or .tsv.gz) required in {RAW_DIR}/",
            file=sys.stderr,
        )
        sys.exit(1)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading raw IMDb files...")
    basics_raw = read_tsv(spark, "title.basics", required=True)
    ratings_raw = read_tsv(spark, "title.ratings", required=False)
    episode_raw = read_tsv(spark, "title.episode", required=False)
    principals_raw = read_tsv(spark, "title.principals", required=False)
    akas_raw = read_tsv(spark, "title.akas", required=False)
    names_raw = read_tsv(spark, "name.basics", required=False)

    print("Cleaning and transforming...")
    basics = clean_basics(basics_raw)
    ratings = (
        ratings_raw.select("tconst", "averageRating", "numVotes")
        if ratings_raw is not None
        else None
    )
    titles = build_titles(basics, ratings)
    names = build_names(names_raw) if names_raw is not None else None

    counts = {}

    counts["titles"] = maybe_write(
        titles,
        os.path.join(LAKE_DIR, "titles"),
        ["start_year", "title_type"],
        "titles",
    )

    if episode_raw is not None:
        episodes = build_episodes(episode_raw, titles)
    else:
        episodes = build_episodes_from_basics(titles)
    counts["episodes"] = maybe_write(
        episodes,
        os.path.join(LAKE_DIR, "episodes"),
        ["start_year", "primary_genre"],
        "episodes",
    )

    if names is not None:
        counts["names"] = maybe_write(
            names,
            os.path.join(LAKE_DIR, "names"),
            ["primary_role"],
            "names",
        )

    if akas_raw is not None:
        akas = build_akas(akas_raw)
        counts["akas"] = maybe_write(
            akas,
            os.path.join(LAKE_DIR, "akas"),
            ["region"],
            "akas",
        )

    if principals_raw is not None and names is not None:
        principals = build_principals(principals_raw, names, titles)
        counts["principals"] = maybe_write(
            principals,
            os.path.join(LAKE_DIR, "principals"),
            ["category"],
            "principals",
        )
    elif principals_raw is not None:
        print("  Skipping principals (name.basics not available for join)")

    print("\nETL complete:")
    for dataset, count in counts.items():
        print(f"  {dataset}: {count:,} rows")

    spark.stop()


if __name__ == "__main__":
    main()

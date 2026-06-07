"""
rawg_pipeline/spark/transform.py
----------------------------------
PySpark transformation layer.
Reads raw JSON from bronze DuckDB tables via pandas bridge,
applies cleaning and typing using PySpark DataFrames,
and writes Parquet output to the data/spark/ directory.

This layer demonstrates a scalable alternative to the SQLAlchemy silver
transform — same bronze source, same logic, distributed processing.
"""
import os

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Output directory for Parquet files
OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "spark")
)

# Path to DuckDB database
DB_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "rawg_data.duckdb")
)

# JSON schemas for PySpark to parse raw_json column
GAME_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("ratings_count", IntegerType(), True),
    StructField("released", StringType(), True),
])

GENRE_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
])

PLATFORM_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
])


def build_spark() -> SparkSession:
    """Create and return a local SparkSession."""
    return (
        SparkSession.builder
        .appName("rawg_pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, table: str):
    """
    Read a bronze DuckDB table into a Spark DataFrame via pandas bridge.
    Avoids JDBC/Java dependency by using the native duckdb Python client.
    """
    conn = duckdb.connect(DB_PATH, read_only=True)
    pandas_df = conn.execute(f"SELECT * FROM {table}").df()
    conn.close()
    return spark.createDataFrame(pandas_df)


def transform_games(spark: SparkSession) -> None:
    """Read bronze games, parse JSON, type fields, write Parquet."""
    df = read_bronze(spark, "bronze.bronze_games")

    parsed = df.select(
        F.from_json(F.col("raw_json"), GAME_SCHEMA).alias("data")
    ).select(
        F.col("data.id").alias("rawg_id"),
        F.col("data.name").alias("name"),
        F.col("data.rating").alias("rating"),
        F.col("data.ratings_count").alias("ratings_count"),
        F.to_date(F.col("data.released"), "yyyy-MM-dd").alias("released"),
    ).dropDuplicates(["rawg_id"])

    output_path = os.path.join(OUTPUT_DIR, "games")
    parsed.write.mode("overwrite").parquet(output_path)
    print(f"Written {parsed.count()} game records to {output_path}")


def transform_genres(spark: SparkSession) -> None:
    """Read bronze genres, parse JSON, write Parquet."""
    df = read_bronze(spark, "bronze.bronze_genres")

    parsed = df.select(
        F.from_json(F.col("raw_json"), GENRE_SCHEMA).alias("data")
    ).select(
        F.col("data.id").alias("rawg_id"),
        F.col("data.name").alias("name"),
        F.col("data.slug").alias("slug"),
    ).dropDuplicates(["rawg_id"])

    output_path = os.path.join(OUTPUT_DIR, "genres")
    parsed.write.mode("overwrite").parquet(output_path)
    print(f"Written {parsed.count()} genre records to {output_path}")


def transform_platforms(spark: SparkSession) -> None:
    """Read bronze platforms, parse JSON, write Parquet."""
    df = read_bronze(spark, "bronze.bronze_platforms")

    parsed = df.select(
        F.from_json(F.col("raw_json"), PLATFORM_SCHEMA).alias("data")
    ).select(
        F.col("data.id").alias("rawg_id"),
        F.col("data.name").alias("name"),
        F.col("data.slug").alias("slug"),
    ).dropDuplicates(["rawg_id"])

    output_path = os.path.join(OUTPUT_DIR, "platforms")
    parsed.write.mode("overwrite").parquet(output_path)
    print(f"Written {parsed.count()} platform records to {output_path}")


if __name__ == "__main__":
    spark = build_spark()
    print("Running PySpark bronze -> Parquet transforms...")
    transform_games(spark)
    transform_genres(spark)
    transform_platforms(spark)
    spark.stop()
    print("Done.")
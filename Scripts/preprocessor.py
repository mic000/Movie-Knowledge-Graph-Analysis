"""
preprocessor.py
Basic data loading and cleaning functions for MovieLens dataset.
Handles: schema normalization, deduplication, null filtering, genre explosion, release year extraction.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, trim, lower, to_timestamp, from_unixtime,
    to_date, regexp_extract, explode, split, length
)
from pyspark.sql.types import IntegerType, DoubleType

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
spark = SparkSession.builder.appName("MovieLens_Cleaning").getOrCreate()


# 1. Data Loading
def load_csv(relative_path):
    """Load a CSV using a path relative to the project root."""
    abs_path = os.path.join(PROJECT_ROOT, relative_path)
    return spark.read.csv(abs_path, header=True, inferSchema=True)

def load_json(relative_path):
    """Load a JSON using a path relative to the project root."""
    abs_path = os.path.join(PROJECT_ROOT, relative_path)
    return spark.read.json(abs_path)

# 2. Null Summary
def null_summary(df, name):
    """Print null counts per column for the given DataFrame."""
    print(f"\n=== Null Summary: {name} ===")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show(truncate=False)

# 3. file to cleaning: Datatype Standard Normalization and Deduplicate
def clean_movies(df):
    """
    Normalize types, deduplicate, drop null PKs, extract release_year from title.
    """
    df = (df
          .withColumn("movieId", col("movieId").cast(IntegerType()))
          .withColumn("title", trim(col("title")))
          .withColumn("genres", trim(col("genres"))))

    df = df.dropDuplicates(["movieId"])
    df = df.filter(col("movieId").isNotNull())

    df = df.withColumn(
        "release_year",
        when(
            regexp_extract(col("title"), r"\((\d{4})\)$", 1) != "",
            regexp_extract(col("title"), r"\((\d{4})\)$", 1).cast(IntegerType())
        ).otherwise(None)
    )
    return df

def clean_ratings(df):
    """
    Normalize types, add datetime columns, deduplicate, enforce 0.5-5.0 range.
    """
    df = (df
          .withColumn("userId", col("userId").cast(IntegerType()))
          .withColumn("movieId", col("movieId").cast(IntegerType()))
          .withColumn("rating", col("rating").cast(DoubleType()))
          .withColumn("rating_datetime", to_timestamp(from_unixtime(col("timestamp"))))
          .withColumn("rating_date", to_date(col("rating_datetime"))))

    df = df.dropDuplicates(["userId", "movieId", "timestamp", "rating"])
    df = df.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
    return df.select("userId", "movieId", "rating", "rating_datetime", "rating_date")

def clean_tags(df):
    """
    Normalize types/case, add datetime columns, deduplicate, drop empty tags.
    """
    df = (df
          .withColumn("userId", col("userId").cast(IntegerType()))
          .withColumn("movieId", col("movieId").cast(IntegerType()))
          .withColumn("tag", trim(lower(col("tag"))))
          .withColumn("tag_datetime", to_timestamp(from_unixtime(col("timestamp"))))
          .withColumn("tag_date", to_date(col("tag_datetime"))))

    df = df.dropDuplicates(["userId", "movieId", "timestamp", "tag"])
    df = df.filter((col("tag").isNotNull()) & (length(col("tag")) > 0))
    return df.select("userId", "movieId", "tag", "tag_datetime", "tag_date")

def explode_genres(df):
    """
    Split the pipe-delimited genres column into individual rows; remove IMAX.
    """
    df = (df
          .withColumn("genre", explode(split(col("genres"), r"\|")))
          .withColumn("genre", trim(col("genre")))
          .filter(col("genre") != "IMAX"))
    return df.select("movieId", "genre")
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, trim, lower, to_timestamp, from_unixtime,
    to_date, regexp_extract, explode, split, length
)
from pyspark.sql.types import IntegerType, DoubleType, LongType

spark = SparkSession.builder.appName("MovieLens_Cleaning_Intelligence").getOrCreate()

# 1. Data Loading
def load_data(file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

# 2. Null Summary
def null_summary(df):
    print(f"\n=== Null Summary: {df.columns} ===")
    df.printSchema()

# 3. file to cleaning: Datatype Standard Normalization and Deduplicate
def clean_movies(df):
    df = (df
        .withColumn("movieId", col("movieId").cast(IntegerType()))
        .withColumn("title", trim(col("title")))
        .withColumn("genres", trim(col("genres")))
    )
    df = df.dropDuplicates(["movieId"])
    df = df.filter(col("movieId").isNotNull())
    # filter out title contained year
    df = df.withColumn(
        "release_year",
        when(regexp_extract(col("title"), r"\((\d{4})\)$", 1) != '',
             regexp_extract(col("title"), r"\((\d{4})\)$", 1).cast(IntegerType())
        ).otherwise(None)
    )
    return df

def clean_ratings(df):
    df = (df
        .withColumn("userId", col("userId").cast(IntegerType()))
        .withColumn("movieId", col("movieId").cast(IntegerType()))
        .withColumn("rating", col("rating").cast(DoubleType()))
        .withColumn("rating_datetime", to_timestamp(from_unixtime(col("timestamp"))))
        .withColumn("rating_date", to_date(col("rating_datetime")))
    )
    df = df.dropDuplicates(["userId", "movieId", "timestamp", "rating"])
    df = df.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
    return df.select("userId", "movieId", "rating", "rating_datetime", "rating_date")

def clean_tags(df):
    df = (df
        .withColumn("userId", col("userId").cast(IntegerType()))
        .withColumn("movieId", col("movieId").cast(IntegerType()))
        .withColumn("tag", trim(lower(col("tag"))))
        .withColumn("tag_datetime", to_timestamp(from_unixtime(col("timestamp"))))
        .withColumn("tag_date", to_date(col("tag_datetime")))
    )
    df = df.dropDuplicates(["userId", "movieId", "timestamp", "tag"])
    df = df.filter((col("tag").isNotNull()) & (length(col("tag")) > 0))
    return df.select("userId", "movieId", "tag", "tag_datetime", "tag_date")

def explode_genres(df):
    df = (df
        .withColumn("genre", explode(split(col("genres"), r"\|")))
        .withColumn("genre", trim(col("genre")))
        .filter(col("genre") != "IMAX")
    )
    return df.select("movieId", "genre")



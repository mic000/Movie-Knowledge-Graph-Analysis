# from encodings.idna import ace_prefix
# from select import select
#
# from pyspark.sql import SparkSession, Row
# from pyspark.sql.functions import (
#     col, trim, lower, split, explode, regexp_extract,
#     from_unixtime, to_timestamp, to_date, count, countDistinct, round,asc,
#     avg, desc, when, length, sum
#
# )
from pyspark.sql.types import IntegerType, DoubleType, LongType
import os
import glob
import shutil

# spark = SparkSession.builder.appName("MovieLens_Cleaning_Intelligence").getOrCreate()

# -----------------------------
# 1. data loading
# # -----------------------------
# movies = spark.read.csv("movies.csv", header=True, inferSchema=True)
# ratings = spark.read.csv("ratings.csv", header=True, inferSchema=True)
# tags = spark.read.csv("tags.csv", header=True, inferSchema=True)
#
# # print("=== Original Schema ===")
# # movies.printSchema()
# # ratings.printSchema()
# # tags.printSchema()
#
# # -----------------------------
# # 2. check up null or missed value
# # -----------------------------
# # def null_summary(df, name):
# #     print(f"\n=== Null Summary: {name} ===")
# #     df.select([
# #         count(when(col(c).isNull(), c)).alias(c) for c in df.columns
# #     ]).show(truncate=False)
# #
# # null_summary(movies, "movies")
# # null_summary(ratings, "ratings")
# # null_summary(tags, "tags")
#
# # -----------------------------
# # 3. datatype standard normalization
# # -----------------------------
# movies_clean = (
#     movies
#     .withColumn("movieId", col("movieId").cast(IntegerType()))
#     .withColumn("title", trim(col("title")))
#     .withColumn("genres", trim(col("genres")))
# )
#
# ratings_clean = (
#     ratings
#     .withColumn("userId", col("userId").cast(IntegerType()))
#     .withColumn("movieId", col("movieId").cast(IntegerType()))
#     .withColumn("rating", col("rating").cast(DoubleType()))
#     .withColumn("timestamp", col("timestamp").cast(LongType()))
#     .withColumn("rating_datetime", to_timestamp(from_unixtime(col("timestamp"))))
#     .withColumn("rating_date", to_date(col("rating_datetime")))
# )
#
# tags_clean = (
#     tags
#     .withColumn("userId", col("userId").cast(IntegerType()))
#     .withColumn("movieId", col("movieId").cast(IntegerType()))
#     .withColumn("tag", trim(lower(col("tag"))))
#     .withColumn("timestamp", col("timestamp").cast(LongType()))
#     .withColumn("tag_datetime", to_timestamp(from_unixtime(col("timestamp"))))
#     .withColumn("tag_date", to_date(col("tag_datetime")))
# )
#
# # -----------------------------
# # 4. duplicate
# # -----------------------------
# movies_clean = movies_clean.dropDuplicates(["movieId"])
# ratings_clean = ratings_clean.dropDuplicates(["userId", "movieId", "timestamp", "rating"])
# tags_clean = tags_clean.dropDuplicates(["userId", "movieId", "timestamp", "tag"])
#
# # -----------------------------
# # 5. data quality cleaning/filtering
# # -----------------------------
# # filter out NULL primary keys
# movies_clean = movies_clean.filter(col("movieId").isNotNull())
#
# # 5-star scale with half-star increments
# ratings_clean = ratings_clean.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
#
# # filter out NULL message on tag
# tags_clean = tags_clean.filter((col("tag").isNotNull()) & (length(col("tag")) > 0))
#
# # -----------------------------
# # 6. filter out title contained year
# # -----------------------------
# movies_clean = movies_clean.withColumn(
#     "release_year",
#     when(
#         regexp_extract(col("title"), r"\((\d{4})\)$", 1) != '',
#         regexp_extract(col("title"), r"\((\d{4})\)$", 1).cast(IntegerType())
#     ).otherwise(None)
# )
#
# # -----------------------------
# # 7. break down genres col.
# # -----------------------------
# movies_genres = (
#     movies_clean
#     .withColumn("genre", explode(split(col("genres"), r"\|")))
#     .withColumn("genre", trim(col("genre")))
# )
#
# # remove IMAX genres from dataset
# movies_genres = movies_genres.filter(col("genre") != "IMAX")


# # -----------------------------
# # 8. data intelligence
# # -----------------------------
# print("\n\n\n=== Data Intelligence/Summary ===")
# print("\n=== Row Counts ===")
# print("movies:", movies_clean.count())
# print("ratings:", ratings_clean.count())
# print("tags:", tags_clean.count())
# print("movie-genre rows:", movies_genres.count())
#
# print("\n=== Distinct Counts ===")
# print("distinct users in ratings:", ratings_clean.select("userId").distinct().count())
# print("distinct movies in ratings:", ratings_clean.select("movieId").distinct().count())
# print("distinct movies in movies:", movies_clean.select("movieId").distinct().count())
# print("distinct genres:", movies_genres.select("genre").distinct().count())
# print("distinct tags:", tags_clean.select("tag").distinct().count())
#
# print("\n=== Movies per Genre ===")
# movies_genres.groupBy("genre").count().orderBy(desc("count")).show(30, truncate=False)
#
# # list top 20 highest rated popular movies
# print("\n=== Most Rated Movies ===")
# ratings_clean.groupBy("movieId") \
#     .agg(count("*").alias("num_ratings"), avg("rating").alias("avg_rating")) \
#     .orderBy(desc("num_ratings")) \
#     .show(20, truncate=False)
#
# # show top 20 users who are most active raters
# print("\n=== Most Active Users ===")
# ratings_clean.groupBy("userId") \
#     .agg(count("*").alias("num_ratings")) \
#     .orderBy(desc("num_ratings")) \
#     .show(20, truncate=False)
#
# print("\n=== Least Active Users ===")
# ratings_clean.groupBy("userId") \
#     .agg(count("*").alias("num_ratings")) \
#     .orderBy(asc("num_ratings")) \
#     .show(20, truncate=False)
#
# # check and show top 20 movies and their avg. rating which at least 10 users have rated
# print("\n=== Average Rating by Movie ===")
# ratings_clean.groupBy("movieId") \
#     .agg(avg("rating").alias("avg_rating"), count("*").alias("num_ratings")) \
#     .filter(col("num_ratings") >= 10) \
#     .orderBy(desc("avg_rating")) \
#     .show(20, truncate=False)
#
# # top 20 tags commented by user
# print("\n=== Tag Usage Top 20 ===")
# tags_clean.groupBy("tag").count().orderBy(desc("count")).show(20, truncate=False)
#
# users_node = ratings_clean.select("userId").distinct()
# movies_node = movies_clean.select("movieId", "title", "release_year")
# genres_node = movies_genres.select("genre").distinct()
# ratings_rel = ratings_clean.select("userId", "movieId", "rating", "timestamp", "rating_datetime")
# movie_genre_rel = movies_genres.select("movieId", "genre").distinct()
# tags_rel = tags_clean.select("userId", "movieId", "tag", "timestamp", "tag_datetime")
#

# -----------------------------
# additional(special case happened)
# -----------------------------
# check over null contained in each row of each dataframe
# for df_name, df in dfs.items():
#     print(f"\n==============================")
#     print(f"Checking NULL counts in: {df_name}")
#     print(f"==============================")
#     print("\nNull counts:")
#     null_counts = df.select([
#         count(when(col(c).isNull(), c)).alias(c) for c in df.columns
#     ])
#     null_counts.show(truncate=False)

# known only movies_node contained null in release_year col.
# Since README: "Errors and inconsistencies may exist in these titles."
# find movidID as release_year is null
# mID_Null_df = movies_node.filter(col("release_year").isNull()).select("movieId")
#
# target_deleted_Mid = (   # deleted movies with null in release year and which only has one rated
#     ratings_rel
#     .join(mID_Null_df, on="movieId", how="inner")
#     .groupBy(col("movieId")).agg(count("*").alias("num_ratings"))
#     .filter(col("num_ratings") == 1)
#     .join(tags_rel.select("movieId").distinct(), on="movieId", how="left_anti")
#     .select("movieId"))
# print("=== Null release_year movieIds to delete from movies_node ===")
# target_deleted_Mid.show(truncate=False)

# keep_movie_ids = mID_Null_df.join(
#     target_deleted_Mid,
#     on="movieId",
#     how="left_anti"
# )
# kept_movies = movies_node.join(
#     keep_movie_ids,
#     on="movieId",
#     how="inner"
# ).select("movieId", "title")
# print("=== Null release_year movieIds to keep from movies_node ===")
# kept_movies.show(truncate=False)

# from kept_movies ID correctly input release_year number
# movies_node = (
#     movies_node
#     .withColumn(
#         "title",
#         when(col("movieId") == 7789, "September 11 (2002)")
#         .otherwise(col("title"))
#     )
#     .withColumn(
#         "release_year",
#         when(col("movieId") == 7789, 2002)
#         .when(col("movieId") == 40697, 1994)
#         .when(col("movieId") == 140956, 2018)
#         # .when(col("movieId") == 149334, 2016)
#         .when(col("movieId") == 156605, 2016)
#         # .when(col("movieId") == 162414, 2016)
#         .when(col("movieId") == 171495, 2018) # movie actually release at 2019
#         .otherwise(col("release_year"))
#     )
# )
#
# movies_node = movies_node.join(
#     target_deleted_Mid,
#     on="movieId",
#     how="left_anti"
# )
#
# # sanity check!!!!
# # movies_node.join(
# #     target_deleted_Mid,
# #     on="movieId",
# #     how="inner"
# # ).show()
#
# ratings_rel = ratings_rel.join(
#     target_deleted_Mid,
#     on="movieId",
#     how="left_anti"
# )
#
# movie_genre_rel = movie_genre_rel.join(
#     target_deleted_Mid,
#     on="movieId",
#     how="left_anti"
# )
#
# tags_rel = tags_rel.join(
#     target_deleted_Mid,
#     on="movieId",
#     how="left_anti"
# )


# -----------------------------
# 11. relabel genre for movies which  is "no genres listed"
# -----------------------------
# re_genre_mID = movies_genres.filter(col("genre") == "(no genres listed)").select("movieId", "title")
# # print(re_genre_mID.join(target_deleted_Mid, on="movieId", how="left_anti").show(n=50, truncate=False))
#
# manual_fixes = [
#     Row(movieId=114335, genre="Comedy"),Row(movieId=114335, genre="Fantasy"),
#     Row(movieId=122888, genre="Drama"),Row(movieId=122888, genre="Action"),Row(movieId=122888, genre="Adventure"),
#     Row(movieId=122896, genre="Action"),Row(movieId=122896, genre="Adventure"),Row(movieId=122896, genre="Fantasy"),
#     Row(movieId=129250, genre="Action"),Row(movieId=129250, genre="Comedy"),
#     Row(movieId=132084, genre="Drama"),Row(movieId=132084, genre="Musical"),
#     Row(movieId=134861, genre="Comedy"),
#     Row(movieId=141131, genre="Action"),Row(movieId=141131, genre="Adventure"),Row(movieId=141131, genre="Sci-Fi"),
#     Row(movieId=141866, genre="Horror"),Row(movieId=141866, genre="Crime"),Row(movieId=141866, genre="Thriller"),
#     Row(movieId=142456, genre="Comedy"),Row(movieId=142456, genre="Fantasy"),
#     Row(movieId=149330, genre="Sci-Fi"),Row(movieId=149330, genre="Adventure"),Row(movieId=149330, genre="Animation"),
#     Row(movieId=152037, genre="Comedy"),Row(movieId=152037, genre="Musical"),Row(movieId=152037, genre="Romance"),
#     Row(movieId=155589, genre="Adventure"),Row(movieId=155589, genre="Comedy"),
#     Row(movieId=156605, genre="Drama"),
#     Row(movieId=159161, genre="Comedy"),Row(movieId=159161, genre="Documentary"),
#     Row(movieId=159779, genre="Comedy"),Row(movieId=159779, genre="Fantasy"),Row(movieId=159779, genre="Romance"),Row(movieId=159779, genre="Drama"),
#     Row(movieId=161008, genre="Musical"),Row(movieId=161008, genre="Romance"),Row(movieId=161008, genre="Drama"),
#     Row(movieId=165489, genre="Drama"),Row(movieId=165489, genre="Animation"),
#     Row(movieId=166024, genre="Drama"),Row(movieId=166024, genre="Musical"),
#     Row(movieId=169034, genre="Musical"),
#     Row(movieId=171495, genre="Sci-Fi"),Row(movieId=171495, genre="Mystery"),Row(movieId=171495, genre="Adventure"),
#     Row(movieId=172497, genre="Action"),Row(movieId=172497, genre="Sci-Fi"),
#     Row(movieId=172591, genre="Crime"),Row(movieId=172591, genre="Drama"),
#     Row(movieId=173535, genre="Crime"),Row(movieId=173535, genre="Drama"),Row(movieId=173535, genre="Mystery"),Row(movieId=173535, genre="Adventure"),
#     Row(movieId=174403, genre="Documentary"),
#     Row(movieId=181413, genre="Documentary"),
#     Row(movieId=181719, genre="Drama"),
#     Row(movieId=182727, genre="Musical"),Row(movieId=182727, genre="Comedy")
# ]
# manual_fixes_df = spark.createDataFrame(manual_fixes)
#
# target_ids = [row.movieId for row in re_genre_mID.collect()]
# movie_genre_rel_cleaned = movie_genre_rel.filter(~col("movieId").isin(target_ids))
# movie_genre_rel = movie_genre_rel_cleaned.union(manual_fixes_df.select("movieId", "genre")).distinct()
# print(movies_node.select("movieId").subtract(movie_genre_rel.select("movieId")).isEmpty())
# movie_genre_rel = movie_genre_rel.join(movies_node.select("movieId"), on="movieId", how="inner")

# genres_node = movie_genre_rel.select("genre").distinct()

# -----------------------------
# 12a. finally save and export csv file for Neo4j
# -----------------------------
# dfs = {
#     "users_node": users_node,
#     "movies_node": movies_node,
#     "genres_node": genres_node,
#     "ratings_rel": ratings_rel,
#     # "movie_genre_rel": movie_genre_rel,
#     "tags_rel": tags_rel
# }

# def export_single_csv(df, folder_name, final_name, base_dir="neo4j_csv"):
#     """
#     Write Spark DataFrame to a temp folder, then extract the single part file
#     and save it as one flat CSV file for Neo4j upload.
#     """
#     temp_dir = os.path.join(base_dir, folder_name)
#
#     # write Spark output (this creates a folder)
#     df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_dir)
#
#     # find the actual csv file inside the folder
#     part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
#     if not part_files:
#         raise FileNotFoundError(f"No part csv found in {temp_dir}")
#
#     # create final flat csv path
#     final_path = os.path.join(base_dir, final_name)
#
#     # copy the part file as a normal csv
#     shutil.copyfile(part_files[0], final_path)
#
#     print(f"Saved: {final_path}")


# make output directory
# os.makedirs("neo4j_csv", exist_ok=True)
#
# export_single_csv(users_node, "users_node_tmp", "users_node.csv")
# export_single_csv(movies_node, "movies_node_tmp", "movies_node.csv")
# export_single_csv(genres_node, "genres_node_tmp", "genres_node.csv")
# export_single_csv(ratings_rel, "ratings_rel_tmp", "ratings_rel.csv")
# export_single_csv(movie_genre_rel, "movie_genre_rel_tmp", "movie_genre_rel.csv")
# export_single_csv(tags_rel, "tags_rel_tmp", "tags_rel.csv")
#
# print("\n=== Cleaned Schema ===")
# movies_clean.printSchema()
# ratings_clean.printSchema()
# tags_clean.printSchema()
#
# # -----------------------------
# # 12b. finally save and Power BI export tables
# # -----------------------------
# movie_ratings_flat = (
#     ratings_rel
#     .join(movies_node, on="movieId", how="left")
#     .join(movie_genre_rel, on="movieId", how="left")
#     .select(
#         "userId",
#         "movieId",
#         "title",
#         "release_year",
#         "genre",
#         "rating",
#         "timestamp",
#         "rating_datetime"
#     )
# )
#
# movie_genre_stats = (
#     movie_ratings_flat
#     .groupBy("movieId", "title", "release_year", "genre")
#     .agg(
#         count("*").alias("rating_count"),
#         avg("rating").alias("avg_rating")
#     )
# )
#
# user_stats = (
#     ratings_rel
#     .groupBy("userId")
#     .agg(
#         count("*").alias("num_ratings"),
#         avg("rating").alias("avg_user_rating")
#     )
# )
#
# export_single_csv(movie_ratings_flat, "movie_ratings_flat_tmp", "movie_ratings_flat.csv")
# export_single_csv(movie_genre_stats, "movie_genre_stats_tmp", "movie_genre_stats.csv")
# export_single_csv(user_stats, "user_stats_tmp", "user_stats.csv")






# spark.stop()
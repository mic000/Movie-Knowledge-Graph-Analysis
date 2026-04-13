from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from preprocessor import *
from io_utils import *
from data_analysis import *


spark = SparkSession.builder.appName("MovieLens_Final_Clean").getOrCreate()


def main():
    movies_raw = load_data("Data/Raw/movies.csv")
    ratings_raw = load_data("Data/Raw/ratings.csv")
    tags_raw = load_data("Data/Raw/tags.csv")

    movies_clean = clean_movies(movies_raw)
    ratings_clean = clean_ratings(ratings_raw)
    tags_clean = clean_tags(tags_raw)
    movie_genre_rel = explode_genres(movies_clean)

    mID_Null_df = movies_clean.filter(col("release_year").isNull()).select("movieId")
    target_deleted_mID = (  # deleted movies with null in release year and which only has one rated
        ratings_clean
        .join(mID_Null_df, on="movieId", how="inner")
        .groupBy(col("movieId")).agg(count("*").alias("num_ratings"))
        .filter(col("num_ratings") == 1)
        .join(tags_clean.select("movieId").distinct(), on="movieId", how="left_anti")
        .select("movieId"))

    movies_node = movies_clean.join(target_deleted_mID, "movieId", "left_anti")
    ratings_rel = ratings_clean.join(target_deleted_mID, "movieId", "left_anti")
    movie_genre_rel = movie_genre_rel.join(target_deleted_mID, "movieId", "left_anti")
    tags_rel = tags_clean.join(target_deleted_mID, "movieId", "left_anti")

    movies_node = movies_node.withColumn("title", when(col("movieId") == 7789, "September 11 (2002)") \
                                         .otherwise(col("title"))).withColumn("release_year",
                                          when(col("movieId") == 7789, 2002)
                                         .when(col("movieId") == 40697, 1994)
                                         .when(col("movieId") == 140956, 2018)
                                         .when(col("movieId") == 156605, 2016)
                                         .when(col("movieId") == 171495, 2018)
                                         .otherwise(col("release_year")))

    no_genre_mID = movie_genre_rel.filter(col("genre") == "(no genres listed)").select("movieId")
    manual_fixes_df = spark.read.json("Data/Manual Fixes/movieId_genre.json")

    target_ids = [row.movieId for row in no_genre_mID.collect()]
    movie_genre_rel = movie_genre_rel.filter(~col("movieId").isin(target_ids)) \
        .union(manual_fixes_df.select("movieId", "genre")) \
        .distinct()

    movie_ratings_flat = ratings_rel.join(movies_node, "movieId", "left") \
        .join(movie_genre_rel, "movieId", "left")
    movie_genre_stats = movie_ratings_flat.groupBy("movieId", "title", "release_year", "genre").agg(
            count("*").alias("rating_count"), avg("rating").alias("avg_rating"))
    user_stats = ratings_rel.groupBy("userId").agg(
        count("*").alias("num_ratings"), avg("rating").alias("avg_user_rating"))

    users_node = ratings_clean.select("userId").distinct()
    movies_node = movies_clean.select("movieId", "title", "release_year")
    genres_node = movie_genre_rel.select("genre").distinct()

    print("cleaning completed, ready for export...")

    # ---  Neo4j  ---
    neo4j_tables = {
        "users_node.csv": users_node,
        "movies_node.csv": movies_node,
        "genres_node.csv": genres_node,
        "ratings_rel.csv": ratings_rel,
        "movie_genre_rel.csv": movie_genre_rel,
        "tags_rel.csv": tags_rel
    }
    for name, df in neo4j_tables.items():
        save_csv(df, name, "output/Neo4j")

    # ---  Power BI  ---
    pbi_tables = {
        "movie_ratings_flat.csv": movie_ratings_flat,
        "movie_genre_stats.csv": movie_genre_stats,
        "user_stats.csv": user_stats
    }
    for name, df in pbi_tables.items():
        save_csv(df, name, "output/Power BI")

if __name__ == "__main__":
    main()

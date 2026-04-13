"""
main.py – MovieLens cleaning pipeline
  1. Load & clean
  2. Fix bad data (null years, missing genres)
  3. Basic analysis
  4. Export for Neo4j & Power BI
"""
from pyspark.sql.functions import col, count, avg, when, desc

from preprocessor import (
    spark, load_csv, load_json,
    clean_movies, clean_ratings, clean_tags, explode_genres
)
from io_utils import export_neo4j, export_powerbi


def main():
    # ── 1. Load & Clean ──────────────────────────────────────
    movies_clean    = clean_movies(load_csv("data/raw/movies.csv"))
    ratings_clean   = clean_ratings(load_csv("data/raw/ratings.csv"))
    tags_clean      = clean_tags(load_csv("data/raw/tags.csv"))
    movie_genre_rel = explode_genres(movies_clean)

    # ── 2. Remove junk movies ────────────────────────────────
    #   null year + "(no genres listed)" + no tags → delete
    null_year_ids = movies_clean.filter(col("release_year").isNull()).select("movieId")

    delete_ids = (
        movie_genre_rel
        .join(null_year_ids, "movieId", "inner")
        .filter(col("genre") == "(no genres listed)")
        .join(tags_clean.select("movieId").distinct(), "movieId", "left_anti")
        .select("movieId").distinct()
    )

    movies_clean    = movies_clean.join(delete_ids, "movieId", "left_anti")
    ratings_clean   = ratings_clean.join(delete_ids, "movieId", "left_anti")
    movie_genre_rel = movie_genre_rel.join(delete_ids, "movieId", "left_anti")
    tags_clean      = tags_clean.join(delete_ids, "movieId", "left_anti")

    # ── 3. Manual fixes ──────────────────────────────────────
    #  3a. title / release_year
    movies_clean = (
        movies_clean
        .withColumn("title",
                     when(col("movieId") == 7789, "September 11")
                     .otherwise(col("title")))
        .withColumn("release_year",
                     when(col("movieId") == 7789,   2002)
                     .when(col("movieId") == 40697,  1994)
                     .when(col("movieId") == 140956, 2018)
                     .when(col("movieId") == 149334, 2016)
                     .when(col("movieId") == 156605, 2016)
                     .when(col("movieId") == 162414, 2016)
                     .otherwise(col("release_year")))
    )

    #  3b. genre fixes from JSON patch
    manual_genres = load_json("data/manual fixes/movieId_genre.json")
    no_genre_ids = [r.movieId for r in
        movie_genre_rel.filter(col("genre") == "(no genres listed)").select("movieId").collect()]
    movie_genre_rel = (
        movie_genre_rel.filter(~col("movieId").isin(no_genre_ids))
        .union(manual_genres.select("movieId", "genre")).distinct())

    # ── 4. Basic analysis ────────────────────────────────────
    print("\n=== Row Counts ===")
    print(f"  movies:  {movies_clean.count()}")
    print(f"  ratings: {ratings_clean.count()}")
    print(f"  tags:    {tags_clean.count()}")
    print(f"  genres:  {movie_genre_rel.count()}")

    print("\n=== Movies per Genre ===")
    movie_genre_rel.groupBy("genre").count().orderBy(desc("count")).show(30, truncate=False)

    print("\n=== Most Rated Movies (top 20) ===")
    ratings_clean.groupBy("movieId").agg(count("*").alias("num_ratings"), avg("rating").alias("avg_rating")) \
        .orderBy(desc("num_ratings")).show(20, truncate=False)

    # ── 5. Build & Export ────────────────────────────────────
    users_node  = ratings_clean.select("userId").distinct()
    movies_node = movies_clean.select("movieId", "title", "release_year")
    genres_node = movie_genre_rel.select("genre").distinct()
    ratings_rel = ratings_clean.select("userId", "movieId", "rating", "rating_datetime", "rating_date")
    tags_rel    = tags_clean.select("userId", "movieId", "tag", "tag_datetime", "tag_date")

    movie_ratings_flat = (ratings_rel.join(movies_node, "movieId", "left")
                   .join(movie_genre_rel, "movieId", "left"))
    movie_genre_stats = (movie_ratings_flat.groupBy("movieId", "title", "release_year", "genre")
        .agg(count("*").alias("rating_count"), avg("rating").alias("avg_rating")))
    user_stats = (ratings_rel.groupBy("userId")
        .agg(count("*").alias("num_ratings"), avg("rating").alias("avg_user_rating")))

    export_neo4j(users_node, movies_node, genres_node, ratings_rel, movie_genre_rel, tags_rel)
    export_powerbi(movie_ratings_flat, movie_genre_stats, user_stats)

    print("\n✓ Done.")
    spark.stop()


if __name__ == "__main__":
    main()
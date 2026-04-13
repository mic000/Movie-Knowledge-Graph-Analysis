from pyspark.sql.functions import col, count, avg, desc, asc

def run_report(movies_node, ratings_rel, movie_genre_rel, tags_rel):
    print("\n" + "="*40)
    print("      DATA INTELLIGENCE REPORT")
    print("="*40)

    print("\n[1. Row Counts]")
    print(f"Movies (Node):  {movies_node.count()}")
    print(f"Ratings (Rel): {ratings_rel.count()}")
    print(f"Tags (Rel):    {tags_rel.count()}")
    print(f"Movie-Genre:   {movie_genre_rel.count()}")

    print("\n[2. Distinct Counts]")
    print(f"Distinct Users:   {ratings_rel.select('userId').distinct().count()}")
    print(f"Distinct Genres:  {movie_genre_rel.select('genre').distinct().count()}")
    print(f"Distinct Tags:    {tags_rel.select('tag').distinct().count()}")

    print("\n[3. Movies per Genre - Top 10]")
    movie_genre_rel.groupBy("genre").count().orderBy(desc("count")).show(10, truncate=False)

    print("\n[4. Most Rated Movies - Top 10]")
    ratings_rel.groupBy("movieId") \
        .agg(count("*").alias("num_ratings"), avg("rating").alias("avg_rating")) \
        .orderBy(desc("num_ratings")) \
        .show(10, truncate=False)

    print("\n[5. Most Active Users - Top 10]")
    ratings_rel.groupBy("userId") \
        .agg(count("*").alias("num_ratings")) \
        .orderBy(desc("num_ratings")) \
        .show(10, truncate=False)

    print("\n[6. Top Rated Movies (min 10 ratings)]")
    ratings_rel.groupBy("movieId") \
        .agg(avg("rating").alias("avg_rating"), count("*").alias("num_ratings")) \
        .filter(col("num_ratings") >= 10) \
        .orderBy(desc("avg_rating")) \
        .show(10, truncate=False)

    print("\n" + "="*40)
    print("      REPORT GENERATED SUCCESSFULLY")
    print("="*40)
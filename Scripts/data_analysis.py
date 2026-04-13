from pyspark.sql.functions import col, count, avg, desc

def print_row_counts(movies, ratings, tags, movie_genre_rel):
    """Print row counts for all core DataFrames."""
    print("\n=== Row Counts ===")
    print(f"  movies:           {movies.count()}")
    print(f"  ratings:          {ratings.count()}")
    print(f"  tags:             {tags.count()}")
    print(f"  movie-genre rows: {movie_genre_rel.count()}")


def print_distinct_counts(movies, ratings, tags, movie_genre_rel):
    """Print distinct-value counts for key columns."""
    print("\n=== Distinct Counts ===")
    print(f"  distinct users in ratings:  {ratings.select('userId').distinct().count()}")
    print(f"  distinct movies in ratings: {ratings.select('movieId').distinct().count()}")
    print(f"  distinct movies in movies:  {movies.select('movieId').distinct().count()}")
    print(f"  distinct genres:            {movie_genre_rel.select('genre').distinct().count()}")
    print(f"  distinct tags:              {tags.select('tag').distinct().count()}")


def show_movies_per_genre(movie_genre_rel, n=30):
    """Show the number of movies in each genre, descending."""
    print(f"\n=== Movies per Genre (top {n}) ===")
    movie_genre_rel.groupBy("genre") \
        .count() \
        .orderBy(desc("count")) \
        .show(n, truncate=False)


def show_most_rated_movies(ratings, n=20):
    """Show the most-rated movies by number of ratings."""
    print(f"\n=== Most Rated Movies (top {n}) ===")
    ratings.groupBy("movieId") \
        .agg(count("*").alias("num_ratings"), avg("rating").alias("avg_rating")) \
        .orderBy(desc("num_ratings")) \
        .show(n, truncate=False)


def show_most_active_users(ratings, n=20):
    """Show the users who have submitted the most ratings."""
    print(f"\n=== Most Active Users (top {n}) ===")
    ratings.groupBy("userId") \
        .agg(count("*").alias("num_ratings")) \
        .orderBy(desc("num_ratings")) \
        .show(n, truncate=False)


def show_top_avg_rating(ratings, min_ratings=10, n=20):
    """Show movies with the highest average rating (minimum threshold applied)."""
    print(f"\n=== Highest Avg Rating (≥{min_ratings} ratings, top {n}) ===")
    ratings.groupBy("movieId") \
        .agg(avg("rating").alias("avg_rating"), count("*").alias("num_ratings")) \
        .filter(col("num_ratings") >= min_ratings) \
        .orderBy(desc("avg_rating")) \
        .show(n, truncate=False)


def show_top_tags(tags, n=20):
    """Show the most frequently used tags."""
    print(f"\n=== Tag Usage (top {n}) ===")
    tags.groupBy("tag") \
        .count() \
        .orderBy(desc("count")) \
        .show(n, truncate=False)


def run_all(movies_node, ratings_rel, tags_rel, movie_genre_rel):
    """Run every analysis report in sequence."""
    print_row_counts(movies_node, ratings_rel, tags_rel, movie_genre_rel)
    print_distinct_counts(movies_node, ratings_rel, tags_rel, movie_genre_rel)
    show_movies_per_genre(movie_genre_rel)
    show_most_rated_movies(ratings_rel)
    show_most_active_users(ratings_rel)
    show_top_avg_rating(ratings_rel)
    show_top_tags(tags_rel)

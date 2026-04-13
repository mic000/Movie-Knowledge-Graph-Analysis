from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from preprocessor import load_data, clean_movies, clean_ratings, clean_tags, explode_genres

spark = SparkSession.builder.appName("MovieLens_Final_Clean").getOrCreate()


# def main():
#     movies_clean = clean_movies(movies)
#     ratings_clean = clean_ratings(ratings)
#     tags_clean = clean_tags(tags)
#     movies_genres = explode_genres(movies_clean)
#
#     return movies_clean, ratings_clean, tags_clean, movies_genres

def main():
    movies_raw = load_data("Data/Raw/movies.csv")
    ratings_raw = load_data("Data/Raw/ratings.csv")
    tags_raw = load_data("Data/Raw/tags.csv")

    movies_clean = clean_movies(movies_raw)
    ratings_clean = clean_ratings(ratings_raw)
    tags_clean = clean_tags(tags_raw)
    movie_genre_rel = explode_genres(movies_clean)

    re_genre_mID = movie_genre_rel.filter(col("genre") == "(no genres listed)").select("movieId")
    manual_fixes_df = spark.read.json("Data/Manual Fixes/movieId_genre.json")

    target_ids = [row.movieId for row in re_genre_mID.collect()]
    movie_genre_rel = movie_genre_rel.filter(~col("movieId").isin(target_ids)) \
        .union(manual_fixes_df.select("movieId", "genre")) \
        .distinct()

    # --- 3. 准备导出节点和关系 ---
    users_node = ratings_clean.select("userId").distinct()
    movies_node = movies_clean.select("movieId", "title", "release_year")
    genres_node = movie_genre_rel.select("genre").distinct()

    # 这里可以继续调用你原来的 export_single_csv 函数...
    print("清洗完成，准备导出...")


if __name__ == "__main__":
    main()
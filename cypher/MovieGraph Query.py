from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, trim, lower, split, explode, regexp_extract,
    from_unixtime, to_timestamp, to_date, count, countDistinct, round,
    avg, desc, when, length, sum, asc,sqrt

)
import builtins
import matplotlib.pyplot as plt
spark = SparkSession.builder.appName("MovieLens_Cleaning_Intelligence").getOrCreate()

genres_node     = spark.read.csv("genres_node.csv", header=True, inferSchema=True)
movies_node     = spark.read.csv("movies_node.csv", header=True, inferSchema=True)
users_node      = spark.read.csv("users_node.csv", header=True, inferSchema=True)
ratings_rel     = spark.read.csv("ratings_rel.csv", header=True, inferSchema=True)
m_t             = spark.read.csv("tags_rel.csv", header=True, inferSchema=True)
movie_genre_rel = spark.read.csv("movie_genre_rel.csv", header=True, inferSchema=True)



print("\n\n\n====== Data Intelligence/Summary After Manual Fixed ======")
print("==== Section 1 ====")
print("=== Total Number of Movies ===")
print(movies_node.count())
print("\n=== Total Number of Ratings ===")
print(ratings_rel.count())

print("\n=== Top 10 Most Rated Movies ===")
top_10_most_rated = ratings_rel.groupBy("movieId") \
    .agg(count("*").alias("num_ratings")) \
    .join(movies_node, on="movieId", how="left") \
    .select("movieId", "title", "release_year", "num_ratings") \
    .orderBy(desc("num_ratings"))
top_10_most_rated.show(10, truncate=False)

print("\n=== Top 10 Highest Rated Movies (min 50 ratings) ===")
top_10_highest_rated = ratings_rel.groupBy("movieId") \
    .agg(
        count("*").alias("num_ratings"),
        round(avg("rating"), 2).alias("avg_rating")
    ) \
    .filter(col("num_ratings") >= 50) \
    .join(movies_node, on="movieId", how="left") \
    .select("movieId", "title", "release_year", "num_ratings", "avg_rating") \
    .orderBy(desc("avg_rating"), desc("num_ratings"))
top_10_highest_rated.show(10, truncate=False)

print("\n=== Average Rating Across All Ratings ===")
ratings_rel.select(
    round(avg("rating"), 2).alias("overall_avg_rating")
).show()

print("\n=== Movies with Best Rated ===")
movies_with_5 = ratings_rel.groupBy("movieId") \
    .agg(
        sum(when(col("rating") == 5.0, 1).otherwise(0)).alias("num_top_rated"),
        count("*").alias("num_ratings"),
        round(avg("rating"), 2).alias("avg_rating")
    ) \
    .filter(col("num_top_rated") > 0) \
    .join(movies_node, on="movieId", how="left") \
    .select("title", "num_top_rated", "num_ratings", "avg_rating") \
    .orderBy(desc("num_top_rated"), desc("num_ratings"))
movies_with_5.show(20, truncate=False)

print("\n=== Movies with Perfect 5.0 Average Rating ===")
perfect_movies = ratings_rel.groupBy("movieId") \
    .agg(
        count("*").alias("num_ratings"),
        round(avg("rating"), 2).alias("avg_rating")
    ) \
    .filter(col("avg_rating") == 5.0) \
    .join(movies_node, on="movieId", how="left") \
    .select("title", "num_ratings", "avg_rating") \
    .orderBy(desc("num_ratings"))
perfect_movies.show(truncate=False)


print("\n\n==== Section 2 ====")
print("=== Total Number of Users ===")
print(users_node.count())

print("\n=== Average Ratings per User ===")
ratings_per_user = ratings_rel.groupBy("userId") \
    .agg(count("*").alias("num_ratings"))
ratings_per_user.select(
    round(avg("num_ratings"), 2).alias("avg_ratings_per_user")
).show()

print("\n=== Most Active User ===")
ratings_per_user.orderBy(desc("num_ratings")).show(10, truncate=False)

print("\n=== Least Active User ===")
ratings_per_user.orderBy(asc("num_ratings")).show(5, truncate=False)

print("\n=== User Activity Distribution ===")
user_activity_distribution = ratings_per_user.withColumn(
    "activity_bucket",
    when((col("num_ratings") >= 1) & (col("num_ratings") <= 20), "1-20")
    .when((col("num_ratings") >= 21) & (col("num_ratings") <= 50), "21-50")
    .when((col("num_ratings") >= 51) & (col("num_ratings") <= 100), "51-100")
    .when((col("num_ratings") >= 101) & (col("num_ratings") <= 200), "101-200")
    .otherwise("200+")
)
user_activity_distribution.groupBy("activity_bucket") \
    .agg(count("*").alias("num_users")) \
    .orderBy("activity_bucket") \
    .show(truncate=False)

print("\n=== Percentage of Power Users (200+ Ratings) ===")
total_users = ratings_per_user.count()
power_users = ratings_per_user.filter(col("num_ratings") >= 200).count()
print("Total users:", total_users)
print("Power users:", power_users)
power_user_pct = 100 * power_users / total_users
print("Power user percentage:", builtins.round(power_user_pct,2), "%")


print("\n\n==== Section 3 ====")
print("=== Total Number of Unique Genres ===")
print(genres_node.count())

print("\n=== Movies per Genre ===")
movies_per_genre = movie_genre_rel.groupBy("genre") \
    .agg(countDistinct("movieId").alias("movie_count")) \
    .orderBy(desc("movie_count"))
movies_per_genre.show(truncate=False)

print("\n=== Ratings per Genre ===")
ratings_per_genre = ratings_rel.join(
    movie_genre_rel,on="movieId",how="inner").groupBy("genre") \
 .agg(count("*").alias("total_ratings")) \
 .orderBy(desc("total_ratings"))
ratings_per_genre.show(truncate=False)

print("\n=== Average Rating per Genre ===")
avg_rating_per_genre = ratings_rel.join(
    movie_genre_rel,on="movieId",how="inner").groupBy("genre") \
    .agg(round(avg("rating"), 2).alias("avg_rating"), count("*").alias("total_ratings")) \
    .orderBy(desc("avg_rating"))
avg_rating_per_genre.show(truncate=False)

print("\n=== Most Popular Genre by Movie Count ===")
movies_per_genre.show(15, truncate=False)

print("\n=== Highest Rated Genre (min 15000 ratings) ===")
avg_rating_per_genre.filter(col("total_ratings") >= 15000) \
    .orderBy(desc("avg_rating"), desc("total_ratings")) \
    .show(5, truncate=False)

print("\n=== Genre Share Percentage/Proportion of Total Categories ===")
total_movies = movies_node.select("movieId").distinct().count()
genre_share_data = (movies_per_genre.withColumn("share_pct",
                            round(col("movie_count") * 100.0 / total_movies, 2)).orderBy(desc("share_pct")))
genre_share_data.show(12, truncate=False)


print("\n=== Multi-Genre Movies Count (3+ genres) ===")
multi_genre_movies = movie_genre_rel.groupBy("movieId") \
    .agg(countDistinct("genre").alias("genre_count")) \
    .filter(col("genre_count") >= 3)
print("Multi-genre movie count:", multi_genre_movies.count())
multi_genre_movies.join(movies_node, on="movieId", how="left") \
    .select("movieId", "title", "genre_count") \
    .orderBy(desc("genre_count")) \
    .show(5, truncate=False)


print("\n\n==== Section 4 ====")
print("=== Shared Movies Count Between User 1 and User 2 ===")
u1_movies = ratings_rel.filter(col("userId") == 1).select("movieId").distinct()
u2_movies = ratings_rel.filter(col("userId") == 2).select("movieId").distinct()
shared_movies = u1_movies.join(u2_movies, on="movieId", how="inner")
print("Shared movies:", shared_movies.count())

print("\n=== Cosine Similarity Between User 1 and User 2 ===")
u1 = ratings_rel.filter(col("userId") == 1).select(
    "movieId",
    col("rating").alias("rating1")
)
u2 = ratings_rel.filter(col("userId") == 2).select(
    "movieId",
    col("rating").alias("rating2")
)
common = u1.join(u2, on="movieId", how="inner")
cosine_df = common.select(
    sum(col("rating1") * col("rating2")).alias("dot_product"),
    round(sqrt(sum(col("rating1") * col("rating1"))), 4).alias("norm1"),
    round(sqrt(sum(col("rating2") * col("rating2"))), 4).alias("norm2")).withColumn(
    "cosine_similarity",
    round(col("dot_product") / (col("norm1") * col("norm2")), 4))
cosine_df.show(truncate=False)

print("\n=== Rating Distribution ===")
rating_distribution = ratings_rel.groupBy("rating") \
    .agg(count("*").alias("frequency")) \
    .orderBy("rating")
rating_distribution.show(truncate=False)


#### plot distribution graph
data = rating_distribution.collect()
ratings = [row["rating"] for row in data]
frequencies = [row["frequency"] for row in data]

plt.figure(figsize=(10, 6))
plt.bar(ratings, frequencies, width=0.35)
plt.xlabel("Rating Score")
plt.ylabel("Number of Ratings")
plt.title("Distribution of Movie Ratings")
plt.xticks(ratings)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig("Rating Distribution.png", dpi=800)
plt.show()


#### plot proportion of Movie in Genre graph
data = genre_share_data.collect()
genres = [row["genre"] for row in data]
shares = [float(row["share_pct"]) for row in data]
top_n = 10
labels = [f"{genres[i]} ({shares[i]:.1f}%)" if i < top_n else "" for i in range(len(genres))]

plt.figure(figsize=(10, 8))
wedges, texts = plt.pie(shares, labels=labels, startangle=180, labeldistance=1.08)
plt.title("Percentage of Movies by Genre")
plt.legend(wedges, genres, title="Genres",loc="center left", bbox_to_anchor=(0.99, 0.85))
plt.savefig("Proportion of Genre.png", dpi=800)
plt.tight_layout()
plt.show()

spark.stop()
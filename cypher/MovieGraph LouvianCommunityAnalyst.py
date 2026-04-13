from pyspark.sql import SparkSession
import pandas as pd
import networkx as nx
from itertools import combinations
from collections import Counter

spark = SparkSession.builder.appName("MovieLens_Cleaning_Intelligence").getOrCreate()

genres_node     = spark.read.csv("genres_node.csv", header=True, inferSchema=True)
movies_node     = spark.read.csv("movies_node.csv", header=True, inferSchema=True)
users_node      = spark.read.csv("users_node.csv", header=True, inferSchema=True)
ratings_rel     = spark.read.csv("ratings_rel.csv", header=True, inferSchema=True)
m_t             = spark.read.csv("tags_rel.csv", header=True, inferSchema=True)
movie_genre_rel = spark.read.csv("movie_genre_rel.csv", header=True, inferSchema=True)


print("======== Summary Communities in Movie Genre ========")
# 1. Convert Spark tables to pandas
movie_genre_pd = movie_genre_rel.select("movieId", "genre").toPandas()
movies_pd = movies_node.select("movieId", "title", "release_year").dropDuplicates(["movieId"]).toPandas()

# 2. Group movies by genre
genre_to_movies = movie_genre_pd.groupby("genre")["movieId"].apply(list).to_dict()

# 3. Count shared genres for each movie pair
pair_counter = Counter()
for genre, movie_list in genre_to_movies.items():
    unique_movies = sorted(set(movie_list))
    for m1, m2 in combinations(unique_movies, 2):
        pair_counter[(m1, m2)] += 1
# print(pair_counter)

# 4. Build graph
G = nx.Graph()
for movie_id in movies_pd["movieId"].tolist():
    G.add_node(movie_id)

for (m1, m2), shared_genres in pair_counter.items():
    if shared_genres >= 2:
        G.add_edge(m1, m2, weight=shared_genres)

# for n in G.nodes:
#     print(n, G.neighbors(n))
# for u,v in G.edges:
#     print(u, v)

print("Number of nodes in graph:", G.number_of_nodes())
print("Number of edges in graph:", G.number_of_edges())
print(nx.density(G))
print(nx.complete_graph(26701))
print(G.neighbors(26701))
# for n,neighbors in G.adjacency():
#     for neighbor,link_attributes in neighbors.items():
#         print('(%d, %d)' % (n,neighbor))
#
# nx.write_adjlist(G, "netfile.adjlist")
# G2 = nx.read_adjlist("netfile.adjlist")

# 5. Louvain community detection
# communities = nx.community.louvain_communities(G, weight="weight", seed=42)
# print("Number of communities detected:", len(communities))

# 6. Convert communities to table
# community_rows = []
# for community_id, nodes in enumerate(communities):
#     for movie_id in nodes:
#         community_rows.append((movie_id, community_id))
# movie_communities_pd = pd.DataFrame(community_rows, columns=["movieId", "community_id"])

# 7. Join movie metadata
# movie_communities_pd = movie_communities_pd.merge(
#     movies_pd[["movieId", "title", "release_year"]],
#     on="movieId",
#     how="left"
# )

# 8. Community size summary
# community_summary = movie_communities_pd.groupby("community_id").size().reset_index(name="num_movies")
# community_summary = community_summary.sort_values("num_movies", ascending=False)
# print(movie_communities_pd)
# print(community_summary)

# print("\n === Number of Communities Detected by Louvain in Genre ===")
# 9. Back to Spark if needed
# movie_communities = spark.createDataFrame(movie_communities_pd)
# community_summary_spark = spark.createDataFrame(community_summary)
# movie_communities.show(5, truncate=False)
# community_summary_spark.show(10, truncate=False)

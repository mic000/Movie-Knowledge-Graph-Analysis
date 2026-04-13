"""
io_utils.py
CSV export utilities — coalesce Spark output into a single clean CSV file.
Supports both Neo4j node/relationship exports and Power BI flat tables.
"""
import os
import glob
import shutil

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def save_csv(df, filename, sub_folder):
    """
    Write a Spark DataFrame to a single CSV file.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
    filename : str          – final CSV name, e.g. "movies_node.csv"
    sub_folder : str        – relative to project root, e.g. "output/neo4j"
    """
    out_dir = os.path.join(PROJECT_ROOT, sub_folder)
    os.makedirs(out_dir, exist_ok=True)

    temp_path = os.path.join(out_dir, "temp_" + filename)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

    # Spark writes part-00000-*.csv inside the temp folder → rename it
    part_file = glob.glob(os.path.join(temp_path, "part-*.csv"))[0]
    final_path = os.path.join(out_dir, filename)
    shutil.copyfile(part_file, final_path)
    shutil.rmtree(temp_path)

    print(f"  ✓ {final_path}")


def export_neo4j(users_node, movies_node, genres_node,
                 ratings_rel, movie_genre_rel, tags_rel):
    """Export the six CSV files used by Neo4j LOAD CSV."""
    print("\n=== Exporting Neo4j CSVs ===")
    tables = {
        "users_node.csv":      users_node,
        "movies_node.csv":     movies_node,
        "genres_node.csv":     genres_node,
        "ratings_rel.csv":     ratings_rel,
        "movie_genre_rel.csv": movie_genre_rel,
        "tags_rel.csv":        tags_rel,
    }
    for name, df in tables.items():
        save_csv(df, name, "output/neo4j")


def export_powerbi(movie_ratings_flat, movie_genre_stats, user_stats):
    """Export the three wide-table CSVs used by Power BI."""
    print("\n=== Exporting Power BI CSVs ===")
    tables = {
        "movie_ratings_flat.csv": movie_ratings_flat,
        "movie_genre_stats.csv":  movie_genre_stats,
        "user_stats.csv":         user_stats,
    }
    for name, df in tables.items():
        save_csv(df, name, "output/powerbi")
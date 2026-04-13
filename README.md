# Movie Knowledge Graph Analysis

A PySpark ETL pipeline that cleans the [MovieLens](https://grouplens.org/datasets/movielens/) dataset and exports structured CSVs for **Neo4j** knowledge graph construction and **Power BI** visual analytics.
 
> **Course**: CSC 501 — University of Victoria
 
---
 
## Project Structure

```
Movie-Knowledge-Graph-Analysis/
├── data/
│   ├── raw/                    # Original MovieLens CSVs (not tracked by git)
│   │   ├── movies.csv
│   │   ├── ratings.csv
│   │   └── tags.csv
│   └── manual fixes/
│       └─── movieId_genre.json # Manual genre patches for movies missing genre info
├── scripts/
│   ├── preprocessor.py         # Data loading & cleaning functions
│   ├── data_analysis.py        # Standalone analysis report functions
│   ├── io_utils.py             # CSV export utilities (Neo4j / Power BI)
│   └── main.py                 # Pipeline orchestrator (entry point)
├── output/                     # Generated at runtime
│   ├── neo4j/                  # 6 CSVs for Neo4j LOAD CSV
│   ├── powerbi/                # 3 wide-table CSVs for Power BI
│   └── exploration_log.txt     # Full pipeline log with analysis results
├── .gitignore
├── requirements.txt
├── README.md
└── README_zh.md
```

## Pipeline Overview

```
Raw CSVs ──▶ Clean & Normalize ──▶ Remove Junk ──▶ Manual Fixes ──▶ Explore ──▶ Export
                  │                     │                │              │           │
            type casting          null year +       patch title,    row counts    Neo4j CSVs
            deduplication        no genre +          year, genre    genre dist.  Power BI CSVs
            null filtering        no tags            from JSON      top movies   exploration_log
            year extraction       → delete                          top users
            genre explosion                                         top tags
```

### Step Details

| Step | What it does |
|------|-------------|
| **Load & Clean** | Cast data types, trim strings, deduplicate, filter invalid ratings (outside 0.5–5.0), extract `release_year` from title, explode pipe-delimited genres into rows, remove IMAX pseudo-genre |
| **Remove Junk** | Delete movies that have null release year AND `(no genres listed)` AND no user tags — these are data noise |
| **Manual Fixes** | Patch 6 known movies with correct titles/years; replace `(no genres listed)` entries using `manual_fixes.json` |
| **Explore** | Print summary statistics — row counts, distinct values, genre distribution, most-rated movies, most active users, highest-rated movies, top tags |
| **Export** | Write final CSVs to `output/neo4j/` and `output/powerbi/` |

All exploration output is simultaneously printed to console and saved to `output/exploration_log.txt`.

## Quick Start

### Prerequisites

- **Python** 3.9+
- **Java** 8 / 11 / 17 (required by PySpark)

### 1. Clone & Install

```bash
git clone https://github.com/<your-username>/Movie-Knowledge-Graph-Analysis.git
cd Movie-Knowledge-Graph-Analysis
pip install -r requirements.txt
```

### 2. Add Data

Download the [MovieLens Latest Small](https://grouplens.org/datasets/movielens/latest/) dataset and place the three CSVs into `data/raw/`:

```
data/raw/movies.csv
data/raw/ratings.csv
data/raw/tags.csv
```

### 3. Run

```bash
cd scripts
python main.py
```

### 4. Check Output

```
output/
├── neo4j/
│   ├── users_node.csv
│   ├── movies_node.csv
│   ├── genres_node.csv
│   ├── ratings_rel.csv
│   ├── movie_genre_rel.csv
│   └── tags_rel.csv
├── powerbi/
│   ├── movie_ratings_flat.csv
│   ├── movie_genre_stats.csv
│   └── user_stats.csv
└── exploration_log.txt          ← full pipeline log
```

## Output Schema

### Neo4j Nodes

| File | Columns | Description |
|------|---------|-------------|
| `users_node.csv` | `userId` | Distinct user IDs |
| `movies_node.csv` | `movieId`, `title`, `release_year` | Movie metadata |
| `genres_node.csv` | `genre` | Distinct genre labels |

### Neo4j Relationships

| File | Columns | Description |
|------|---------|-------------|
| `ratings_rel.csv` | `userId`, `movieId`, `rating`, `rating_datetime`, `rating_date` | User → Movie ratings |
| `movie_genre_rel.csv` | `movieId`, `genre` | Movie → Genre mapping |
| `tags_rel.csv` | `userId`, `movieId`, `tag`, `tag_datetime`, `tag_date` | User → Movie tags |

### Power BI Tables

| File | Description |
|------|-------------|
| `movie_ratings_flat.csv` | Denormalized: each rating row joined with movie info and genre |
| `movie_genre_stats.csv` | Aggregated rating count & average per movie-genre combination |
| `user_stats.csv` | Per-user rating count & average |

## Module Reference

| Module | Responsibility |
|--------|---------------|
| `preprocessor.py` | `load_csv()`, `load_json()`, `clean_movies()`, `clean_ratings()`, `clean_tags()`, `explode_genres()` |
| `data_analysis.py` | `run_all()` — standalone analysis reports (genre distribution, top movies, active users, etc.) |
| `io_utils.py` | `save_csv()`, `export_neo4j()`, `export_powerbi()` — coalesce Spark partitions into single CSVs |
| `main.py` | Pipeline orchestrator — runs all steps in order, writes exploration log |

## Data Source

[MovieLens Latest Small](https://grouplens.org/datasets/movielens/latest/) — 100,000 ratings and 3,600 tag applications across 9,700 movies by 600 users. Last updated 9/2018.

> F. Maxwell Harper and Joseph A. Konecny. 2015. The MovieLens Datasets: History and Context. *ACM Transactions on Interactive Intelligent Systems* 5, 4, Article 19.

## License

This project is for academic use.

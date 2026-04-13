# Movie Knowledge Graph Analysis

**[中文版](README_zh.md)**

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
├── cypher/
│   ├── schema.cypher               # Constraints & indexes
│   ├── import_nodes.cypher          # LOAD CSV — create nodes
│   ├── import_relationships.cypher  # LOAD CSV — create relationships
│   └── queries.cypher               # Sample analytical queries
├── output/                     # Generated at runtime
│   ├── neo4j/                  # 6 CSVs for Neo4j LOAD CSV
│   └── powerbi/                # 3 wide-table CSVs for Power BI
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
- **Neo4j Desktop** 4.x / 5.x (for graph import)

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

### 4. Import into Neo4j
 
Copy the CSVs from `output/neo4j/` into your Neo4j `import/` directory, then run the Cypher scripts in order:
 
```
neo4j/schema.cypher                 ← constraints & indexes
neo4j/import_nodes.cypher           ← create User, Movie, Genre nodes
neo4j/import_relationships.cypher   ← create RATED, HAS_GENRE, TAGGED relationships
neo4j/queries.cypher                ← sample analytical queries
```
 
## Output Schema
 
### Neo4j Graph Model
 
```
(:User) ─[:RATED {rating, rating_datetime, rating_date}]─▶ (:Movie {movieId, title, release_year})
(:User) ─[:TAGGED {tag, tag_datetime, tag_date}]──────────▶ (:Movie)
(:Movie) ─[:HAS_GENRE]───────────────────────────────────▶ (:Genre {name})
```
 
### Neo4j CSV Files (`output/neo4j/`)
 
| File | Columns | Description |
|------|---------|-------------|
| `users_node.csv` | `userId` | Distinct user IDs |
| `movies_node.csv` | `movieId`, `title`, `release_year` | Movie metadata |
| `genres_node.csv` | `genre` | Distinct genre labels |
| `ratings_rel.csv` | `userId`, `movieId`, `rating`, `rating_datetime`, `rating_date` | User → Movie ratings |
| `movie_genre_rel.csv` | `movieId`, `genre` | Movie → Genre mapping |
| `tags_rel.csv` | `userId`, `movieId`, `tag`, `tag_datetime`, `tag_date` | User → Movie tags |

## Neo4j Cypher Scripts
 
| Script | Purpose |
|--------|---------|
| `schema.cypher` | Create uniqueness constraints and indexes for fast lookups |
| `import_nodes.cypher` | `LOAD CSV` commands to create `:User`, `:Movie`, `:Genre` nodes |
| `import_relationships.cypher` | `LOAD CSV` commands to create `:RATED`, `:HAS_GENRE`, `:TAGGED` relationships |
| `queries.cypher` | Sample queries — top-rated movies, genre co-occurrence, user similarity, recommendation paths |
 
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

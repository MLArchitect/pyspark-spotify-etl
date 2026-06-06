# Spotify Album Data Pipeline

An end-to-end data pipeline that pulls new album releases from the Spotify API and processes them through a lakehouse-style medallion architecture using PySpark and Delta Lake.

## Overview

This project connects to the Spotify Web API, extracts new album release data, and transforms it through three layers:

- **Bronze** — raw data ingested as-is into Delta tables
- **Silver** — cleaned and modeled into a star schema (fact + dimension tables with bridge tables for many-to-many relationships)
- **Gold** — aggregated business-level summaries for artist performance and album growth trends

**Stack:** Python, PySpark, Delta Lake, Spotify Web API, Docker, GitHub Actions

For the full architecture diagram, see [docs/architecture.md](docs/architecture.md).

## Data Models

### Silver Layer (Star Schema)

| Table | Description |
|-------|-------------|
| `fact_albums` | Core album attributes |
| `dim_artists` | Artist details |
| `dim_images` | Album artwork metadata |
| `bridge_artists_albums` | Artist-to-album relationships |
| `bridge_images_albums` | Image-to-album relationships |

### Gold Layer

| Table | Description |
|-------|-------------|
| `gold_artists` | Per-artist metrics with tier classification, Z-ORDER optimized |
| `gold_artist_albums_growth` | Album growth trends over time |

## Project Structure

```
├── .github/workflows/ci.yml          # GitHub Actions CI
├── docs/architecture.md              # Mermaid architecture diagrams
├── src/
│   ├── general_functions/
│   │   ├── spark_manager.py           # Spark session (singleton)
│   │   ├── upsert_into_path.py        # Delta MERGE with schema evolution
│   │   ├── read_path_into_spark.py    # Read helpers
│   │   ├── write_into_path.py         # Write helpers
│   │   ├── parser.py                  # API calls with retry logic
│   │   └── access_token_generator.py  # Spotify OAuth token
│   └── pipelines/
│       └── album_release/
│           ├── extraction_layer/      # Spotify API extraction
│           ├── silver_layer/          # Transformations, data quality, profiling
│           └── gold_layer/            # Business aggregations
├── scripts/
│   ├── step_01 → step_04             # Individual pipeline steps
│   └── run_full_pipeline.py           # Orchestrator
├── tests/                             # Unit tests (pytest)
├── utils/
│   ├── config.py                      # Configuration
│   └── logger.py                      # Logging setup
├── Dockerfile
├── Makefile
├── .env.example
└── requirements.txt
```

## Setup

### Prerequisites

- Python 3.8+
- Java 8 or 11
- A Spotify Developer account ([developer.spotify.com](https://developer.spotify.com))

### Installation

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in your Spotify credentials
```

### Running the Pipeline

```bash
# Full pipeline
make pipeline

# Individual layers
make bronze
make silver
make gold
```

### Running with Docker

```bash
docker build -t spotify-pipeline .
docker run --env-file .env spotify-pipeline
```

## Highlights

- **Incremental loads** — uses Delta Lake MERGE to upsert records by primary key, so re-runs only process new or changed data
- **Schema evolution** — new fields from the API are handled automatically via `mergeSchema`
- **Z-ORDER optimization** — applied on the gold artist table for faster multi-column queries
- **Data quality checks** — validates referential integrity between fact and dimension tables and checks field completeness
- **Data profiling** — profiles key columns in the silver layer
- **Retry logic** — API calls use exponential backoff to handle rate limits and transient failures
- **Broadcast joins** — small dimension tables are broadcast for efficient joins
- **CI/CD** — GitHub Actions runs tests on every push and pull request

## Tests

```bash
pytest tests/ -v
```

Tests cover:
- API parser (retries, timeouts, error handling)
- Token generation (auth flows, error cases)
- Upsert logic (empty DataFrames, missing PKs, null filtering)
- Silver transformations (deduplication, derived columns, empty inputs)
- Data quality checks (completeness thresholds, referential integrity)

## License

MIT

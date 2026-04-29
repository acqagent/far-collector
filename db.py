"""DuckDB schema. Adds FAR-specific tables on top of the doc's urls/pages/runs."""
import duckdb
from pathlib import Path

DB = Path(__file__).parent / "data" / "collector.duckdb"


def init():
    con = duckdb.connect(DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            url VARCHAR PRIMARY KEY,
            status VARCHAR,
            depth INTEGER,
            source VARCHAR,
            found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fetched_at TIMESTAMP,
            content_hash VARCHAR,
            error VARCHAR
        );
        CREATE TABLE IF NOT EXISTS pages (
            url VARCHAR PRIMARY KEY,
            title VARCHAR,
            author VARCHAR,
            published VARCHAR,
            body TEXT,
            topics VARCHAR[],
            relevance DOUBLE,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS runs (
            run_id VARCHAR PRIMARY KEY,
            prompt TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ended_at TIMESTAMP,
            urls_collected INTEGER
        );
        CREATE TABLE IF NOT EXISTS far_provisions_clauses (
            number VARCHAR PRIMARY KEY,
            title VARCHAR,
            kind VARCHAR,
            effective_date VARCHAR,
            full_text TEXT,
            source_url VARCHAR,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS far_class_deviations (
            id VARCHAR PRIMARY KEY,
            agency VARCHAR,
            deviation_number VARCHAR,
            title VARCHAR,
            effective_date VARCHAR,
            scope TEXT,
            link VARCHAR,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    con.close()


def get():
    return duckdb.connect(DB)


if __name__ == "__main__":
    init()
    print(f"DB ready at {DB}")

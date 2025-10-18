import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta
import sqlite3
import os


def peek_db(db_path="data/storage/news_articles.db"):
    with sqlite3.connect(db_path) as conn:
        # version
        ver = pd.read_sql_query("SELECT sqlite_version() AS version;", conn)
        print("SQLite version:", ver.loc[0, "version"])

        # tables
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", conn
        )
        print("\nTables:\n", tables)

        # schema
        schema = pd.read_sql_query("PRAGMA table_info(news_article);", conn)
        print("\nSchema of news_article:\n", schema)

        # counts
        n = pd.read_sql_query("SELECT COUNT(*) AS rows FROM news_article;", conn)
        print("\nRow count:", int(n.loc[0, "rows"]))

        # sample rows
        sample = pd.read_sql_query(
            "SELECT source, title, url, published_at, scraped_at FROM news_article LIMIT 5;", conn
        )
        print("\nSample rows:\n", sample)

        # min/max timestamps
        rng = pd.read_sql_query("""
            SELECT
                MIN(published_at) AS min_published,
                MAX(published_at) AS max_published,
                MIN(scraped_at)   AS min_scraped,
                MAX(scraped_at)   AS max_scraped
            FROM news_article;
        """, conn)
        print("\nTimestamp ranges:\n", rng)

        # duplicates by url (should be none if url is PRIMARY KEY)
        dups = pd.read_sql_query("""
            SELECT url, COUNT(*) AS c
            FROM news_article
            GROUP BY url
            HAVING c > 1
            ORDER BY c DESC;
        """, conn)
        if len(dups):
            print("\nDuplicate URLs found:\n", dups)
        else:
            print("\nNo duplicate URLs (PK enforcement OK).")


if __name__ == "__main__":
    os.makedirs('data/storage', exist_ok=True)
    DB_PATH = 'data/storage/news_articles.db'
    records = pd.read_parquet(
        f'data/transformed/news_{datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")}.parquet',
        engine='pyarrow',
    ).to_dict(orient='records')
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            curr = conn.cursor()
            curr.execute("""
                CREATE TABLE IF NOT EXISTS news_article (
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url   TEXT PRIMARY KEY,
                    published_at TEXT,
                    scraped_at   TEXT
                )
            """)

            curr.executemany("""
                INSERT INTO news_article (source, title, url, published_at, scraped_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    source = excluded.source,
                    title = excluded.title,
                    published_at = excluded.published_at,
                    scraped_at = excluded.scraped_at
            """, [
                (r["source"], r["title"], r["url"], r["published_at"], r["scraped_at"])
                for r in records
            ])
            conn.commit()

            print(f"Upserted {len(records)} rows into {DB_PATH}")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise

    # to check the database contents
    # peek_db()
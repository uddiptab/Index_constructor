import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))



from app.db.duckdb_redis import DBManager

def create_tables():
    conn = DBManager().conn

    conn.execute("""
    CREATE TABLE IF NOT EXISTS daily_data (
        ticker TEXT,
        date DATE,
        market_cap DOUBLE,
        close DOUBLE
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS index_composition (
        date DATE,
        ticker TEXT,
        weight DOUBLE
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS index_performance (
        date DATE,
        daily_return DOUBLE,
        cumulative_return DOUBLE
    )
    """)

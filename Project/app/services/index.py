import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pandas as pd
from fastapi.responses import FileResponse
from app.db.duckdb_redis import DBManager
from app.core.base import logger
import tempfile

def build_index(start_date, end_date):
    db = DBManager().conn
    redis = DBManager().redis
    if not end_date:
        end_date = start_date

    df = db.execute(f"""
    SELECT * FROM daily_data WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """).fetchdf()

    all_days = pd.date_range(start=start_date, end=end_date)
    perf_records = []
    comp_records = []

    for day in all_days:
        sub = df[df['date'] == pd.Timestamp(day).date()]
        if len(sub) < 100:
            continue
        top100 = sub.sort_values("market_cap", ascending=False).head(100)
        top100['weight'] = 1.0 / 100
        comp_records.append(top100[['date', 'ticker', 'weight']])
        redis.set(f"composition:{day.date()}", top100.to_json())

        perf = top100['close'].pct_change().mean()
        perf_records.append({'date': day.date(), 'daily_return': perf})

    if comp_records:
        all_comp = pd.concat(comp_records)
        db.execute("INSERT INTO index_composition SELECT * FROM all_comp")

    perf_df = pd.DataFrame(perf_records)
    perf_df['cumulative_return'] = (1 + perf_df['daily_return']).cumprod() - 1
    db.execute("INSERT INTO index_performance SELECT * FROM perf_df")

    return {"status": "built", "days": len(perf_records)}

def get_performance(start_date, end_date):
    redis = DBManager().redis
    key = f"performance:{start_date}:{end_date}"
    if cached := redis.get(key):
        return pd.read_json(cached).to_dict(orient='records')

    db = DBManager().conn
    df = db.execute(f"""
        SELECT * FROM index_performance WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """).fetchdf()
    redis.set(key, df.to_json())
    return df.to_dict(orient='records')

def get_composition(date):
    redis = DBManager().redis
    key = f"composition:{date}"
    if cached := redis.get(key):
        return pd.read_json(cached).to_dict(orient='records')

    db = DBManager().conn
    df = db.execute(f"SELECT * FROM index_composition WHERE date = '{date}'").fetchdf()
    redis.set(key, df.to_json())
    return df.to_dict(orient='records')

def get_composition_changes(start_date, end_date):
    db = DBManager().conn
    df = db.execute(f"""
        SELECT date, ticker FROM index_composition 
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """).fetchdf()

    grouped = df.groupby('date')['ticker'].apply(set).reset_index()
    grouped['prev'] = grouped['ticker'].shift()
    grouped['entered'] = grouped.apply(lambda row: list(row['ticker'] - row['prev']) if pd.notna(row['prev']) else [], axis=1)
    grouped['exited'] = grouped.apply(lambda row: list(row['prev'] - row['ticker']) if pd.notna(row['prev']) else [], axis=1)
    return grouped[['date', 'entered', 'exited']].to_dict(orient='records')

def export_to_excel(start_date, end_date):
    db = DBManager().conn
    perf = db.execute(f"SELECT * FROM index_performance WHERE date BETWEEN '{start_date}' AND '{end_date}'").fetchdf()
    comp = db.execute(f"SELECT * FROM index_composition WHERE date BETWEEN '{start_date}' AND '{end_date}'").fetchdf()
    changes = get_composition_changes(start_date, end_date)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        file_path = tmp.name
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            perf.to_excel(writer, sheet_name="Performance", index=False)
            comp.to_excel(writer, sheet_name="Composition", index=False)
            pd.DataFrame(changes).to_excel(writer, sheet_name="Changes", index=False)

        logger.info(f"Exported Excel to {file_path}")
        return FileResponse(file_path, filename="index_export.xlsx")

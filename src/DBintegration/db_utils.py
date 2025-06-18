# db_utils.py
# Contains utility functions for saving stock data to the database and previewing or clearing data.
from __future__ import annotations
import sys
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pathlib import Path
from DBintegration.database import engine
from DBintegration.models import SP500Index
from DBintegration.models import DailyStockData
from DBintegration.models import Base
from sqlalchemy.orm import Session
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy import delete
from sqlalchemy import MetaData
from alpha_vantage.timeseries import TimeSeries
from collections import defaultdict
from datetime import date
from sqlalchemy.dialects.postgresql import insert as pg_insert
from DBintegration.models import BetaCalculation  # → ORM model that maps to beta_calculation
from sqlalchemy.dialects.postgresql import insert
from itertools import islice 
from sqlalchemy import text
from contextlib import contextmanager
from typing import List, Dict, Iterable

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY in environment variables")
from DBintegration.database import SessionLocal
sys.path.append(str(Path(__file__).resolve().parents[1])) 
env_path = Path(__file__).resolve().parents[2] / "Algo_env" / ".env"
load_dotenv(dotenv_path=env_path)

def remove_data(model, symbols):
    """
    Deletes all rows from the `model` table where the `symbol` column matches 
    any entry in the provided `symbols` list.

    Args:
        model:      The SQLAlchemy model class (e.g., DailyStockData, SectorData, SP500Index).
        symbols:    A list of symbol strings to delete from the table.
    """
    session = SessionLocal()
    try:
        if not symbols:
            print("No symbols provided to remove.")
            return

        # Construct and execute a bulk DELETE statement
        stmt = delete(model).where(model.symbol.in_(symbols))
        result = session.execute(stmt)
        session.commit()

        deleted_count = result.rowcount if hasattr(result, 'rowcount') else None
        print(f"✅ Removed rows for symbols {symbols}. Deleted count: {deleted_count}")

    except Exception as e:
        session.rollback()
        print(f"❌ Error deleting data for symbols {symbols}: {e}")
    finally:
        session.close()

#############################################################################
def update_data(model, df):
    """
    Updates the database with only the changes in the DataFrame.
    - Existing rows are updated if any field has changed.
    - New rows are inserted.
    - Rows not in df but present in DB are left untouched.
    """
    session = SessionLocal()
    Base.metadata.create_all(bind=engine)  # Ensure tables are created
    df = df.copy()

    # Detect date column
    date_column = next((col for col in df.columns if col.lower() in ['date', 'datetime']), None)
    if date_column is None:
        print("❌ No date column found in DataFrame.")
        session.close()
        return

    df[date_column] = pd.to_datetime(df[date_column]).dt.date

    if df.empty:
        print("❌ DataFrame is empty.")
        session.close()
        return

    primary_keys = ['symbol', 'date']  # You can generalize if your model differs

    updates = 0
    inserts = 0

    for _, row in df.iterrows():
        try:
            query = {key: row[key] for key in primary_keys if key in row}
            existing = session.query(model).filter_by(**query).first()

            row_data = {col.name: row[col.name] for col in model.__table__.columns if col.name in row}

            if existing:
                # Check for differences
                changed = False
                for field, value in row_data.items():
                    if getattr(existing, field) != value:
                        setattr(existing, field, value)
                        changed = True
                if changed:
                    updates += 1
            else:
                new_obj = model(**row_data)
                session.add(new_obj)
                inserts += 1

            if (updates + inserts) % 100 == 0:
                session.commit()

        except Exception as e:
            print(f"⚠️ Error processing row {row.to_dict()}: {e}")
            session.rollback()
            continue

    try:
        session.commit()
        print(f"✅ Update complete. {updates} updated, {inserts} inserted.")
    except Exception as e:
        session.rollback()
        print(f"❌ Final commit failed: {e}")
    finally:
        session.close()
#########################################################################################################
def model_to_dataframe(model_class): ###input: model, output: 
    """
    Given a SQLAlchemy model class, return a Pandas DataFrame of all its rows.
    
    Args:
        model_class: The SQLAlchemy model class (e.g., StockPrice, User, etc.)
    
    Returns:
        pd.DataFrame: DataFrame with the table's data.
    """
    session: Session = SessionLocal()
    try:
        results = session.query(model_class).all()
        if not results:
            return pd.DataFrame()  # empty table

        data = [row.__dict__.copy() for row in results]
        for row in data:
            row.pop("_sa_instance_state", None)

        return pd.DataFrame(data)
    finally:
        session.close()
##########################################################################################################
def delete_all_rows(model: DeclarativeMeta):
    """
    Deletes all rows from the table associated with the given SQLAlchemy model class.

    Parameters:
        model (DeclarativeMeta): A SQLAlchemy model class.
    """
    session = SessionLocal()
    try:
        session.execute(delete(model))
        session.commit()
        print(f"All rows deleted from {model.__tablename__}")
    except Exception as e:
        session.rollback()
        print(f"Error deleting rows from {model.__tablename__}: {e}")
    finally:
        session.close()

def save_dataframe_to_db(symbol, df):
    """
    Saves a stock's historical data from a DataFrame to the database.
    Filters out existing entries and commits in batches of 100.
    """
    session = SessionLocal()
    Base.metadata.create_all(bind=engine) #sync model with DB

    df = df.copy()
    symbol = symbol.upper()
    
    date_column = None
    for col in df.columns:
        if col in ['Date', 'Datetime', 'date', 'datetime']:
            date_column = col
            break
    
    if date_column is None:
        print(f"❌ No date column found in DataFrame for {symbol}")
        print(f"Available columns: {df.columns.tolist()}")
        session.close()
        return
    
    df[date_column] = pd.to_datetime(df[date_column])
    
    if df.empty:
        print(f"❌ DataFrame for {symbol} is empty.")
        session.close()
        return  
    required_columns = ["Open", "High", "Low", "Close"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Missing columns for {symbol}: {missing_columns}")
        session.close()
        return
    
    records_added = 0
    for idx, row in df.iterrows():
        try:
   
            stock_data = {
                'symbol': symbol,
                'date': row[date_column].date(),
                'open': float(row["Open"]),
                'high': float(row["High"]),
                'low': float(row["Low"]),
                'close': float(row["Close"]),
            }

            # Volume is optional
            if 'Volume' in df.columns:
                stock_data['volume'] = int(row["Volume"]) if not pd.isna(row["Volume"]) else None
            
            stock = StockPrice(**stock_data)
            session.add(stock)
            records_added += 1
            
            if records_added % 100 == 0:
                session.commit()
                print(f"Committed {records_added} records so far for {symbol}")
                
        except Exception as e:
            print(f"⚠️ Skipping row {idx} due to error: {e}")
            continue
    
    try:
        if records_added % 100 != 0:
            session.commit()
        print(f"✅ {symbol} saved successfully. Total records: {records_added}")
    except Exception as e:
        session.rollback()
        print(f"❌ Error saving {symbol} to DB: {e}")
    finally:
        session.close()

def fetch_and_store_data(symbol: str, model: str):
    """
    Fetches full daily historical data for a given symbol (stock or index or sector),
    processes the data, and stores it into the appropriate database table
    based on the given model.

    Parameters:
        symbol (str): The stock/index symbol to fetch .
        model (str): 'index' to store in SP500Index, 'stock' to store in DailyStockData, 'sector
                    'sector' to store in SectorData."""
    
    if model not in ['index', 'stock', 'sector']:
        raise ValueError("Model must be either 'index', 'stock', or 'sector'.")
    
    ts = TimeSeries(key=API_KEY, output_format='pandas')

    print(f"Fetching full daily data for {symbol}")
    try:
        data, meta_data = ts.get_daily(symbol=symbol, outputsize='full')
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return

    if data.empty:
        print(f"No data fetched for {symbol}.")
        return

    data = data.sort_index()
    data = data.loc["2013-01-01":"2024-12-31"]

    data = data.rename(columns={
        '1. open': 'Open',
        '2. high': 'High',
        '3. low': 'Low',
        '4. close': 'Close',
        '5. volume': 'Volume'
    })

    session = SessionLocal()
    try:
        for date, row in data.iterrows():
            volume = row['Volume']
            volume = int(volume) if not pd.isna(volume) else None

            if model == 'index':
                entry = SP500Index(
                    date=date.date(),
                    open=round(float(row['Open']), 2),
                    high=round(float(row['High']), 2),
                    low=round(float(row['Low']), 2),
                    close=round(float(row['Close']), 2),
                    volume=volume
                )
            elif model == 'stock':
                entry = DailyStockData(
                    symbol=symbol,
                    date=date.date(),
                    open=round(float(row['Open']), 2),
                    high=round(float(row['High']), 2),
                    low=round(float(row['Low']), 2),
                    close=round(float(row['Close']), 2),
                    volume=volume
                )

            else:
                raise ValueError("Model must be either 'index' or 'stock'.")

            session.merge(entry)
        session.commit()
        print(f"{symbol} data saved to database in '{model}' model.")
    except Exception as e:
        session.rollback()
        print(f"Error inserting data for {symbol}: {e}")
    finally:
        session.close()

def fetch_and_store_sector_etfs(etf_list=None):
    """
    Fetches and stores historical daily OHLCV data for a list of sector ETFs
    from Alpha Vantage into the 'sector_data' table in the database.
    """
    if etf_list is None:
        etf_list = ["XLF", "XLK", "XLE", "XLI", "XLY", "XLV", "XLP", "XLU", "XLC", "XLRE", "XLB"]

    ts = TimeSeries(key=API_KEY, output_format='pandas')

    for symbol in etf_list:
        try:
            print(f"📥 Fetching data for {symbol}...")
            data, meta = ts.get_daily(symbol=symbol, outputsize='full')

            if data.empty:
                print(f"⚠️ No data returned for {symbol}")
                continue

            data = data.sort_index()
            data = data.loc["2013-01-01":"2024-12-31"]

            data = data.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })

            data = data.reset_index()
            data = data.rename(columns={"date": "date"})  # for clarity
            data["symbol"] = symbol
            data["date"] = pd.to_datetime(data["date"]).dt.date

            df = data[["date", "open", "high", "low", "close", "volume", "symbol"]].copy()

            update_data(SectorData, df)
            print(f"✅ {symbol} saved to DB.")

        except Exception as e:
            print(f"❌ Error processing {symbol}: {e}")

def drop_table(model):
    """
    Drops the entire table from the database (irreversible).
    """
    try:
        table_name = model.__tablename__
        metadata = MetaData()
        metadata.reflect(bind=engine)
        if table_name in metadata.tables:
            table = metadata.tables[table_name]
            table.drop(bind=engine)
            print(f"✅ Table '{table_name}' dropped successfully.")
        else:
            print(f"⚠️ Table '{table_name}' not found in metadata.")
    except Exception as e:
        print(f"❌ Error dropping table: {e}")

def update_score_from_csv(csv_path: str):
    """
    Updates the 'score' column in the SP500Index table using the values from the last column
    of a given CSV file. Matches by date.
    """
    session = None
    try:
        df = pd.read_csv(csv_path)

        if 'date' not in df.columns:
            print("❌ CSV missing 'date' column.")
            return

        if 'regime_signal_combined' not in df.columns:
            print("❌ CSV missing 'regime_signal_combined' column.")
            return

        df['date'] = pd.to_datetime(df['date']).dt.date
        session = SessionLocal()
        updated_count = 0

        dates = df['date'].tolist()
        records = session.query(SP500Index).filter(SP500Index.date.in_(dates)).all()
        record_map = {r.date: r for r in records}

        for _, row in df.iterrows():
            day = row['date']
            score = row['regime_signal_combined']

            record = record_map.get(day)
            if record:
                record.score = score
                updated_count += 1

        session.commit()
        print(f"✅ Updated 'score' for {updated_count} rows from {csv_path}")
    except Exception as e:
        print(f"❌ Failed to update scores: {e}")
    finally:
        if session:
            session.close()

def bulk_load_stocks_optimized(
    csv_path: str,
    chunk_size: int  = 50_000,  
    batch_size: int  = 5_000,   
):
    """
    Efficiently loads a large CSV file containing stock data into the database
    in safe, deduplicated chunks, supporting re-runnable operation.
    """
    csv_path = Path(csv_path).expanduser().resolve()
    if not csv_path.exists():
        print(f"❌ CSV not found: {csv_path}")
        return

    print(f"⚡ Loading {csv_path} in chunks of {chunk_size:,} rows…")

    required_cols = {
        "date", "Ticker", "Open", "High", "Low",
        "Close", "Volume", "Return", "SP_return"
    }

    total_rows = inserted_rows = 0
    chunk_no = 0

    # Helper to split list into fixed-size batches
    def grouper(seq, n):
        it = iter(seq)
        while True:
            batch = list(islice(it, n))
            if not batch:
                break
            yield batch

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        chunk_no += 1
        total_rows += len(chunk)
        print(f"📦  Chunk {chunk_no}: {len(chunk):,} rows")

        # Skip chunks missing required columns
        if missing := (required_cols - set(chunk.columns)):
            print(f"⚠️  Chunk {chunk_no} skipped – missing columns: {missing}")
            continue

        # Rename CSV columns to match DB model field names
        chunk = chunk.rename(columns={
            "Ticker":     "symbol",
            "Return":     "return_daily",
            "SP_return":  "sp_return",
            "Open":       "open",
            "High":       "high",
            "Low":        "low",
            "Close":      "close",
            "Volume":     "volume",
            "date":       "date",
        })
        chunk.columns = [c.lower() for c in chunk.columns]

        # Clean rows missing key fields
        chunk = chunk.dropna(subset=["symbol", "date"])
        chunk["date"] = pd.to_datetime(
            chunk["date"], format="%d%b%Y", errors="coerce"
        ).dt.date
        chunk = chunk.dropna(subset=["date"])

        # Convert numeric columns, coerce bad values to NaN
        numeric_cols = [
            "open", "high", "low", "close",
            "volume", "return_daily", "sp_return"
        ]
        chunk[numeric_cols] = chunk[numeric_cols].apply(
            pd.to_numeric, errors="coerce"
        )

        # Drop duplicates within each chunk to prevent redundant upserts
        before = len(chunk)
        chunk = (
            chunk.sort_values("date")
                  .drop_duplicates(
                     subset=["symbol", "date"],
                     keep="last"
                  )
        )
        duplicates_dropped = before - len(chunk)
        if duplicates_dropped:
            print(f"   ↪️  {duplicates_dropped:,} intra-chunk duplicates removed")

        # Convert DataFrame to list of dicts for DB insertion
        used_fields = [
            "symbol", "date", "open", "high", "low", "close",
            "volume", "return_daily", "sp_return"
        ]
        records = chunk[used_fields].to_dict(orient="records")
        if not records:
            continue

        # Insert data in safe batches with upsert logic
        session = SessionLocal()
        try:
            for batch in grouper(records, batch_size):
                stmt = insert(DailyStockData).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol", "date"],
                    set_={k: stmt.excluded[k]
                          for k in batch[0] if k not in ("symbol", "date")}
                )
                session.execute(stmt)
            session.commit()
            inserted_rows += len(records)
            print(f"✅  Chunk {chunk_no} committed ({len(records):,} rows) "
                  f"— total inserted {inserted_rows:,}/{total_rows:,}")
        except Exception as e:
            session.rollback()
            print(f"❌  Chunk {chunk_no} failed: {e}")
        finally:
            session.close()

    print("\n🚀 Finished. "
          f"inserted/updated {inserted_rows:,}/{total_rows:,} rows.")


MIN_DAYS        = 252
MAX_MISS_RATIO  = 0.20  
MAX_ABS_RETURN  = 0.30       
BATCH_DELETE    = 10_000      
ROLLING_WINDOWS = [30, 60, 90, 180, 360] 
MIN_POINTS_WIN  = 20          
VAR_EPS         = 1e-6        
SYMBOL_BATCH    = 250       
BETA_TABLE      = "beta_calculation"  


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def clean_stock_table(min_days: int = MIN_DAYS,
                      max_missing: float = MAX_MISS_RATIO,
                      max_abs_ret: float = MAX_ABS_RETURN) -> Dict[str, int]:
    """
    Permanently remove low‑quality symbols from *daily_stock_data*.

    * **Trading history** - requires *min_days* observations.
    * **Missing‑ratio** - at most *max_missing* fraction of index‑days may be missing.
    * **Data validity** - OHLC>0 and |return| ≤ *max_abs_ret*.

    Returns
    -------
    dict
        Mapping *reason → count* summarising removed symbols.
    """

    reasons: Dict[str, set[str]] = defaultdict(set)

    with session_scope() as s:
        # 1. compare per‑symbol row‑count with index coverage
        stats_sql = text("""
        WITH symbol_stats AS (
            SELECT symbol, COUNT(*) AS n_obs,
                   MIN(date) AS start_d, MAX(date) AS end_d
              FROM daily_stock_data
          GROUP BY symbol
        ), exp_cnt AS (
            SELECT ss.symbol, ss.n_obs,
                   (SELECT COUNT(*)
                      FROM sp500_index idx
                     WHERE idx.date BETWEEN ss.start_d AND ss.end_d) AS n_exp
              FROM symbol_stats ss)
        SELECT symbol,
               CASE
                 WHEN n_obs < :min_days THEN 'too_short'
                 WHEN n_obs < n_exp * (1 - :max_missing) THEN 'too_many_missing'
               END AS fail_reason
          FROM exp_cnt
         WHERE n_obs < :min_days
            OR n_obs < n_exp * (1 - :max_missing);
        """)
        for sym, reason in s.execute(stats_sql, dict(min_days=min_days, max_missing=max_missing)):
            reasons[reason].add(sym)

        # 2. invalid numeric values / missing returns
        bad_val_sql = text("""
        SELECT DISTINCT symbol, 'bad_values' AS fail_reason
          FROM daily_stock_data
         WHERE open  <= 0 OR high <= 0 OR low  <= 0 OR close <= 0
            OR volume IS NULL
            OR return_daily IS NULL
            OR ABS(return_daily) > :max_abs_ret;
        """)
        for sym, reason in s.execute(bad_val_sql, dict(max_abs_ret=max_abs_ret)):
            reasons[reason].add(sym)

        # 3. delete in manageable chunks
        to_drop = set().union(*reasons.values())
        if to_drop:
            print(f"⚠️  {len(to_drop):,} symbols flagged for removal – deleting…")
            syms = list(to_drop)
            for i in range(0, len(syms), BATCH_DELETE):
                s.execute(text("DELETE FROM daily_stock_data WHERE symbol = ANY(:syms)"),
                          dict(syms=syms[i:i+BATCH_DELETE]))

        kept = s.execute(text("SELECT COUNT(DISTINCT symbol) FROM daily_stock_data")).scalar_one()

    # final report
    print("\n🚀 Cleaning complete.")
    for r, st in reasons.items():
        print(f"   · {len(st):>6} symbols removed – {r}")
    print(f"   · {kept:>6} symbols remain in *daily_stock_data*.\n")
    return {k: len(v) for k, v in reasons.items()}

def count_distinct_symbols() -> int:
    """Return the number of unique symbols present in *daily_stock_data*."""
    with session_scope() as s:
        cnt = s.execute(text("SELECT COUNT(DISTINCT symbol) FROM daily_stock_data")).scalar_one()
    print(f"📊 Universe size: {cnt} symbols")
    return cnt


def _beta_series(mkt, stk, mask, win):
    """
    Calculate rolling beta series for a single condition (either up or down market days).

    Parameters:
        mkt (pd.Series): Market returns (benchmark series).
        stk (pd.Series): Stock returns (individual stock series).
        mask (pd.Series): Boolean mask indicating which days to include (e.g., up or down days).
        win (int): Rolling window size (number of days).

    Returns:
        pd.Series: Rolling beta values where variance is sufficiently large.
    """
    min_req = int(win * 0.2)
    m = mkt.where(mask)
    r = stk.where(mask)

    cov  = r.rolling(win, min_periods=min_req).cov(m)
    var  = m.rolling(win, min_periods=min_req).var()
    beta = cov / var
    return beta.where(var > VAR_EPS)

def _fast_betas(df: pd.DataFrame, win: int) -> pd.DataFrame:
    """
    Compute rolling up-beta and down-beta for a given window size (fully vectorized).

    Parameters:
        df (pd.DataFrame): DataFrame containing columns: 'date', 'sp_return', 'return_daily'.
        win (int): Rolling window size (number of days).

    Returns:
        pd.DataFrame: DataFrame with columns: 'date', 'beta_up_<win>', 'beta_down_<win>'.
    """
    mkt = df['sp_return']
    stk = df['return_daily']
    up  = mkt > 0
    dn  = mkt < 0

    return pd.DataFrame({
        'date': df['date'],
        f'beta_up_{win}':  _beta_series(mkt, stk, up, win).to_numpy(),
        f'beta_down_{win}': _beta_series(mkt, stk, dn, win).to_numpy()
    })

def _rolling_betas(df: pd.DataFrame, windows: Iterable[int]) -> Dict[int, pd.DataFrame]:
    """
    Compute rolling up/down betas for multiple window sizes.

    Parameters:
        df (pd.DataFrame): DataFrame containing columns: 'date', 'sp_return', 'return_daily'.
        windows (Iterable[int]): Iterable of rolling window sizes (number of days).

    Returns:
        Dict[int, pd.DataFrame]: Mapping of window size to corresponding beta DataFrame.
    """
    return {w: _fast_betas(df, w) for w in windows}

def _bulk_upsert(session, rows: List[Dict], columns: List[str]):
    """
    Perform a bulk upsert (insert or update) into the BetaCalculation table.

    Parameters:
        session (Session): Active SQLAlchemy session.
        rows (List[Dict]): List of row dictionaries to insert.
        columns (List[str]): List of column names for upsert; primary keys are assumed to be first two columns.
    """
    if not rows:
        return
    insert_stmt = pg_insert(BetaCalculation).values(rows)
    update_cols = {c: insert_stmt.excluded[c] for c in columns[2:]}  # exclude primary keys
    session.execute(
        insert_stmt.on_conflict_do_update(
            index_elements=['symbol', 'date'],
            set_=update_cols
        )
    )
    
def compute_stock_betas(windows: List[int] = ROLLING_WINDOWS,
                         symbol_batch: int = SYMBOL_BATCH) -> None:
    """Calculate rolling up/down betas and persist to *beta_calculation*.

    Parameters
    ----------
    windows : list[int]
        Rolling window lengths (trading‑days).
    symbol_batch : int
        Number of symbols processed per DB round‑trip.
    """

    # column order for both pandas → dict and DB insert
    col_list = ['symbol', 'date'] + [f'beta_up_{w}' for w in windows] + [f'beta_down_{w}' for w in windows]

    # full symbol universe
    with session_scope() as s:
        symbols = [row[0] for row in s.execute(text("SELECT DISTINCT symbol FROM daily_stock_data ORDER BY symbol"))]

    total = len(symbols)
    processed = 0

    for i in range(0, total, symbol_batch):
        batch = symbols[i:i + symbol_batch]
        print(f"⚡ Processing symbols {i + 1:,} – {i + len(batch):,} / {total:,} …")

        # pull once per batch
        with session_scope() as s:
            df = pd.read_sql(text("""
                SELECT symbol, date, return_daily, sp_return
                  FROM daily_stock_data
                 WHERE symbol = ANY(:syms)
              ORDER BY symbol, date"""), s.bind, params={'syms': batch})

        records: List[Dict] = []
        for sym, g in df.groupby('symbol', sort=False):
            g = g.dropna(subset=['return_daily', 'sp_return']).reset_index(drop=True)
            if g.empty:
                continue
            betas = _rolling_betas(g[['date', 'return_daily', 'sp_return']], windows)
            merged = g[['date']].copy()
            for w in windows:
                merged = merged.merge(betas[w], on='date')
            merged.insert(0, 'symbol', sym)
            records.extend(merged[col_list].to_dict('records'))

        # single upsert per batch
        with session_scope() as s:
            _bulk_upsert(s, records, col_list)

        processed += len(batch)
        print(f"✅ Batch finished – {processed:,}/{total:,} symbols done.\n")

    print("🚀 Beta calculation complete.")

if __name__ == "__main__":
    compute_stock_betas() 
    # monitor_beta_progress(poll_interval=60)
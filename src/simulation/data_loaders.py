"""simulation/loaders.py – data access layer

Pull market‑level, stock‑level and beta tables from the cloud database **once**,
serialize them to disk, then serve fast pandas DataFrames to the rest of the
project.

Key points
----------
*  **Lazy cache** – first call hits the DB, afterwards we read from a local
   cache (Parquet if possible, Pickle otherwise).
*  **No external deps required** – if *pyarrow* or *fastparquet* are missing we
   gracefully downgrade to Pickle so the code still works out‑of‑the‑box.
*  **Identical signature** for all loaders:
       >>> df = load_<table>(*, force_reload=False, **filters)

This module exposes exactly three public helpers – import with:
    from simulation.loaders import load_sp500, load_stocks, load_betas
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Optional Parquet support – fall back to Pickle if engines are missing
# ---------------------------------------------------------------------------
try:
    import pyarrow as _pa  # noqa: F401 – presence only
    _PARQUET_ENGINE = "pyarrow"
    _HAS_PARQUET = True
except ModuleNotFoundError:  # pragma: no cover – handled at runtime only
    try:
        import fastparquet as _fp  # noqa: F401
        _PARQUET_ENGINE = "fastparquet"
        _HAS_PARQUET = True
    except ModuleNotFoundError:
        _HAS_PARQUET = False
        _PARQUET_ENGINE = None

# ---------------------------------------------------------------------------
# Project imports (DB session etc.)
# ---------------------------------------------------------------------------
from DBintegration.database import SessionLocal, engine  # noqa: E402 – after sys.path gymnastics upstream

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_CACHE_DIR = Path(__file__).resolve().parent.parent / "data_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Helper utils
# ---------------------------------------------------------------------------

def _cache_path(name: str, ext: Optional[str] = None) -> Path:
    """Return path under data_cache with proper extension."""
    if ext is None:
        ext = "parquet" if _HAS_PARQUET else "pkl"
    return _CACHE_DIR / f"{name}.{ext}"


def _save_to_cache(df: pd.DataFrame, name: str) -> None:
    if _HAS_PARQUET:
        p = _cache_path(name, "parquet")
        df.to_parquet(p, index=False, engine=_PARQUET_ENGINE)
    else:
        p = _cache_path(name, "pkl")
        df.to_pickle(p)
    logger.info("✅ cached %s rows → %s", len(df), p.name)


def _load_from_cache(name: str) -> Optional[pd.DataFrame]:
    p_parquet = _cache_path(name, "parquet")
    p_pickle = _cache_path(name, "pkl")
    if _HAS_PARQUET and p_parquet.exists():
        logger.info("📄 loading %s", p_parquet.name)
        return pd.read_parquet(p_parquet, engine=_PARQUET_ENGINE)
    if p_pickle.exists():
        logger.info("📄 loading %s", p_pickle.name)
        return pd.read_pickle(p_pickle)
    return None

# ---------------------------------------------------------------------------
# Public loader functions
# ---------------------------------------------------------------------------

def load_sp500(*, force_reload: bool = False) -> pd.DataFrame:
    """Return full `sp500_index` table as DataFrame."""
    name = "sp500_index"
    if not force_reload and (df := _load_from_cache(name)) is not None:
        return df

    with engine.connect() as conn:
        sql = text("SELECT * FROM sp500_index ORDER BY date")
        df = pd.read_sql(sql, conn).astype({"date": "datetime64[ns]"})

    _save_to_cache(df, name)
    return df


def load_stocks(
    *,
    tickers: Optional[Iterable[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    force_reload: bool = False,
) -> pd.DataFrame:
    """Load daily_stock_data (optionally filtered) as DataFrame."""
    name = "daily_stock_data"
    cache_key = name
    if tickers is not None:
        cache_key += "_" + str(abs(hash(tuple(sorted(set(tickers))))))
    if start or end:
        cache_key += f"_{start or ''}_{end or ''}"

    if not force_reload and (df := _load_from_cache(cache_key)) is not None:
        return df

    clauses: List[str] = []
    params: dict = {}
    if tickers is not None:
        clauses.append("symbol = ANY(:tickers)")
        params["tickers"] = list(tickers)
    if start:
        clauses.append("date >= :start")
        params["start"] = start
    if end:
        clauses.append("date <= :end")
        params["end"] = end

    where_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
    sql = text(
        f"""
        SELECT *
        FROM daily_stock_data
        {where_clause}
        ORDER BY date, symbol
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    df["date"] = pd.to_datetime(df["date"])

    _save_to_cache(df, cache_key)
    return df


def load_betas(*, force_reload: bool = False) -> pd.DataFrame:
    """Return full beta_calculation table."""
    name = "beta_calculation"
    if not force_reload and (df := _load_from_cache(name)) is not None:
        return df

    with engine.connect() as conn:
        sql = text("SELECT * FROM beta_calculation ORDER BY date, symbol")
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])

    _save_to_cache(df, name)
    return df

# ---------------------------------------------------------------------------
# CLI helper for quick manual testing
# ---------------------------------------------------------------------------

def _main():  # pragma: no cover – manual smoke‑test only
    import argparse, sys  # noqa: E402

    parser = argparse.ArgumentParser(description="Quick loaders test")
    parser.add_argument("table", choices=["sp", "stocks", "betas"], help="Table to load")
    parser.add_argument("--force", action="store_true", help="Ignore cache")
    parser.add_argument("--ticker", action="append", help="Filter by ticker(s)")
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    if args.table == "sp":
        df = load_sp500(force_reload=args.force)
    elif args.table == "stocks":
        df = load_stocks(
            tickers=args.ticker,
            start=args.start,
            end=args.end,
            force_reload=args.force,
        )
    else:
        df = load_betas(force_reload=args.force)

    print(df.head())
    print("Rows:", len(df))
    sys.exit(0)


if __name__ == "__main__":
    _main()


__all__ = [
    "load_sp500",
    "load_stocks",
    "load_betas",
]

import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DBintegration.db_utils import model_to_dataframe
from DBintegration.db_utils import update_data
from DBintegration.models import DailyStockData
from sqlalchemy.orm import sessionmaker
from DBintegration.models import Base
from pathlib import Path
import pandas as pd
"""functions List:
count_symbols(df) -----> count unique symbols.
"""
def count_symbols(dataframe: pd.DataFrame) -> int:
    """
    Counts the number of unique symbols in a pandas DataFrame.

    Args:
        dataframe (pd.DataFrame): The input DataFrame, expected to have a 'symbol' column.

    Returns:
        int: The number of unique symbols.
    """
    if 'symbol' in dataframe.columns:
        return dataframe['symbol'].nunique()
    else:
        print("Warning: 'symbol' column not found in the DataFrame.")
        return 0

# Example usage of the function with your 'df'

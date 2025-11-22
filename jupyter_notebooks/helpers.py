import importlib
from typing import Optional

import pandas as pd

import utils
importlib.reload(utils)
from utils import logger


def create_df(
        name: str,
        source: pd.DataFrame,
        columns: list,
        id_column: Optional[str] = None,
        sort_column: Optional[str] = None,
) -> pd.DataFrame | None:
    """Creates a new dataframe using data from another dataframe's column(s).

    Args:
        name: Name of the new dataframe.
        id_column (optional): Name of the column to use as ID column.
        sort_column (optional): Name of the column to use for sorting.
        source: Source dataframe for data.
        columns: List of column names.

    Returns:
        A new pandas DataFrame.
    """
    logger.info(f'Creating {name}...')

    try:
        # Create dataframe from source.
        df = pd.DataFrame(source[columns])

        # Convert strings to lowercase and trim whitespace.
        df.map(lambda x: x.lower().strip() if isinstance(x, str) else x)
        df.columns = df.columns.str.lower().str.strip()

        if id_column:
            # Create ID column.
            df[f'{id_column}_id'] = df.index + 1

            # Make sure ID column is Int64.
            df[f'{id_column}_id'] = df[f'{id_column}_id'].astype('Int64')

        if sort_column:
            # Drop dupes.
            df = df.drop_duplicates(subset=sort_column, keep='first')

            # Sort by sort column.
            df = df.sort_values(by=sort_column).reset_index(drop=True)

        logger.info(f'{name} created.')

        return df
    except Exception as exception:
        logger.error(f'An unexpected error occurred: {exception}')
        return None
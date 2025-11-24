from datetime import datetime
from difflib import SequenceMatcher
import importlib
from typing import Optional

import pandas as pd

import utils
importlib.reload(utils)
from utils import logger


def calculate_similarity(word: str, target: str) -> float:
    """
    Calculate similarity ratio (float between 0 and 1: higher score = better match) between two strings.
    :param word: The string to be compared.
    :param target: The string it should be.
    :return: The similarity ratio score.
    """
    return SequenceMatcher(None, word.lower(), target.lower()).ratio()


def create_df(
        name: str,
        source: pd.DataFrame,
        columns: list,
        subset: Optional[str] = None,
        sort_column: Optional[str] = None,
        id_column: Optional[str] = None,
        date_column: Optional[str] = None
) -> pd.DataFrame | None:
    """
    Creates a new dataframe using data from another dataframe's column(s).
    :param name: Name of the new dataframe.
    :param source: Source dataframe for data.
    :param columns: List of column names.
    :param subset: (Optional) Name of the column to use for subsetting when dropping NaN values.
    :param sort_column: (Optional) Name of the column to use for sorting.
    :param id_column: (Optional) Name of the column to use as ID column.
    :param date_column: (Optional) Name of the column containing a date.
    :return: A new pandas DataFrame.
    """
    logger.info(f'Creating {name}...')

    try:
        # Create dataframe from source.
        df = pd.DataFrame(source[columns])

        # Convert strings to lowercase and trim whitespace.
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].str.strip().str.lower()

        # Drop dupes.
        og_length = len(df)
        df = df.drop_duplicates().reset_index(drop=True)
        logger.info(f'{og_length - len(df)} duplicate rows dropped.')

        # Drop rows with NaN or null values if dropna == True.
        df_na = len(df)
        if subset:
            df = df.dropna(subset=subset)
            logger.info(f'{df_na - len(df)} rows where {subset} column contains NaN or null values dropped.')
        else:
            df = df.dropna()
            logger.info(f'{df_na - len(df)} rows with NaN or null values dropped.')

        if sort_column:
            # Sort by sort column.
            df = df.sort_values(by=sort_column).reset_index(drop=True)

        if id_column:
            # Create ID column.
            df[f'{id_column}_id'] = df.index + 1

            # Make sure ID column is Int64.
            df[f'{id_column}_id'] = df[f'{id_column}_id'].astype('Int64')

        if date_column:
            drop_future_dates(df, date_column)

        logger.info(f'{name} dataframe with {df.shape[0]} rows and {df.shape[1]} columns created.')

        return df
    except Exception as exception:
        logger.error(f'An unexpected error occurred: {exception}')
        return None


def drop_future_dates(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Drop future dates from a dataframe.
    :param df: dataframe with future dates to be dropped.
    :param column: name of the column with future dates to be dropped.
    :return: Updated dataframe with no future dates.
    """
    logger.info('Dropping future dates...')

    # Convert column to datetime.
    df[column] = pd.to_datetime(df[column], errors='coerce')

    # Remove future dates (keep only dates <= today).
    df_before = len(df)
    df = df[df[column].copy().values <= pd.Timestamp.now()]

    logger.info(f'{df_before - len(df)} future dates dropped.')

    return df


def map_id_column(source_df: pd.DataFrame,
                  source_name_col: str,
                  source_id_col: str,
                  target_df: pd.DataFrame,
                  target_name_col: str
) -> pd.DataFrame | None:
    """
    Maps values to corresponding IDs and drops the original name column.
    :param source_df: DataFrame containing the mapping from value to ID.
    :param source_name_col: Name of the column in `source_df` with the values to map.
    :param source_id_col: Name of the ID column in `source_df` to map to.
    :param target_df: DataFrame to apply the mapping to.
    :param target_name_col: Name of the column in `target_df` containing the names to be mapped.

    Returns:
        The target DataFrame with the new ID column added and the original name column dropped.
    """
    try:
        logger.info(f'Mapping {source_id_col}...')
        source_df = source_df.drop_duplicates(subset=[source_name_col])
        mapping = source_df.set_index(source_name_col)[source_id_col]
        new_id_col_name = source_id_col  # Use the actual ID column name
        target_df[new_id_col_name] = target_df[target_name_col].map(mapping)
        target_df = target_df.drop(columns=[target_name_col])
        logger.info('Column mapping completed.')
        return target_df
    except Exception as exception:
        logger.error(f'An unexpected error occurred: {exception}')
        return None


def match_string(word: str, targets: list) -> str:
    """
    Match a string with possible typos to a string in a target list based on similarity ratio score.
    :param word: The string to be compared.
    :param targets: The target strings to compare to.
    :return: The best possible match.
    """

    # Classify as 'not specified' if value is null.
    if pd.isna(word):
        return 'n/a'

    # Make sure it's a string.
    word = str(word)

    # Calculate similarity scores for each target.
    scores = {target: calculate_similarity(word, target) for target in targets}

    # Return the target with most similarity.
    target_match = max(scores, key=scores.get)

    return target_match


def validate_ticker_format(df: pd.DataFrame) -> pd.Series:
    """
    Validate ticker_symbol column values against pattern: 3 letters + 3 digits (i.e. stk182).
    :param df: The dataframe with ticker column to be validated.
    :return: Boolean.
    """
    pattern = r'^[a-z]{3}\d{3}$'

    # Check pattern match (handle NaN)
    is_valid = df['ticker_symbol'].str.match(pattern, na=False)

    # Summary
    total = len(df)
    valid = is_valid.sum()
    invalid = (~is_valid).sum()
    null = df['ticker_symbol'].isna().sum()

    print(f"Total rows: {total}")
    print(f"Valid format: {valid} ({valid/total*100:.1f}%)")
    print(f"Invalid format: {invalid} ({invalid/total*100:.1f}%)")
    print(f"Null values: {null}")

    # Show invalid values
    if invalid > 0:
        invalid_values = df.loc[~is_valid, 'ticker_symbol'].unique()
        print(f"\nInvalid ticker symbols (unique values): {len(invalid_values)}")
        for val in invalid_values:
            print(f" - {val}")

    return is_valid
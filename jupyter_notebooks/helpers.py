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
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].str.strip().str.lower()

        # Drop dupes.
        og_length = len(df)
        df = df.drop_duplicates().reset_index(drop=True)
        logger.info(f'{og_length - len(df)} duplicate rows dropped.')

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

        logger.info(f'{name} dataframe with {df.shape[0]} rows and {df.shape[1]} columns created.')

        return df
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


def validate_ticker_format(df):
    """
    Validate ticker_symbol column values against pattern: 3 letters + 3 digits
    Returns summary and invalid values
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
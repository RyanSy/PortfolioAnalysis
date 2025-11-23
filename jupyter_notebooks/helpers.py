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
        df[df.select_dtypes(include="object").columns] = (
            df.select_dtypes(include="object").apply(lambda col: col.str.strip().str.lower())
        )

        # Drop dupes and rows with nan/null values and reset index.
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

        logger.info(f'{name} created.')

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
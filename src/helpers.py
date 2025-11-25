from difflib import SequenceMatcher
import importlib
from typing import List, Optional

import pandas as pd

import utils
importlib.reload(utils)
from utils import logger

def arrange_and_convert_columns(df: pd.DataFrame, columns: list, dtype_map: dict, df_name: str) -> pd.DataFrame:
    """
    Reorder columns and convert them to specified dtypes.

    Args:
        df: Dataframe to prepare.
        columns: Columns in the proper order for database insertion.
        dtype_map: Mapping from column names to dtypes.
        df_name: Name of the dataframe to arrange (used for logging).

    Returns:
        Dataframe with reordered columns and converted dtypes.
    """
    # Order columns according to schema.
    df = df[columns]

    # Convert to appropriate dtypes.
    df = df.astype(dtype_map)

    logger.info(f'Columns in {df_name} arranged and data converted to appropriate dtypes.')

    return df


def calculate_similarity(word: str, target: str) -> float:
    """
    Calculate similarity ratio between two strings.

    The similarity score is a float between 0 and 1, where a higher score
    indicates a better match. Comparison is case-insensitive.

    Args:
        word: The string to be compared.
        target: The string it should be compared against.

    Returns:
        The similarity ratio score.
    """
    return SequenceMatcher(None, word.lower(), target.lower()).ratio()


def clean_tickers(df: pd.DataFrame,
                  column: str = "ticker_symbol",
                  min_val: int = 1,
                  max_val: int = 500) -> pd.DataFrame:
    """
    Cleans a DataFrame by dropping rows with invalid stock tickers.

    Valid tickers must match the format 'stkNNN' (1–3 digits) and fall
    within the range [min_val, max_val]. Valid tickers are normalized
    to the 'stkNNN' format (zero-padded).

    Args:
        df: DataFrame containing a ticker column to clean.
        column: Name of the column containing ticker strings.
            Defaults to "ticker_symbol".
        min_val: Minimum valid numeric value for the ticker. Defaults to 1.
        max_val: Maximum valid numeric value for the ticker. Defaults to 500.

    Returns:
        A cleaned DataFrame containing only valid tickers, normalized to 'stkNNN'.
    """
    # Normalize case and whitespace.
    df[column] = df[column].str.strip().str.lower()

    # Strict regex mask: only 'stk' followed by 1–3 digits.
    regex_mask = df[column].str.match(r'^stk\d{1,3}$')

    # Extract digits for valid rows.
    extracted = df[column].str.extract(r'^stk(\d{1,3})$', expand=False)
    numeric = pd.to_numeric(extracted, errors='coerce')

    # Apply numeric range filter.
    range_mask = (numeric >= min_val) & (numeric <= max_val)

    # Final mask.
    valid_mask = regex_mask & range_mask

    # Keep only valid rows.
    valid_df = df.loc[valid_mask].copy()

    # Normalize ticker symbols to 'stkNNN'.
    valid_df[column] = 'stk' + numeric[valid_mask].astype(int).astype(str).str.zfill(3)

    # Number of rows dropped.
    rows_dropped = len(df.loc[~valid_mask].copy())

    logger.info(
        f'{rows_dropped} rows with invalid ticker symbols dropped.'
    )
    return valid_df


def create_df(
        name: str,
        source: pd.DataFrame,
        columns: list,
        subset: Optional[List[str]] = None,
        sort_column: Optional[str] = None,
        id_column: Optional[str] = None,
        date_column: Optional[str] = None
) -> pd.DataFrame | None:
    """
    Creates a new DataFrame from column(s) of a source DataFrame, applying
    cleaning steps and optional ID creation.

    Steps include:
    1. Lowercasing and trimming whitespace of object columns.
    2. Dropping duplicate rows.
    3. Dropping rows with NaN/null values.
    4. Optional sorting.
    5. Optional ID column creation (index + 1).
    6. Optional future date dropping.

    Args:
        name: Name of the new dataframe (used for logging).
        source: Source DataFrame for data.
        columns: List of column names to include in the new DataFrame.
        subset: Optional list of column names to use for subsetting when
            dropping NaN values. If None, drops row if any column has NaN.
        sort_column: Optional name of the column to use for sorting.
        id_column: Optional name of the column used to create an ID column
            (e.g., 'ticker' creates 'ticker_id').
        date_column: Optional name of the column containing a date to check for future dates.

    Returns:
        A new pandas DataFrame or None if an error occurs.
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
    Drop rows from a DataFrame where the specified date column contains a future date.

    The column is first converted to datetime objects.

    Args:
        df: DataFrame with future dates to be dropped.
        column: Name of the column with dates.

    Returns:
        Updated DataFrame with no future dates.
    """
    logger.info('Dropping future dates...')

    # Convert column to datetime.
    df[column] = pd.to_datetime(df[column], errors='coerce')

    # Remove future dates (keep only dates <= today).
    df_before = len(df)
    df = df[df[column].copy().values <= pd.Timestamp.now()]

    logger.info(f'{df_before - len(df)} future dates dropped.')

    return df


def filter_account_ids(
    df: pd.DataFrame,
    df2: pd.DataFrame,
    column: str = "account_id"
) -> pd.DataFrame:
    """
    Filter rows in a DataFrame based on membership in another DataFrame.

    This function keeps only rows in `df` where the specified column's
    values are present in the same column of `df2` (the reference DataFrame).

    Args:
        df: Input DataFrame containing data to be filtered.
        df2: Reference DataFrame containing valid values for the filter column.
        column: Column name to apply the filter on. Defaults to 'account_id'.

    Returns:
        A new DataFrame with rows retained only if the column value
        exists in `df2[column]`.

    Raises:
        ValueError: If the specified column does not exist in either DataFrame.
    """
    if column not in df.columns:
        raise ValueError(f'Column "{column}" not found in dataframe to be filtered.')
    if column not in df2.columns:
        raise ValueError(f'Column "{column}" not found in reference dataframe.')

    valid_ids = set(df2[column].unique())
    filtered_df = df[df[column].isin(valid_ids)].copy()

    logger.info(f'Rows where {column} are not in reference dataframe dropped.')

    return filtered_df


def map_id_column(source_df: pd.DataFrame,
                  source_column: str,
                  source_id_column: str,
                  target_df: pd.DataFrame,
                  target_column: str
) -> pd.DataFrame | None:
    """
    Maps values in a target DataFrame column to corresponding IDs from a source DataFrame,
    and then drops the original value column from the target DataFrame.

    Args:
        source_df: DataFrame containing the mapping from value to ID.
        source_column: Column in `source_df` with the values to map (keys).
        source_id_column: ID column in `source_df` to map to (values).
        target_df: DataFrame to apply the mapping to.
        target_column: Column in `target_df` containing the values to be mapped.

    Returns:
        The target DataFrame with the new ID column added and the original
        value column dropped, or None if an error occurs.
    """
    try:
        logger.info(f'Mapping {source_id_column}...')
        source_df = source_df.drop_duplicates(subset=[source_column])
        mapping = source_df.set_index(source_column)[source_id_column]
        new_id_col_name = source_id_column  # Use the actual ID column name.
        target_df[new_id_col_name] = target_df[target_column].map(mapping)
        target_df = target_df.drop(columns=[target_column])
        logger.info('Column mapping completed.')
        return target_df
    except Exception as exception:
        logger.error(f'An unexpected error occurred: {exception}')
        return None


def match_string(word: str, targets: list) -> str:
    """
    Matches a string (potentially with typos) to the closest string in a target list.

    Matching is based on the highest similarity ratio score. If the input
    string is null (NaN), it returns 'n/a'.

    Args:
        word: The string to be compared.
        targets: The list of target strings to compare against.

    Returns:
        The target string with the best possible match, or 'n/a'.
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
    Validates values in the 'ticker_symbol' column against the pattern:
    three lowercase letters followed by three digits (e.g., 'stk182').

    Also prints a summary of valid, invalid, and null counts, and lists
    unique invalid values.

    Args:
        df: The DataFrame with the 'ticker_symbol' column to be validated.

    Returns:
        A boolean pandas Series indicating whether each row's 'ticker_symbol'
        matches the required format.
    """
    pattern = r'^[a-z]{3}\d{3}$'

    # Check pattern match (handle NaN).
    is_valid = df['ticker_symbol'].str.match(pattern, na=False)

    # Summary.
    total = len(df)
    valid = is_valid.sum()
    invalid = (~is_valid).sum()
    null = df['ticker_symbol'].isna().sum()

    print(f"Total rows: {total}")
    print(f"Valid format: {valid} ({valid/total*100:.1f}%)")
    print(f"Invalid format: {invalid} ({invalid/total*100:.1f}%)")
    print(f"Null values: {null}")

    # Show invalid values.
    if invalid > 0:
        invalid_values = df.loc[~is_valid, 'ticker_symbol'].unique()
        print(f"\nInvalid ticker symbols (unique values): {len(invalid_values)}")
        for val in invalid_values:
            print(f" - {val}")

    return is_valid
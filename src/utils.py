import logging
import os

import sys
from io import StringIO

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError

load_dotenv()


logging.basicConfig(
    level=logging.INFO,  # Set the minimum level of messages to log (e.g., INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    datefmt='%Y-%m-%d %H:%M:%S',  # Define the timestamp format
    stream=sys.stdout  # Direct log output to the console (can also be a file)
)
logger = logging.getLogger(__name__)

DB_DIALECT = os.getenv('DB_DIALECT')
DB_DRIVER = os.getenv('DB_DRIVER')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_URL = f'{DB_DIALECT}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DB_URL)


def insert_table_data(source: pd.DataFrame, schema: str, table: str) -> None:
    """
    Inserts DataFrame data into a PostgreSQL table using `COPY FROM STDIN`.

    This method is used for efficient bulk insertion.

    Args:
        source: DataFrame containing the data to insert.
        schema: Database schema name.
        table: Table name to insert the data into.
    """
    logger.info(f'Opening raw db connection, inserting data into {table}...')

    raw_conn = None
    cursor = None

    try:
        raw_conn = engine.raw_connection()
        cursor = raw_conn.cursor()
        copy_sql = f'COPY {schema}.{table} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
        output = StringIO()
        source.to_csv(output, sep=',', header=False, index=False)
        output.seek(0)
        cursor.copy_expert(sql=copy_sql, file=output)
        raw_conn.commit()
        logger.info(f'Dataframe successfully copied to the {table} table.')
    except IntegrityError as integrity_error:
        logger.error(f'Integrity constraint violation: {integrity_error.orig}')
        if raw_conn:
            raw_conn.rollback()
    except OperationalError as operational_error:
        logger.error(f'Database operational error: {operational_error.orig}')
        if raw_conn:
            raw_conn.rollback()
    except DBAPIError as dbapi_error:
        logger.error(f'A general database API error occurred: {dbapi_error.orig}')
        if raw_conn:
            raw_conn.rollback()
    except Exception as exception:
        logger.error(f'An unexpected error occurred: {exception}')
        if raw_conn:
            raw_conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if raw_conn:
            raw_conn.close()


def run_sql(log_msg :str, sql_query: str, data=None) -> None:
    """
    Executes a SQL query within a transaction (using engine.begin()).

    Args:
        log_msg: A descriptive message to log before execution.
        sql_query: The SQL query string to execute.
        data: Optional data to pass to the SQL query (e.g., for
            parameterized queries).
    """
    logger.info(f'{log_msg}')
    try:
        with engine.begin() as run_sql_conn:
            run_sql_conn.execute(text(sql_query), data)
            logger.info(f'SQL query executed successfully.')
    except IntegrityError as integrity_error:
        # Handle unique key violations, foreign key errors, etc.
        logger.error(f'Integrity constraint violation: {integrity_error.orig}')
    except OperationalError as operational_error:
        # Handle connection errors, database down, etc.
        logger.error(f'Database operational error: {operational_error.orig}')
    except DBAPIError as dbapi_error:
        # Catch any other DBAPI-related errors
        logger.error(f'A general database API error occurred: {dbapi_error.orig}')
    except Exception as exception:
        # Catch any other unexpected errors
        logger.error(f'An unexpected error occurred: {exception}')
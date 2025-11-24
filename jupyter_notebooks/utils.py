import logging
import os
import sys

from dotenv import load_dotenv
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

engine = create_engine(os.getenv('DATABASE_URL'))


def run_sql(log_msg :str, sql_query: str, data=None) -> None:
    """
    Executes a SQL query.
    :param log_msg: A message to log.
    :param sql_query: The SQL query to execute.
    :param data: Optional data to pass to the SQL query (e.g., for parameterized queries).
    :return: None.
    """
    logger.info(f'{log_msg}')
    try:
        with engine.begin() as run_sql_conn:
            run_sql_conn.execute(text(sql_query), data)
            logger.info(f'SQL query executed successfully.')
            return None
    except IntegrityError as integrity_error:
        # Handle unique key violations, foreign key errors, etc.
        logger.error(f'Integrity constraint violation: {integrity_error.orig}')
        return None
    except OperationalError as operational_error:
        # Handle connection errors, database down, etc.
        logger.error(f'Database operational error: {operational_error.orig}')
        return None
    except DBAPIError as dbapi_error:
        # Catch any other DBAPI-related errors
        logger.error(f'A general database API error occurred: {dbapi_error.orig}')
        return None
    except Exception as exception:
        # Catch any other unexpected errors
        logger.error(f'An unexpected error occurred: {exception}')
        return None


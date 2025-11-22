import logging
import sys


logging.basicConfig(
    level=logging.INFO,  # Set the minimum level of messages to log (e.g., INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    datefmt='%Y-%m-%d %H:%M:%S',  # Define the timestamp format
    stream=sys.stdout  # Direct log output to the console (can also be a file)
)
logger = logging.getLogger(__name__)


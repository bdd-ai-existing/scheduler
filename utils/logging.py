import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os

def setup_task_logger(task_name, log_dir="logs"):
    """Set up a logger for a specific task with date-based log files."""
    # Ensure the log directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Generate log file name based on task and date
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{task_name}_{date_str}.log")

    # Create a new logger for the task
    logger = logging.getLogger(task_name)
    logger.setLevel(logging.DEBUG)

    # File handler for rotating logs
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3
    )
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Avoid adding duplicate handlers
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

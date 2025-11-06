"""
Centralized Configuration
=========================

Single source of truth for all database and application settings.

Location: src/utils/config.py
Author: Abhiiram
Date: November 6, 2025
"""

import os
from typing import Dict, Any
from pathlib import Path

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_RAW_DIR = PROJECT_ROOT / 'data' / 'raw'
DATA_PROCESSED_DIR = PROJECT_ROOT / 'data' / 'processed'
LOGS_DIR = PROJECT_ROOT / 'logs'

# ========================================
# ENVIRONMENT VARIABLES WITH DEFAULTS
# ========================================


class Config:
    """Centralized configuration class."""

    # ========== DATABASE CONFIGURATION ==========

    # Primary E-commerce Database (from .env)
    ECOMMERCE_DB_HOST = os.getenv('ECOMMERCE_DB_HOST', 'ecommerce-postgres')
    ECOMMERCE_DB_PORT = int(os.getenv('ECOMMERCE_DB_PORT', 5432))
    ECOMMERCE_DB_NAME = os.getenv('ECOMMERCE_DB_NAME', 'ecommerce_db')
    ECOMMERCE_DB_USER = os.getenv('ECOMMERCE_DB_USER', 'dataeng')
    ECOMMERCE_DB_PASSWORD = os.getenv('ECOMMERCE_DB_PASSWORD', 'pipeline123')
    ECOMMERCE_DB_TIMEOUT = int(os.getenv('ECOMMERCE_DB_TIMEOUT', 5))

    # ========== DATA PATHS ==========

    DATA_RAW_DIR = str(DATA_RAW_DIR)
    DATA_PROCESSED_DIR = str(DATA_PROCESSED_DIR)
    LOGS_DIR = str(LOGS_DIR)
    PROJECT_ROOT = str(PROJECT_ROOT)

    # ========== DATA GENERATION PARAMETERS ==========

    NUM_CUSTOMERS = int(os.getenv('NUM_CUSTOMERS', 10000))
    NUM_PRODUCTS = int(os.getenv('NUM_PRODUCTS', 500))
    NUM_ORDERS = int(os.getenv('NUM_ORDERS', 50000))

    # Random seed for reproducibility
    RANDOM_SEED = int(os.getenv('RANDOM_SEED', 42))

    # ========== LOGGING CONFIGURATION ==========

    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE_PREFIX = os.getenv('LOG_FILE_PREFIX', 'app')

    # ========== QUALITY CHECK PARAMETERS ==========

    QUALITY_CHECK_SEVERITY = os.getenv('QUALITY_CHECK_SEVERITY', 'ERROR')
    ANOMALY_ZSCORE_THRESHOLD = float(
        os.getenv('ANOMALY_ZSCORE_THRESHOLD', 3.0))

    # ========== APPLICATION SETTINGS ==========

    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    RETRY_DELAY_SECONDS = int(os.getenv('RETRY_DELAY_SECONDS', 5))

    @classmethod
    def get_db_config(cls) -> Dict[str, Any]:
        """Return database configuration dictionary."""
        return {
            'host': cls.ECOMMERCE_DB_HOST,
            'port': cls.ECOMMERCE_DB_PORT,
            'database': cls.ECOMMERCE_DB_NAME,
            'user': cls.ECOMMERCE_DB_USER,
            'password': cls.ECOMMERCE_DB_PASSWORD,
            'connect_timeout': cls.ECOMMERCE_DB_TIMEOUT,
        }

    @classmethod
    def validate_config(cls) -> bool:
        """Validate critical configuration values."""
        required_fields = [
            cls.ECOMMERCE_DB_HOST,
            cls.ECOMMERCE_DB_NAME,
            cls.ECOMMERCE_DB_USER,
        ]
        return all(required_fields)

    @classmethod
    def create_directories(cls) -> None:
        """Create necessary directories if they don't exist."""
        for directory in [cls.DATA_RAW_DIR, cls.DATA_PROCESSED_DIR, cls.LOGS_DIR]:
            os.makedirs(directory, exist_ok=True)


def get_db_config() -> Dict[str, Any]:
    """Get database configuration."""
    return Config.get_db_config()


if __name__ == "__main__":
    if Config.validate_config():
        print("✓ Configuration valid")
        Config.create_directories()
        print(f"✓ Directories created at: {Config.PROJECT_ROOT}")
    else:
        print("✗ Configuration validation failed")

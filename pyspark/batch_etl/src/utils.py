"""
Shared Helper function used across the package
- load_config() : reads config/config.yaml into a python dict
- get_spark_session(): Creates (or reuses) a SparkSession
- get_logger() : sets up a logger that writes to both console and a log file
"""

import logging
import os
import sys
from pathlib import Path
import yaml
from pyspark.sql import SparkSession

# PROJECT_ROOT points to the bactc_etl folder regardless of where the script is run from. 
# This makes files path in the config .yaml reliable
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config(config_path: str = 'config/config.yaml') -> dict:
    full_path = PROJECT_ROOT/config_path
    with open(full_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def get_spark_session(config: dict) -> SparkSession:
    spark_conf = config['spark']
    spark = (
        SparkSession.builder.master(spark_conf['master'])
        .appName(config['app_name'])
        .config('spark.sql.shuffle.partitions', spark_conf['shuffle_partitions'])
        .config('spark.ui.showConsoleProgress', 'false')
        .getOrCreate()

    )
    spark.sparkContext.setLogLevel('ERROR')
    return spark

def get_logger(config: dict, name: str ='etl_pipeline') -> logging.Logger:
    log_cfg = config['logging']
    log_dir = PROJECT_ROOT/log_cfg['log_dir']
    log_dir.mkdir(parents =True, exist_ok =True)
    log_file = log_cfg['log_file']

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s| %(levelname)s| %(name)s| %(message)s'
        )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger   

def resolve_path(relative_path : str ) -> str:
    return str(PROJECT_ROOT/relative_path)
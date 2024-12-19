# src/utils.py
import logging
import os
import pandas as pd
import json
from pathlib import Path

# Cache to store loaded tables
TABLE_CACHE = {}
SCRIPT_DIR = Path(__file__).resolve().parent

def load_config():
    json_path = SCRIPT_DIR / '../config/config.json'
    with open(json_path, 'r') as file:
        config = json.load(file)
    print("Loaded configuration from config.json")
    
    return config

config = load_config()

# FIXME: change the input arg to config
def setup_logging(log_file: str = "logs/test.log"):
    """
    Sets up logging for the ETL pipeline.

    Args:
        log_file (str): Path to the log file. Default is "logs/pipeline.log".
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging initialized.")

# -----------
#     I/O 
# -----------

def load_mimic_table(
    module: {"icu", "hosp"}, table, file_type: {"csv", "parquet", "pq"} = None,
    mimic_version = "3.1"
):
    """
    Loads a MIMIC-IV table from the specified module and caches it to avoid reloading.

    Args:
        module (str): The MIMIC module (e.g., "icu", "hosp").
        table (str): The table name (e.g., "patients", "admissions").
        file_type (str): The file type to load (e.g., "csv", "parquet").
        mimic_version (str): Version of MIMIC-IV to use (default is "3.1").

    Returns:
        pd.DataFrame: The loaded table as a Pandas DataFrame.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If an unsupported file type is provided.
    """
    global TABLE_CACHE
    cache_key = f"{module}_{table}"

    # Check if table is already in cache
    if cache_key in TABLE_CACHE:
        logging.info(f"Using cached data for {table} from module {module}.")
        return TABLE_CACHE[cache_key]

    # Define file paths
    parquet_path = SCRIPT_DIR / f"../data/mimic-data/mimic-iv-{mimic_version}/{module}/{table}.parquet"
    csv_path = SCRIPT_DIR / f"../data/mimic-data/mimic-iv-{mimic_version}/{module}/{table}.csv.gz"

    if file_type is not None:
        if file_type in ["pq", "parquet"]:
            if os.path.exists(parquet_path):
                logging.info(f"Loading {table} from {parquet_path}.")
                table_df = pd.read_parquet(parquet_path)
            else:
                raise FileNotFoundError(f"Parquet file not found at {parquet_path}")
        elif file_type == "csv":
            if os.path.exists(csv_path):
                logging.info(f"Loading {table} from {csv_path}.")
                table_df = pd.read_csv(csv_path, low_memory=False)
            else:
                raise FileNotFoundError(f"CSV file not found at {csv_path}")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    else:
        if os.path.exists(parquet_path):
            logging.info(f"Reading already converted parquet file for {module}/{table}")
            table_df = pd.read_parquet(parquet_path, engine="auto")
        else:
            logging.info(f"No parquet file exists yet for {module}/{table}, so reading from csv")
            table_df = pd.read_csv(csv_path, low_memory=False)
            table_df.to_parquet(parquet_path)

    # Cache the table
    TABLE_CACHE[cache_key] = table_df
    return table_df

CLIF_DATA_DIR_NAME = config["clif_data_dir_name"]
CLIF_VERSION = config["clif_version"]

def save_to_rclif(df: pd.DataFrame, table_name: str):
    global CLIF_DATA_DIR_NAME
    if not CLIF_DATA_DIR_NAME:
        # if it is an empty str (not specified by user), use the default syntax 
        CLIF_DATA_DIR_NAME = f"rclif-{CLIF_VERSION}"
    output_path = SCRIPT_DIR / f'../data/{CLIF_DATA_DIR_NAME}/clif_{table_name}.parquet'
    # e.g. '../data/rclif-2.0/clif_adt.parquet'
    return df.to_parquet(output_path, index = False)
        
def read_from_rclif(table_name, file_format = "pq"):
    # FIXME: update this when validation / testing
    if file_format in ["pq", "parquet"]:
        return pd.read_parquet(SCRIPT_DIR / f'../rclif/clif_{table_name}.parquet')

def load_mapping_csv(csv_name: str, dtype = None):
    return pd.read_csv(
        SCRIPT_DIR / f"../data/mappings/mimic-to-clif-mappings - {csv_name}.csv", dtype = dtype
        )

def construct_mapper_dict(
    mapping_df: pd.DataFrame, key_col: str, value_col: str, map_none_to_none = False,
    excluded_item_ids: list = None
    ):
    '''
    covert to a dict for df col renaming later
    '''
    if not excluded_item_ids:
        excluded_item_ids = []
    
    if "itemid" in mapping_df.columns:
        mapping_df = mapping_df.loc[
            ~mapping_df["itemid"].isin(excluded_item_ids)
            , 
            ]
    
    mapper_dict = dict(zip(mapping_df[key_col], mapping_df[value_col]))
    
    # Replace "NO MAPPING" with NA
    for key, value in mapper_dict.items():
        if value == "NO MAPPING":
            mapper_dict[key] = None
    
    # to enable a None -> None mapping
    if map_none_to_none:
        mapper_dict[None] = None
        
    return mapper_dict


# -------
#   ETL
# -------
def rename_and_reorder_cols(df: pd.DataFrame, rename_mapper: dict, new_col_order: list[str]) -> pd.DataFrame:
    """
    Renames and reorders columns in a DataFrame.

    Args: FIXME
        df (pd.DataFrame): The DataFrame to process.
        rename_mapper (dict): Dictionary mapping old column names to new names.
        new_col_order (list[str]): List specifying the desired column order.

    Returns:
        pd.DataFrame: DataFrame with renamed and reordered columns.
    """
    baseline_rename_mapper = {
        "subject_id": "patient_id", "hadm_id": "hospitalization_id",
    }
    return (
        df.rename(columns = baseline_rename_mapper | rename_mapper)
        .reindex(columns = new_col_order)
        )

# --------
#   EDA
# --------

def check_duplicates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Checks for duplicates in a DataFrame based on specified columns.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        cols (list[str]): List of columns to check for duplicates.

    Returns:
        pd.DataFrame: DataFrame containing the duplicate rows.
    """
    duplicates = df[df.duplicated(subset=cols, keep=False)]
    if not duplicates.empty:
        logging.warning(f"Found {len(duplicates)} duplicate rows based on columns {cols}.")
    return duplicates

# src/tables/position.py
import numpy as np
import pandas as pd
import logging
import duckdb
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, con  

setup_logging()

POSITION_COL_NAMES = [
    "hospitalization_id", "recorded_dttm", "position_name", "position_category"
    ]

def main():
    logging.info("starting to build clif position table -- ")
    po_events = fetch_mimic_events([224093])
    query = f"""
    SELECT 
        CAST(hadm_id AS VARCHAR) as hospitalization_id,
        CAST(time AS TIMESTAMP) as recorded_dttm,
        CAST(value AS VARCHAR) as position_name,
        CAST(
            CASE 
                WHEN value = 'Prone' THEN 'prone'
                ELSE 'not_prone'
            END AS VARCHAR
        ) as position_category
    FROM po_events
    """
    po_events_c = duckdb.query(query).df()
    

if __name__ == "__main__":
    main()
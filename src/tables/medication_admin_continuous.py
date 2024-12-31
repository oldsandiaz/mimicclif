# src/tables/medication_admin_continuous.py
import numpy as np
import pandas as pd
import logging
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging    

setup_logging()

def main():
    logging.info("starting to build clif medication_admin_continuous table -- ")
    
    
    
if __name__ == "__main__":
    main()
# src/tables/adt.py
import numpy as np
import pandas as pd
import logging
from importlib import reload
import src.utils
reload(src.utils)
from src.utils import construct_mapper_dict, load_mapping_csv, \
    rename_and_reorder_cols, save_to_rclif, setup_logging, mimic_table_pathfinder

setup_logging()

ADT_COL_NAMES = [
    "patient_id", "hospitalization_id", "hospital_id", "in_dttm", "out_dttm", "location_name", "location_category"
]

ADT_COL_RENAME_MAPPER = {
    'intime': 'in_dttm',
    'outtime': 'out_dttm',
    'careunit': 'location_name'
}

def main():
    """
    Create the CLIF ADT table.
    
    """
    logging.info("starting to build clif adt table -- ")
    # load mapping
    adt_mapping = load_mapping_csv("adt")  
    adt_mapper_dict = construct_mapper_dict(adt_mapping, "careunit", "location_category")

    # Filter transfers with valid careunit and hadm_id
    mimic_transfers = pd.read_parquet(mimic_table_pathfinder("transfers"))
    
    logging.info("filtering out NA transfers...") 
    adt = mimic_transfers.dropna(subset=["hadm_id"]) \
        .query("careunit != 'UNKNOWN'")
    
    logging.info("mapping mimic careunit to mimic location_category...")
    adt['location_category'] = adt['careunit'].map(adt_mapper_dict)

    logging.info("renaming, reordering, and re-casting columns...")
    adt_final = rename_and_reorder_cols(adt, ADT_COL_RENAME_MAPPER, ADT_COL_NAMES)
    adt_final["patient_id"] = adt_final["patient_id"].astype(str)
    adt_final['hospitalization_id'] = adt_final['hospitalization_id'].astype(int).astype(str)
    adt_final['hospital_id'] = adt_final['hospital_id'].astype(str)
    adt_final['in_dttm'] = pd.to_datetime(adt_final['in_dttm'])
    adt_final['out_dttm'] = pd.to_datetime(adt_final['out_dttm'])

    save_to_rclif(adt_final, "adt")
    logging.info("output saved to a parquet file, everything completed for the adt table!")

if __name__ == "__main__":
    main()

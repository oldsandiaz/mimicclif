import pandas as pd
import logging
from src.utils import * # load_mimic_table, save_to_rclif, rename_and_reorder_cols, load_mapping_csv, construct_mapper_dict

# Define column names and mappings for the final output
ADT_COL_NAMES = [
    "patient_id", "hospitalization_id", "hospital_id", "in_dttm", "out_dttm", "location_name", "location_category"
]

ADT_COL_RENAME_MAPPER = {
    'intime': 'in_dttm',
    'outtime': 'out_dttm',
    'careunit': 'location_name'
}

def map_to_adt_table(mimic_transfers):
    """
    Processes the `transfers` table to create the CLIF ADT table.
    
    Args:
        mimic_transfers (pd.DataFrame): Preloaded transfers table.
        adt_mapper_dict (dict): Dictionary for mapping care units to location categories.
    """
    logging.info("Starting to process ADT table...")
    adt_mapping = load_mapping_csv("adt")  
    adt_mapper_dict = construct_mapper_dict(adt_mapping, "careunit", "location_category")

    # Filter transfers with valid careunit and hadm_id
    logging.info("Filtering valid transfers...")
    adt = mimic_transfers.dropna(subset=["careunit", "hadm_id"]).copy()

    # Map location categories
    logging.info("Mapping location categories...")
    adt['location_category'] = adt['careunit'].map(adt_mapper_dict)

    # Rename and reorder columns
    logging.info("Renaming and reordering columns...")
    adt_final = rename_and_reorder_cols(adt, ADT_COL_RENAME_MAPPER, ADT_COL_NAMES)

    # Cast data types
    logging.info("Casting data types...")
    adt_final["patient_id"] = adt_final["patient_id"].astype(str)
    adt_final['hospitalization_id'] = adt_final['hospitalization_id'].astype(int).astype(str)
    adt_final['hospital_id'] = adt_final['hospital_id'].astype(str)
    adt_final['in_dttm'] = pd.to_datetime(adt_final['in_dttm'])
    adt_final['out_dttm'] = pd.to_datetime(adt_final['out_dttm'])

    # Save final output
    save_to_rclif(adt_final, "adt")
    logging.info("ADT table processed and saved successfully.")
    
    return adt_final

if __name__ == "__main__":
    mimic_transfers = load_mimic_table("hosp", "transfers")
    adt_final = map_to_adt_table(mimic_transfers)

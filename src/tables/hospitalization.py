# src/tables/hospitalization.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import construct_mapper_dict, load_mapping_csv, \
    rename_and_reorder_cols, save_to_rclif, setup_logging, mimic_table_pathfinder

setup_logging()
HOSP_COL_NAMES = [
    "patient_id", "hospitalization_id", "hospitalization_joined_id", "admission_dttm", "discharge_dttm",
    "age_at_admission", "admission_type_name", "admission_type_category",
    "discharge_name", "discharge_category", "zipcode_nine_digit", "zipcode_five_digit", 
    "census_block_code", "census_block_group_code", "census_tract", "state_code", "county_code"
]

HOSP_COL_RENAME_MAPPER = {
    "admittime": "admission_dttm",
    "dischtime": "discharge_dttm",
    "admission_type": "admission_type_name",
    "discharge_location": "discharge_name"
}

def main():
    """
    Processes the `admissions` and `patients` tables to create the CLIF hospitalization table.
    """
    logging.info("starting to build clif hospitalization table -- ")
    discharge_mapping = load_mapping_csv("discharge")
    discharge_mapper = construct_mapper_dict(
        discharge_mapping, "discharge_location", "disposition_category"
        )
    # add mapping of all NA discharge_location to "missing"
    discharge_mapper[None] = "Missing" # OR: discharge_mapper[np.nan] = 'Missing'
    mimic_admissions = pd.read_parquet(mimic_table_pathfinder("admissions"))
    mimic_patients = pd.read_parquet(mimic_table_pathfinder("patients"))
    
    hosp = mimic_admissions[[
        "subject_id", "hadm_id", "admittime", "dischtime", "admission_type", "discharge_location"
    ]].copy()

    hosp["discharge_category"] = hosp["discharge_location"].map(discharge_mapper)

    logging.info("adding demographic data to calculate age at admission...")
    hosp_merged = pd.merge(
        hosp, 
        mimic_patients[["subject_id", "anchor_age", "anchor_year"]],
        on="subject_id", 
        how="left"
    )

    hosp_merged["age_at_admission"] = hosp_merged["anchor_age"] \
        + pd.to_datetime(hosp_merged["admittime"]).dt.year \
        - hosp_merged["anchor_year"]

    logging.info("renaming, reordering, and recasting columns...")
    hosp_final = rename_and_reorder_cols(hosp_merged, HOSP_COL_RENAME_MAPPER, HOSP_COL_NAMES)

    for col in hosp_final.columns:
        if "dttm" in col:
            hosp_final[col] = pd.to_datetime(hosp_final[col], errors="coerce")
        elif col == "age_at_admission":
            continue
        else:
            hosp_final[col] = hosp_final[col].astype("string")

    save_to_rclif(hosp_final, "hospitalization")
    logging.info("output saved to a parquet file, everything completed for the hospitalization table!")

if __name__ == "__main__":
    main()
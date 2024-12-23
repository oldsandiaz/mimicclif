'''
TODO
- fix all FIXME
- fix import 
- update logging info or print
'''

import pandas as pd
import logging
from src.utils import *

# Define column names and mappings for the final output
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

# FIXME
discharge_mapping = load_mapping_csv("discharge")
discharge_mapper_dict = construct_mapper_dict(
    discharge_mapping, "discharge_location", "disposition_category"
    )

def map_to_hospitalization_table(mimic_admissions, mimic_patients):
    """
    Processes the `admissions` and `patients` tables to create the CLIF hospitalization table.
    """
    logging.info("Starting to process hospitalization table...")

    # Process hospitalization data
    logging.info("Processing admissions data...")
    hosp = mimic_admissions[[
        "subject_id", "hadm_id", "admittime", "dischtime", "admission_type", "discharge_location"
    ]].copy()

    # Map discharge categories
    hosp["discharge_category"] = hosp["discharge_location"].map(discharge_mapper_dict)

    # Add patient demographics for age calculation
    logging.info("Adding demographic data for age calculation...")
    hosp_merged = pd.merge(
        hosp, 
        mimic_patients[["subject_id", "anchor_age", "anchor_year"]],
        on="subject_id", 
        how="left"
    )

    # Calculate age at admission
    hosp_merged["age_at_admission"] = hosp_merged["anchor_age"] \
        + pd.to_datetime(hosp_merged["admittime"]).dt.year \
        - hosp_merged["anchor_year"]

    # Rename and reorder columns
    logging.info("Renaming and reordering columns...")
    hosp_final = rename_and_reorder_cols(hosp_merged, HOSP_COL_RENAME_MAPPER, HOSP_COL_NAMES)

    # Cast data types
    logging.info("Casting data types...")
    for col in hosp_final.columns:
        if "dttm" in col:
            hosp_final[col] = pd.to_datetime(hosp_final[col], errors="coerce")
        elif col == "age_at_admission":
            continue
        else:
            hosp_final[col] = hosp_final[col].astype(str)

    # Save final output
    # FIXME
    save_to_rclif(hosp_final, "../rclif/clif_hospitalization.parquet")
    logging.info("Hospitalization table processed and saved successfully.")

if __name__ == "__main__":
    # Load required tables
    mimic_admissions = load_mimic_table("hosp", "admissions")
    mimic_patients = load_mimic_table("hosp", "patients")
    map_to_hospitalization_table(mimic_admissions, mimic_patients)

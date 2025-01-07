# src/tables/patient_assessments.py
import numpy as np
import pandas as pd
import logging
import duckdb
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, con, REPO_ROOT, mimic_table_pathfinder

setup_logging()

GCS_MAPPER = {"gcs": "gcs_total", "gcs_eyes": "gcs_eye"}

PA_COL_NAMES = [
    "hospitalization_id", "recorded_dttm", "assessment_name", "assessment_category",
    "assessment_group", "numerical_value", "categorical_value", "text_value"
    ]

PA_COLS_RENAME_MAPPER = {
    "time": "recorded_dttm", "value": "text_value"
}

def main():
    logging.info("starting to build clif patient assessments table -- ")
    pa_mcide_url = "https://raw.githubusercontent.com/clif-consortium/CLIF/main/mCIDE/clif_patient_assessment_categories.csv"
    pa_mcide_mapping = pd.read_csv(pa_mcide_url)
    pa_category_to_group_mapper = dict(
        zip(pa_mcide_mapping["assessment_category"], pa_mcide_mapping["assessment_group"]))

    logging.info("part 1: fetching GCS data...")
    icustays = pd.read_parquet(mimic_table_pathfinder("icustays"))
    hadmid_stayid = icustays[["hadm_id", "stay_id"]].drop_duplicates()
    logging.info("executing official MIMIC script to fetch GCS data...")
    gcs_sql_path = REPO_ROOT / 'src/tables/patient_assessments_gcs.sql'
    with open(str(gcs_sql_path), 'r') as file:
        gcs_sql_script = file.read()
    query = gcs_sql_script.format(chartevents = mimic_table_pathfinder("chartevents"))
    gcs = con.execute(query).fetchdf()
    
    logging.info("pivoting and cleaning GCS data...")
    gcs_c = pd.merge(
        gcs, hadmid_stayid, on = "stay_id", how = "left"
    )
    gcs_cl = pd.melt(
        gcs_c, id_vars = ["subject_id", "hadm_id", "charttime"], 
        value_vars = ["gcs", "gcs_motor", "gcs_verbal", "gcs_eyes"],
        var_name = "assessment_name", value_name = "numerical_value")
    gcs_cl["assessment_category"] = np.where(
        gcs_cl["assessment_name"].isin(["gcs", "gcs_eyes"]),
        gcs_cl["assessment_name"].map(GCS_MAPPER),
        gcs_cl["assessment_name"]
    )
    gcs_cl.dropna(subset = ["hadm_id"], inplace = True)
    gcs_cl["hadm_id"] = gcs_cl["hadm_id"].astype(int).astype(str)
    gcs_clf = rename_and_reorder_cols(gcs_cl, {"charttime": "recorded_dttm"}, PA_COL_NAMES)
    gcs_clf["assessment_group"] = gcs_clf["assessment_category"].map(pa_category_to_group_mapper)
    
    logging.info("part 2: fetching RASS data...")
    rass_events = fetch_mimic_events([228096])
    rass_events = convert_and_sort_datetime(rass_events)
    rass_events['numerical_value'] = rass_events['value'].str.slice(0,3).astype(float)
    rass_events["assessment_name"] = "Richmond-RAS Scale"
    rass_events["assessment_category"] = "RASS"
    rass_events["assessment_group"] = rass_events["assessment_category"].map(pa_category_to_group_mapper)
    rass_events_clean = rename_and_reorder_cols(rass_events, PA_COLS_RENAME_MAPPER, PA_COL_NAMES)
    rass_events_clean.drop_duplicates(
        subset=["hospitalization_id", "recorded_dttm", "assessment_category", "numerical_value"], inplace = True)

    logging.info("merging GCS and RASS data...")
    pa_m = pd.concat([gcs_clf, rass_events_clean])
    
    logging.info("converting column dtypes...")
    pa_m["hospitalization_id"] = pa_m["hospitalization_id"].astype(str)
    pa_m["categorical_value"] = pa_m["categorical_value"].astype(str)
    pa_m["recorded_dttm"] = pd.to_datetime(pa_m["recorded_dttm"])

    save_to_rclif(pa_m, "patient_assessments")
    logging.info("output saved to a parquet file, everything completed for the patient assessments table!")

if __name__ == "__main__":
    main()


# src/tables/patient_assessments.py
import numpy as np
import pandas as pd
import logging
import duckdb
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, con, REPO_ROOT, mimic_table_pathfinder, \
    convert_tz_to_utc

setup_logging()

GCS_MAPPER = {"gcs": "gcs_total", "gcs_eyes": "gcs_eye"}

PA_COL_NAMES = [
    "hospitalization_id", "recorded_dttm", "assessment_name", "assessment_category",
    "assessment_group", "numerical_value", "categorical_value", "text_value"
    ]

PA_COLS_RENAME_MAPPER = {
    "time": "recorded_dttm", "value": "text_value"
}

def fetch_gcs():
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
    gcs_cl["hadm_id"] = gcs_cl["hadm_id"].astype(int).astype("string")
    gcs_clf = rename_and_reorder_cols(gcs_cl, {"charttime": "recorded_dttm"}, PA_COL_NAMES)
    return gcs_clf


def fetch_rass():
    rass_events = fetch_mimic_events([228096])
    rass_events = convert_and_sort_datetime(rass_events)
    rass_events['numerical_value'] = rass_events['value'].str.slice(0,3).astype(float)
    rass_events["assessment_name"] = "Richmond-RAS Scale"
    rass_events["assessment_category"] = "RASS"
    rass_events_clean = rename_and_reorder_cols(rass_events, PA_COLS_RENAME_MAPPER, PA_COL_NAMES)
    rass_events_clean.drop_duplicates(
        subset=["hospitalization_id", "recorded_dttm", "assessment_category", "numerical_value"], inplace = True)
    return rass_events_clean

def fetch_braden():
    braden = fetch_mimic_events([224054, 224055, 224056, 224057, 224058, 224059])
    query = f"""
    PIVOT braden
    ON label
    USING FIRST(value)
    GROUP BY hadm_id, time
    """
    braden_w = con.execute(query).fetchdf()
    query = f"""
    SELECT
        hadm_id, time, 
        CASE
            WHEN "Braden Activity" = 'Bedfast' THEN 1
            WHEN "Braden Activity" = 'Chairfast' THEN 2
            WHEN "Braden Activity" = 'Walks Occasionally' THEN 3
            WHEN "Braden Activity" = 'Walks Frequently' THEN 4
            ELSE NULL
            END AS braden_activity,
        CASE
            WHEN "Braden Friction/Shear" = 'Problem' THEN 1
            WHEN "Braden Friction/Shear" = 'Potential Problem' THEN 2
            WHEN "Braden Friction/Shear" = 'No Apparent Problem' THEN 3
            ELSE NULL
            END AS braden_friction,
        CASE
            WHEN "Braden Mobility" = 'Completely Immobile' THEN 1
            WHEN "Braden Mobility" = 'Very Limited' THEN 2
            WHEN "Braden Mobility" = 'Slight Limitations' THEN 3
            WHEN "Braden Mobility" = 'No Limitations' THEN 4
            ELSE NULL
            END AS braden_mobility,
        CASE
            WHEN "Braden Moisture" = 'Consistently Moist' THEN 1
            WHEN "Braden Moisture" = 'Moist' THEN 2
            WHEN "Braden Moisture" = 'Occasionally Moist' THEN 3
            WHEN "Braden Moisture" = 'Rarely Moist' THEN 4
            ELSE NULL
            END AS braden_moisture,
        CASE
            WHEN "Braden Nutrition" = 'Very Poor' THEN 1
            WHEN "Braden Nutrition" = 'Probably Inadequate' THEN 2
            WHEN "Braden Nutrition" = 'Adequate' THEN 3
            WHEN "Braden Nutrition" = 'Excellent' THEN 4
            ELSE NULL
            END AS braden_nutrition,
        CASE
            WHEN "Braden Sensory Perception" = 'Completely Limited' THEN 1
            WHEN "Braden Sensory Perception" = 'Very Limited' THEN 2
            WHEN "Braden Sensory Perception" = 'Slight Impairment' THEN 3
            WHEN "Braden Sensory Perception" = 'No Impairment' THEN 4
            ELSE NULL
            END AS braden_sensory,
        (braden_activity + braden_friction + braden_mobility + braden_moisture + braden_nutrition + braden_sensory) AS braden_total
    FROM braden_w
    """
    braden_wc = con.execute(query).fetchdf()
    # unpivot (from wide to long) for the numerical values
    query = f"""
    UNPIVOT braden_wc
    ON COLUMNS('braden_')
    INTO
        NAME assessment_category
        VALUE numerical_value;
    """
    braden_wcl = con.execute(query).fetchdf()
    # unpivot (from wide to long) for the *categorical* values
    query = f"""
    UNPIVOT (
        SELECT
            hadm_id, time,
            "Braden Activity" AS braden_activity,
            "Braden Friction/Shear" AS braden_friction,
            "Braden Mobility" AS braden_mobility,
            "Braden Moisture" AS braden_moisture,
            "Braden Nutrition" AS braden_nutrition,
            "Braden Sensory Perception" AS braden_sensory
        FROM braden_w
    )
    ON COLUMNS('braden_')
    INTO
        NAME assessment_category
        VALUE categorical_value;
    """
    braden_wl = con.execute(query).fetchdf()
    # merging numerical_value and categorical_value
    braden_m = pd.merge(braden_wcl, braden_wl, on=["hadm_id", "time", "assessment_category"], how="outer")
    query = f"""
    SELECT
        hadm_id as hospitalization_id,
        time as recorded_dttm,
        CASE
            WHEN assessment_category = 'braden_total' THEN NULL
            WHEN assessment_category = 'braden_activity' THEN 'Braden Activity'
            WHEN assessment_category = 'braden_friction' THEN 'Braden Friction/Shear'
            WHEN assessment_category = 'braden_mobility' THEN 'Braden Mobility'
            WHEN assessment_category = 'braden_moisture' THEN 'Braden Moisture'
            WHEN assessment_category = 'braden_nutrition' THEN 'Braden Nutrition'
            WHEN assessment_category = 'braden_sensory' THEN 'Braden Sensory Perception'
            ELSE assessment_category
            END AS assessment_name,
        assessment_category,
        numerical_value,
        categorical_value
    FROM braden_m
    """
    braden_mf = con.execute(query).fetchdf()
    return braden_mf

def fetch_cam():
    cam_icu = fetch_mimic_events(
        [228300, 228337, 229326, 228301, 228336, 229325, 228302, 228334, 228303, 228335, 229324]
    )
    query = f"""
    PIVOT (
        SELECT
            hadm_id, time,
            itemid,
            CASE
                WHEN itemid = 228300 THEN 'CAM-ICU MS Change'
                ELSE label
            END AS label,
            value
        FROM cam_icu
    )
    ON label
    USING FIRST(value)
    GROUP BY hadm_id, time
    """
    cam_icu_w = con.execute(query).fetchdf()

    # delirious = Yes if 
    # (mental = True) AND (inattention = True) AND (LOC OR thinking = True)
    # which implies that delirious = No
    # if mental = False OR inattention = False OR (LOC = False AND thinking = False)

    query = f"""
    SELECT
        hadm_id, time, COLUMNS('CAM-ICU'),
        coalesce("CAM-ICU Altered LOC", "CAM-ICU RASS LOC") AS loc,
        CASE
            WHEN ("CAM-ICU MS Change" LIKE '%Yes%') 
                AND ("CAM-ICU Inattention" LIKE '%Yes%') 
                AND (("CAM-ICU Disorganized thinking" LIKE '%Yes%') OR (loc LIKE '%Yes%'))
                THEN 'Positive'
            WHEN ("CAM-ICU MS Change" LIKE '%No%') 
                OR ("CAM-ICU Inattention" LIKE '%No%') 
                OR (("CAM-ICU Disorganized thinking" LIKE '%No%') AND (loc LIKE '%No%'))
                THEN 'Negative'
            ELSE NULL
            END AS cam_total
    FROM cam_icu_w
    """
    cam_icu_wc = con.execute(query).fetchdf()
    # unpivot (from wide to long) 
    query = f"""
    UNPIVOT (
        SELECT
            COLUMNS(* EXCLUDE ('loc'))
        FROM cam_icu_wc
    )
    ON COLUMNS('CAM-ICU|cam')
    INTO
        NAME assessment_name
        VALUE categorical_value;
    """
    cam_icu_wcl = con.execute(query).fetchdf()
    query = f"""
    SELECT
        hadm_id as hospitalization_id,
        time as recorded_dttm,
        CASE
            WHEN assessment_name = 'CAM-ICU MS Change' THEN 'cam_mental'
            WHEN assessment_name = 'cam_total' THEN 'cam_total'
            WHEN assessment_name = 'CAM-ICU Inattention' THEN 'cam_inattention'
            WHEN assessment_name = 'CAM-ICU Disorganized thinking' THEN 'cam_thinking'
            WHEN assessment_name in ('CAM-ICU RASS LOC', 'CAM-ICU Altered LOC') THEN 'cam_loc'
            ELSE NULL
            END AS assessment_category,
        CASE
            WHEN assessment_name = 'cam_total' THEN NULL
            ELSE assessment_name
            END AS assessment_name,
        categorical_value
    FROM cam_icu_wcl
    """
    cam_icu_wclc = con.execute(query).fetchdf()
    
    return cam_icu_wclc

def main():
    logging.info("starting to build clif patient assessments table -- ")
    pa_mcide_url = "https://raw.githubusercontent.com/clif-consortium/CLIF/main/mCIDE/clif_patient_assessment_categories.csv"
    pa_mcide_mapping = pd.read_csv(pa_mcide_url)
    pa_category_to_group_mapper = dict(
        zip(pa_mcide_mapping["assessment_category"], pa_mcide_mapping["assessment_group"]))

    logging.info("part 1: fetching GCS data...")
    gcs = fetch_gcs()
    
    logging.info("part 2: fetching RASS data...")
    rass = fetch_rass()

    logging.info("part 3: fetching Braden data...")
    braden = fetch_braden()
    
    logging.info("part 4: fetching CAM data...")
    cam = fetch_cam()   

    logging.info("merging all of the above...")
    pa_m = pd.concat([gcs, rass, braden, cam])
    
    logging.info("converting column dtypes...")
    pa_m["hospitalization_id"] = pa_m["hospitalization_id"].astype("string")
    pa_m["categorical_value"] = pa_m["categorical_value"].astype("string")
    pa_m["recorded_dttm"] = pd.to_datetime(pa_m["recorded_dttm"])
    pa_m["recorded_dttm"] = convert_tz_to_utc(pa_m["recorded_dttm"])
    pa_m["assessment_group"] = pa_m["assessment_group"].map(pa_category_to_group_mapper)
    
    save_to_rclif(pa_m, "patient_assessments")
    logging.info("output saved to a parquet file, everything completed for the patient assessments table!")

if __name__ == "__main__":
    main()


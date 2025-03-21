# src/tables/patient.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
import src.utils
reload(src.utils)
from src.utils import construct_mapper_dict, load_mapping_csv, \
    rename_and_reorder_cols, save_to_rclif, setup_logging, mimic_table_pathfinder

setup_logging()

PATIENT_COL_NAMES = [
    "patient_id", "race_name", "race_category", "ethnicity_name", "ethnicity_category",
    "sex_name", "sex_category", "birth_date", "death_dttm", "language_name", "language_category"
]

def check_multi_race_over_encounters(df, col: str = "race_category"):
    '''
    Check if a patient has multiple differing races recorded over encounters.
    '''
    race_counts = df.groupby('patient_id')[col].nunique()
    multi_race_indices = race_counts[race_counts > 1].index
    multi_race_encounters = df[
        df['patient_id'].isin(multi_race_indices)
        ]
    return multi_race_encounters

def report_nonunique_race_ethn_across_encounters(df):
    '''
    Report patients with non-unique race and ethnicity across encounters.
    '''
    query = """
    SELECT 
        patient_id,
        FIRST(race_category) as race_category,
        FIRST(ethnicity_category) as ethnicity_category,
        COUNT(DISTINCT race_category) AS unique_race_count,
        COUNT(DISTINCT ethnicity_category) AS unique_ethn_count
    FROM df
    /* WHERE race_category NOT IN ('Other', 'Unknown') OR ethnicity_category NOT IN ('Other', 'Unknown') */
    GROUP BY patient_id
    HAVING unique_race_count > 1 OR unique_ethn_count > 1
    """
    df2 = duckdb.query(query).df()
    n1 = (df2['unique_race_count'] > 1).sum() 
    n2 = (df2['unique_ethn_count'] > 1).sum()
    n_total = df.patient_id.nunique()
    logging.info(f"number of patients with non-unique race: {n1} ({n1/n_total:.2%})")
    logging.info(f"number of patients with non-unique ethnicity: {n2} ({n2/n_total:.2%})")
    return n1, n2




def main():
    logging.info("starting to build clif patient table -- ")

    # load mapping
    race_ethnicity_mapping = load_mapping_csv("race_ethnicity")
    race_mapper = construct_mapper_dict(race_ethnicity_mapping, "mimic_race", "race")
    race_mapper[None] = "Unknown"
    ethnicity_mapper = construct_mapper_dict(race_ethnicity_mapping, "mimic_race", "ethnicity")
    ethnicity_mapper[None] = "Unknown"

    # load mimic data
    mimic_patients = pd.read_parquet(mimic_table_pathfinder("patients"))
    mimic_admissions = pd.read_parquet(mimic_table_pathfinder("admissions"))
    
    logging.info("fetching and processing the first component of the patient table: sex/gender data...")
    # fetch sex (intended in CLIF) / gender (available in MIMIC) from mimic_patients
    sex = mimic_patients[["subject_id", "gender"]].copy()
    sex.columns = ["patient_id", "sex_name"]
    sex_mapper = {"M": "Male", "F": "Female"}
    sex["sex_category"] = sex["sex_name"].map(sex_mapper)
    
    logging.info("fetching and processing the second component of the patient table: race and ethnicity data...")
    query = """
    SELECT 
        subject_id as patient_id, 
        hadm_id as hospitalization_id,
        race as race_name, 
        race as ethnicity_name,
        admittime as admittime
    FROM mimic_admissions
    """
    race_ethn = duckdb.query(query).df()
    race_ethn["race_category"] = race_ethn["race_name"].map(race_mapper)
    race_ethn["ethnicity_category"] = race_ethn["ethnicity_name"].map(ethnicity_mapper)
    query = """
    SELECT 
        patient_id,
        hospitalization_id,
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category,
        admittime,
        /* mark patients with 'truly' uninformative race and ethnicity, defined by both race and ethnicity being "Other" or "Unknown". */
        CASE
            WHEN (race_category IN ('Other', 'Unknown')) AND (ethnicity_category IN ('Other', 'Unknown')) THEN 1
            ELSE 0
        END AS true_uninfo 
    FROM race_ethn
    """
    race_ethn_uninfo = duckdb.query(query).df()
    
    query = """
    SELECT 
        patient_id, 
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category,
        COUNT(*) AS count,
        MAX(admittime) AS most_recent,
        true_uninfo,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id 
            ORDER BY 
                count DESC, 
                true_uninfo,
                most_recent DESC
                ) 
            AS rn /* row number */
    FROM race_ethn_uninfo
    GROUP BY patient_id, race_name, race_category, ethnicity_name, ethnicity_category, true_uninfo
    """
    race_ethn_ranked = duckdb.query(query).df()
    
    # report patients with non-unique race and ethnicity across encounters
    n1, n2 = report_nonunique_race_ethn_across_encounters(race_ethn_ranked)
    
    query = """
    SELECT 
        patient_id,
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category
    FROM race_ethn_ranked
    WHERE rn = 1
    """
    race_ethn_c = duckdb.query(query).df()

    logging.info("fetching and processing the third component: death data...")
    death = mimic_admissions[["subject_id", "deathtime"]].copy().dropna(subset=["deathtime"]).drop_duplicates()
    death.columns = ["patient_id", "death_dttm"]
    
    # TODO: add language data
    # logging.info("fetching and processing the fourth component: language data...") 
    # language = mimic_patients[["subject_id", "language"]].copy().dropna(subset=["language"])
    # language.columns = ["patient_id", "language_name"]
    
    logging.info("merging the four components...")
    # merge
    patient_merged = pd.merge(race_ethn_c, sex, on = "patient_id", how = "outer", validate = "one_to_one")
    patient_merged = pd.merge(patient_merged, death, on = "patient_id", how = "outer", validate = "one_to_one")
    # patient_merged = pd.merge(patient_merged, language, on = "patient_id", how = "outer")

    logging.info("reindexing columns and converting data types...")
    patient_final = patient_merged.reindex(columns = PATIENT_COL_NAMES)
    patient_final["patient_id"] = patient_final["patient_id"].astype("string")
    patient_final["birth_date"] = pd.to_datetime(patient_final["birth_date"])
    patient_final["death_dttm"] = pd.to_datetime(patient_final["death_dttm"])
    patient_final["language_name"] = patient_final["language_name"].astype("string")
    patient_final["language_category"] = patient_final["language_category"].astype("string")

    patient_final.drop_duplicates(inplace = True)
    
    save_to_rclif(patient_final, "patient")
    logging.info("output saved to a parquet file, everything completed for the patient table!")

if __name__ == "__main__":
    main()
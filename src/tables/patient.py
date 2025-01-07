# src/tables/patient.py
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

def main():
    logging.info("starting to build clif patient table -- ")

    # load mapping
    race_ethnicity_mapping = load_mapping_csv("race_ethnicity")
    race_mapper_dict = construct_mapper_dict(race_ethnicity_mapping, "mimic_race", "race")
    ethnicity_mapper_dict = construct_mapper_dict(race_ethnicity_mapping, "mimic_race", "ethnicity")

    # load mimic data
    mimic_patients = pd.read_parquet(mimic_table_pathfinder("patients"))
    mimic_admissions = pd.read_parquet(mimic_table_pathfinder("admissions"))
    
    logging.info("fetching and processing the first component of the patient table: sex/gender data...")
    # fetch sex (intended in CLIF) / gender (available in MIMIC) from mimic_patients
    sex = mimic_patients[["subject_id", "gender"]].copy()
    sex.columns = ["patient_id", "sex_name"]
    sex["sex_category"] = sex["sex_name"].map(lambda x: "Female" if x == "F" else "Male")
    
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
    race_ethn["race_category"] = race_ethn["race_name"].map(race_mapper_dict)
    race_ethn["ethnicity_category"] = race_ethn["ethnicity_name"].map(ethnicity_mapper_dict)
    query = """
    SELECT 
        patient_id,
        hospitalization_id,
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category,
        admittime,
        CASE
            WHEN (race_category IN ('Other', 'Unknown')) AND (ethnicity_category IN ('Other', 'Unknown')) THEN 1
            ELSE 0
        END AS true_noninfo
    FROM race_ethn
    """
    race_ethn = duckdb.query(query).df()
    race_ethn
    
    query = """
    SELECT 
        patient_id, 
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category,
        COUNT(*) AS count,
        MAX(admittime) AS most_recent,
        true_noninfo,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id 
            ORDER BY 
                count DESC, 
                true_noninfo,
                most_recent DESC
                ) 
            AS rn
    FROM race_ethn
    GROUP BY patient_id, race_name, race_category, ethnicity_name, ethnicity_category, true_noninfo
    """
    race_ethn_ranked = duckdb.query(query).df()
    
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
    
    # apply de-deduplication logic to create one-to-one mapping from patient_id to race
    # race_ethn_informative = race_ethn.loc[~race_ethn["race_category"].isin(["Other", "Unknown"]), ]
    # multi_race_ethn_informative = check_multi_race_over_encounters(race_ethn_informative)
    
    # multi_race_deduped = (
    #     multi_race_ethn_informative.groupby('patient_id')
    #     .apply(lambda x: (
    #         x.groupby('race_category')
    #         .agg(count=('race_category', 'size'),
    #             most_recent=('admittime', 'max'))
    #         .sort_values(['count', 'most_recent'], ascending=[False, False])
    #         .head(1)))
    #     .reset_index()
    #     )
    
    # unique_race_mapper_dict = dict(zip(multi_race_deduped["patient_id"], multi_race_deduped["race_category"]))

    # race_ethn_deduped = race_ethn.drop_duplicates(["patient_id", "race_category", "ethnicity_category"]).copy()

    # race_ethn_deduped["race_category"] = np.where(
    #     race_ethn_deduped["patient_id"].isin(multi_race_deduped["patient_id"]),
    #     race_ethn_deduped["patient_id"].map(unique_race_mapper_dict),
    #     race_ethn_deduped["race_category"]
    # )

    # race_ethn_deduped.drop_duplicates(["patient_id", "race_category", "ethnicity_category"], inplace=True)

    # # remove the non-informative others unless they are the only race
    # race_ethn_deduped_informative = race_ethn_deduped.groupby("patient_id").apply(
    #     lambda gr: gr if len(gr) == 1 else gr[~gr["race_category"].isin(["Other","Unknown"])]
    # ).reset_index(drop = True)
    
    # # repeat the same for ethnicity
    # race_ethn_deduped_informative = race_ethn_deduped.groupby("patient_id").apply(
    #     lambda gr: gr if len(gr) == 1 else gr[~gr["ethnicity_category"].isin(["Other", "Unknown", "Non-Hispanic"])]
    # ).reset_index(drop = True)
    
    logging.info("fetching and processing the third component: death data...")
    death = mimic_admissions[["subject_id", "deathtime"]].copy().dropna(subset=["deathtime"]).drop_duplicates()
    death.columns = ["patient_id", "death_dttm"]
    
    # TODO: add language data
    # logging.info("fetching and processing the fourth component: language data...") 
    # language = mimic_patients[["subject_id", "language"]].copy().dropna(subset=["language"])
    # language.columns = ["patient_id", "language_name"]
    
    logging.info("merging the four components...")
    # merge
    patient_merged = pd.merge(race_ethn_c, sex, on = "patient_id", how = "outer")
    patient_merged = pd.merge(patient_merged, death, on = "patient_id", how = "outer")
    # patient_merged = pd.merge(patient_merged, language, on = "patient_id", how = "outer")

    logging.info("reindexing columns and converting data types...")
    patient_final = patient_merged.reindex(columns = PATIENT_COL_NAMES)
    patient_final["patient_id"] = patient_final["patient_id"].astype(str)
    patient_final["birth_date"] = pd.to_datetime(patient_final["birth_date"])
    patient_final["death_dttm"] = pd.to_datetime(patient_final["death_dttm"])
    patient_final["language_name"] = patient_final["language_name"].astype(str)
    patient_final["language_category"] = patient_final["language_category"].astype(str)

    save_to_rclif(patient_final, "patient")
    logging.info("output saved to a parquet file, everything completed for the patient table!")

if __name__ == "__main__":
    main()
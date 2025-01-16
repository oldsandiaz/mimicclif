import logging
from src.tables import patient, hospitalization, adt, vitals, labs, patient_assessments, \
    respiratory_support, medication_admin_continuous, position
from src.utils import setup_logging, resave_all_mimic_tables_from_csv_to_parquet, \
    resave_select_mimic_tables_from_csv_to_parquet, resave_mimic_table_from_csv_to_parquet, \
    MIMIC_TABLES_NEEDED_FOR_CLIF, config, MIMIC_CSV_DIR, MIMIC_PARQUET_DIR, create_dir_if_not_exists, \
    CURRENT_WORKSPACE
    

setup_logging()

CLIF_TABLES = config["clif_tables"]
CLIF_TABLES_TO_BUILD = [clif_table for clif_table, to_build in CLIF_TABLES.items() if to_build == 1]
logging.info(f"identified {len(CLIF_TABLES_TO_BUILD)} clif tables to build: {CLIF_TABLES_TO_BUILD}")

def main():
    if config["create_mimic_parquet_from_csv"] == 1:
        logging.info(f"since you elected to create the mimic parquet files from csv, we first create these files:")
        create_dir_if_not_exists(MIMIC_PARQUET_DIR)
        overwrite = (config["overwrite_existing_mimic_parquet"] == 1)
        resave_select_mimic_tables_from_csv_to_parquet(tables = MIMIC_TABLES_NEEDED_FOR_CLIF, overwrite = overwrite)
    counter = 1
    logging.info(f"--------------------------------")
    for clif_table_str in CLIF_TABLES_TO_BUILD:
        logging.info(f"building {counter} out of {len(CLIF_TABLES_TO_BUILD)} clif tables")
        clif_table_object = globals()[clif_table_str]
        try:
            clif_table_object.main()
        except Exception as e:
            logging.error(f"error building {clif_table_str}: {e}")
        counter += 1
        logging.info(f"------------------------------")
    logging.info("finished building all clif tables!")

if __name__ == "__main__":
    main()

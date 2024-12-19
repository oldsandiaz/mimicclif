import logging
from src.tables import hospitalization, adt # , vitals # , labs, assessments, dialysis, medications
from src.utils import setup_logging, load_mimic_table

mimic_transfers = load_mimic_table("hosp", "transfers")

mimic_admissions = load_mimic_table("hosp", "admissions")
mimic_patients = load_mimic_table("hosp", "patients")
    

# Initialize logging
setup_logging()

def main():
    logging.info("Pipeline execution started.")
    adt.map_to_adt_table(mimic_transfers)
    # hospitalization.map_to_hospitalization_table(mimic_admissions, mimic_patients)

if __name__ == "__main__":
    main()

import pandas as pd
from src.tables.patient import main as patient_etl_main
from unittest.mock import patch

def test_patient_etl_condenses_rows():
    # Mock data
    mimic_patients = pd.DataFrame({
        'subject_id': [1, 1, 1, 1, 1],
        'gender': ['M', 'M', 'M', 'M', 'M']
    })

    mimic_admissions = pd.DataFrame({
        'subject_id': [1, 1, 1, 1, 1],
        'hadm_id': [101, 102, 103, 104, 105],
        'race': ['White', 'White', 'Black', 'White', 'White'],
        'admittime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']),
        'deathtime': [None, None, None, None, None]
    })

    race_ethnicity_mapping = pd.DataFrame({
        'mimic_race': ['White', 'Black'],
        'race': ['Caucasian', 'African American'],
        'ethnicity': ['Non-Hispanic', 'Non-Hispanic']
    })

    # Mock the functions that load data
    with patch('src.tables.patient.load_mapping_csv', return_value=race_ethnicity_mapping), \
         patch('src.tables.patient.pd.read_parquet', side_effect=[mimic_patients, mimic_admissions]), \
         patch('src.tables.patient.save_to_rclif') as mock_save:

        # Run the ETL process
        patient_etl_main()

        # Check the output
        args, kwargs = mock_save.call_args
        patient_final = args[0]

        # Assert that the output has one row for the patient
        assert len(patient_final) == 1

        # Assert that the race and gender are correctly assigned
        assert patient_final.iloc[0]['race_category'] == 'Caucasian'
        assert patient_final.iloc[0]['sex_category'] == 'Male' 
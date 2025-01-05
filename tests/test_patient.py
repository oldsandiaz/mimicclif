import pandas as pd
import pytest
from importlib import reload
import src.utils
reload(src.utils)
from src.utils import clif_table_pathfinder, clif_test_data_pathfinder
from src.tables.patient import PATIENT_COL_NAMES

# Read the output file
clif_patient = pd.read_parquet(clif_table_pathfinder("patient"))
clif_patient_test_data = pd.read_csv(clif_test_data_pathfinder("patient"))

def test_patient_output():
    for _, expected_row in clif_patient_test_data.iterrows():
        patient_id = expected_row['patient_id']
        actual_row = clif_patient[clif_patient['patient_id'] == str(patient_id)]
        assert len(actual_row) == 1, f"Patient {patient_id} should have only one row."
        for var in PATIENT_COL_NAMES:
            if var == "patient_id":
                continue
            actual_val = actual_row.iloc[0][var]
            expected_val = expected_row[var]
            assert actual_val == expected_val, f"Patient {patient_id}'s {var} is {actual_val}, but should be {expected_val}."

# Run the test
if __name__ == "__main__":
    pytest.main([__file__]) 
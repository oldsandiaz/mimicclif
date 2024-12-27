import logging
import os
import pandas as pd
import json
from pathlib import Path
from functools import cache
import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent

def load_mapping_csv(csv_name: str, dtype = None):
    return pd.read_csv(
        SCRIPT_DIR / f"../data/mappings/mimic-to-clif-mappings - {csv_name}.csv", dtype = dtype
        )
    
def get_relevant_item_ids(mapping_df: pd.DataFrame, decision_col: str, 
                          excluded_labels: list = EXCLUDED_LABELS_DEFAULT,
                          excluded_item_ids: list = None
                          ):
    '''
    Parse the mapping files and find all the relevant item ids for a table
    - decision_col: the col on which to apply the excluded_labels
    - excluded_item_ids: additional item ids to exclude
    '''
    if not excluded_item_ids:
        excluded_item_ids = []
    
    return mapping_df.loc[
        (~mapping_df[decision_col].isin(excluded_labels)) & 
        (~mapping_df["itemid"].isin(excluded_item_ids))
        , "itemid"
        ].unique()
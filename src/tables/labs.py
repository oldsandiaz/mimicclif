# src/tables/labs.py
import numpy as np
import pandas as pd
import logging
import importlib 
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, check_output
import pandera as pa
from typing import Dict, List

from src.utils import (
    construct_mapper_dict,
    fetch_mimic_events,
    load_mapping_csv,
    get_relevant_item_ids,
    find_duplicates,
    rename_and_reorder_cols,
    save_to_rclif,
    convert_and_sort_datetime,
    setup_logging,
    convert_tz_to_utc,
)

COL_NAMES: List[str] = [
    "hospitalization_id", "lab_order_dttm", "lab_collect_dttm", "lab_result_dttm", 
    "lab_order_name", "lab_order_category", "lab_name", "lab_category", 
    "lab_value", "lab_value_numeric", "reference_unit", "lab_specimen_name", 
    "lab_specimen_category", "lab_loinc_code"
]

COL_RENAME_MAPPER: Dict[str, str] = {
    "charttime": "lab_collect_dttm", 
    "storetime": "lab_result_dttm", 
    "value": "lab_value", 
    "valuenum": "lab_value_numeric", 
    "valueuom": "reference_unit"
}

def labs_mapping() -> pd.DataFrame:
    logging.info("Starting data extraction")
    # Load and prepare mapping
    labs_mapping = load_mapping_csv("labs")
    # drop the row corresponding to procalcitonin which is not available in MIMIC
    labs_mapping.dropna(subset=["itemid"], inplace=True)
    labs_mapping["itemid"] = labs_mapping["itemid"].astype(int)
    return labs_mapping

def id_to_name_mapper(labs_mapping: pd.DataFrame) -> dict:
    return construct_mapper_dict(
        labs_mapping, "itemid", "label", 
        excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
    )

def id_to_category_mapper(labs_mapping: pd.DataFrame) -> dict:
    return construct_mapper_dict(
        labs_mapping, "itemid", "lab_category", 
        excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
    )

def labs_items(labs_mapping: pd.DataFrame) -> pd.DataFrame:
    return labs_mapping.loc[
        labs_mapping["decision"].isin(["TO MAP, CONVERT UOM", "TO MAP, AS IS", "UNSURE"]),
        ["lab_category", "itemid", "label", "count"]
    ].copy()

def extracted_le_labs(labs_items: pd.DataFrame) -> pd.DataFrame:
    logging.info("Extracting from labevents table")
    # labevents table has itemids with 5 digits
    labs_items_le = labs_items[labs_items['itemid'].astype("string").str.len() == 5]
    df_le = fetch_mimic_events(labs_items_le['itemid'], original=True, for_labs=True)
    return df_le

def extracted_ce_labs(labs_items: pd.DataFrame) -> pd.DataFrame:
    logging.info("Extracting from chartevents table")
    # chartevents table has itemids with 6 digits
    labs_items_ce = labs_items[labs_items['itemid'].astype("string").str.len() == 6]
    df_ce = fetch_mimic_events(labs_items_ce['itemid'], original=True, for_labs=False)
    return df_ce

def le_labs_translated(extracted_le_labs: pd.DataFrame, id_to_name_mapper: dict, id_to_category_mapper: dict) -> pd.DataFrame:
    extracted_le_labs["lab_name"] = extracted_le_labs["itemid"].map(id_to_name_mapper)
    extracted_le_labs["lab_category"] = extracted_le_labs["itemid"].map(id_to_category_mapper)
    return extracted_le_labs

def le_labs_renamed_reordered(le_labs_translated: pd.DataFrame) -> pd.DataFrame:
    return rename_and_reorder_cols(le_labs_translated, COL_RENAME_MAPPER, COL_NAMES + ["itemid"])

def le_labs_units_converted(le_labs_renamed_reordered: pd.DataFrame) -> pd.DataFrame:
    """Convert units of measurement for specific lab items."""
    # for ionized (free) calcium, to convert a result from mmol/L to mg/dL, multiply the mmol/L value by 4.  
    # https://www.abaxis.com/sites/default/files/resource-packages/Ionized%20Calcium%20CTI%20Sheete%20714179-00P.pdf
    mask_ca = le_labs_renamed_reordered["itemid"].isin([50808, 51624])
    le_labs_renamed_reordered.loc[mask_ca, "lab_value_numeric"] *= 4
    le_labs_renamed_reordered.loc[mask_ca, "reference_unit"] = "mg/dL"
    le_labs_renamed_reordered.loc[mask_ca, "lab_value"] = le_labs_renamed_reordered.loc[mask_ca, "lab_value_numeric"].astype("string")
    
    # convert troponin_t and troponin_i from ng/mL to ng/L, which is to multiply by 1000
    mask_trop = le_labs_renamed_reordered["itemid"].isin([51003, 52642])
    le_labs_renamed_reordered.loc[mask_trop, "lab_value_numeric"] *= 1000
    le_labs_renamed_reordered.loc[mask_trop, "reference_unit"] = "ng/L"
    le_labs_renamed_reordered.loc[mask_trop, "lab_value"] = le_labs_renamed_reordered.loc[mask_trop, "lab_value_numeric"].astype("string")
    
    return le_labs_renamed_reordered

def ce_labs_translated(extracted_ce_labs: pd.DataFrame, id_to_name_mapper: dict, id_to_category_mapper: dict) -> pd.DataFrame:
    extracted_ce_labs["lab_name"] = extracted_ce_labs["itemid"].map(id_to_name_mapper)
    extracted_ce_labs["lab_category"] = extracted_ce_labs["itemid"].map(id_to_category_mapper)
    return extracted_ce_labs

def ce_labs_renamed_reordered(ce_labs_translated: pd.DataFrame) -> pd.DataFrame:
    return rename_and_reorder_cols(ce_labs_translated, COL_RENAME_MAPPER, COL_NAMES + ["itemid"])

def merged(le_labs_units_converted: pd.DataFrame, ce_labs_renamed_reordered: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([le_labs_units_converted, ce_labs_renamed_reordered])
    merged.drop(columns = "itemid", inplace = True)
    return merged

def columns_recasted(merged: pd.DataFrame) -> pd.DataFrame:
    """Clean and recast columns to appropriate data types."""
    for col in merged.columns:
        if "dttm" in col:
            merged[col] = pd.to_datetime(merged[col])
            merged[col] = convert_tz_to_utc(merged[col])
        elif any(x in col for x in ["order", "specimen", "loinc"]):
            merged[col] = ""
        elif col == "hospitalization_id":
            merged[col] = merged[col].astype(int).astype("string")
    return merged

def duplicates_removed(columns_recasted: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate lab results."""
    columns_recasted.drop_duplicates(
        subset=["hospitalization_id", "lab_collect_dttm", "lab_result_dttm", 
                "lab_category", "lab_value_numeric"],
        inplace=True
    )
    return columns_recasted

@datasaver()
def save(duplicates_removed: pd.DataFrame) -> dict:
    save_to_rclif(duplicates_removed, "labs")
    
    metadata = {
        "table_name": "labs"
    }
    
    return metadata

def _main():
    from hamilton import driver
    import __main__
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

if __name__ == "__main__":
    _main()

# src/tables/labs.py
import numpy as np
import pandas as pd
import logging
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, check_duplicates 
    
LABS_COL_NAMES = [
    "hospitalization_id", "lab_order_dttm", "lab_collect_dttm", "lab_result_dttm", "lab_order_name",
    "lab_order_category", "lab_name", "lab_category", "lab_value", "lab_value_numeric",
    "reference_unit", "lab_specimen_name", "lab_specimen_category", "lab_loinc_code"
]

LABS_COL_RENAME_MAPPER = {
    "charttime": "lab_collect_dttm", "storetime": "lab_result_dttm", 
    "value": "lab_value", "valuenum": "lab_value_numeric", "valueuom": "reference_unit"
}
    
    
def main():
    logging.info("starting to build clif labs table -- ")
    labs_mapping = load_mapping_csv("labs")
    # drop the row corresponding to procalcitonin which is not available in MIMIC
    labs_mapping.dropna(subset = ["itemid"], inplace = True)
    labs_mapping["itemid"] = labs_mapping["itemid"].astype(int)
    labs_id_to_category_mapper = construct_mapper_dict(labs_mapping, "itemid", "lab_category")
    labs_items = labs_mapping.loc[
        labs_mapping["decision"].isin(["TO MAP, CONVERT UOM", "TO MAP, AS IS", "UNSURE"]),
        ["lab_category", "itemid", "label", "count"]
    ].copy()
    labs_id_to_name_mapper = dict(zip(labs_items['itemid'], labs_items['label']))

    logging.info("part 1: fetching from labevents table...")
    # labevents table has itemids with 5 digits
    labs_items_le = labs_items[labs_items['itemid'].astype("string").str.len() == 5]
    labs_events_le = fetch_mimic_events(labs_items_le['itemid'], original = True, for_labs = True)

    labs_events_le["lab_name"] = labs_events_le["itemid"].map(labs_id_to_name_mapper)
    labs_events_le["lab_category"] = labs_events_le["itemid"].map(labs_id_to_category_mapper)
    labs_events_le_c = rename_and_reorder_cols(labs_events_le, LABS_COL_RENAME_MAPPER, LABS_COL_NAMES + ["itemid"])

    logging.info("converting units of measurement...")
    # for ionized (free) calcium, to convert a result from mmol/L to mg/dL, multiply the mmol/L value by 4.  
    # https://www.abaxis.com/sites/default/files/resource-packages/Ionized%20Calcium%20CTI%20Sheete%20714179-00P.pdf
    labs_events_le_c["lab_value_numeric"] = np.where(
        labs_events_le_c["itemid"].isin([50808, 51624]),
        labs_events_le_c["lab_value_numeric"]*4,
        labs_events_le_c["lab_value_numeric"]
    )

    labs_events_le_c["reference_unit"] = np.where(
        labs_events_le_c["itemid"].isin([50808, 51624]),
        "mg/dL",
        labs_events_le_c["reference_unit"]
    )

    labs_events_le_c["lab_value"] = np.where(
        labs_events_le_c["itemid"].isin([50808, 51624]),
        labs_events_le_c["lab_value_numeric"].astype("string"),
        labs_events_le_c["lab_value"]
    )
    
    # convert troponin_t and troponin_i from ng/mL to ng/L, which is to multiply by 1000
    labs_events_le_c["lab_value_numeric"] = np.where(
        labs_events_le_c["itemid"].isin([51003, 52642]),
        labs_events_le_c["lab_value_numeric"]*1000,
        labs_events_le_c["lab_value_numeric"]
    )

    labs_events_le_c["reference_unit"] = np.where(
        labs_events_le_c["itemid"].isin([51003, 52642]),
        "ng/L",
        labs_events_le_c["reference_unit"]
    )

    labs_events_le_c["lab_value"] = np.where(
        labs_events_le_c["itemid"].isin([51003, 52642]),
        labs_events_le_c["lab_value_numeric"].astype("string"),
        labs_events_le_c["lab_value"]
    )
    
    logging.info("part 2: fetching from chartevents table...")
    # chartevents items has 6-digit item ids 
    labs_items_ce = labs_items[labs_items['itemid'].astype("string").str.len() == 6]
    labs_events_ce = fetch_mimic_events(labs_items_ce['itemid'], original = True, for_labs = False)
    labs_events_ce["lab_name"] = labs_events_ce["itemid"].map(labs_id_to_name_mapper)
    labs_events_ce["lab_category"] = labs_events_ce["itemid"].map(labs_id_to_category_mapper)
    labs_events_ce_c = rename_and_reorder_cols(labs_events_ce, LABS_COL_RENAME_MAPPER, LABS_COL_NAMES + ["itemid"])
        
    logging.info("concatenating lab events from labevents and chartevents...")
    labs_events_f = pd.concat([labs_events_le_c, labs_events_ce_c])
    labs_events_f.drop(columns = "itemid", inplace = True)
    
    logging.info("cleaning and recasting columns...")
    for col in labs_events_f.columns:
        # logging.info(f"processing column {col}")
        if "dttm" in col:
            labs_events_f[col] = pd.to_datetime(labs_events_f[col])
        elif ("order" in col) or ("specimen" in col) or ("loinc" in col):
            labs_events_f[col] = ""
        elif col == "hospitalization_id":
            labs_events_f[col] = labs_events_f[col].astype(int).astype("string")
        else:
            continue
    
    # save to an intermediate file for further eda
    # save_to_rclif(labs_events_f, "labs_intm")
    
    # TODO: more complex deduplication
    labs_events_f.drop_duplicates(
        subset = ["hospitalization_id", "lab_collect_dttm", "lab_result_dttm", "lab_category", "lab_value_numeric"],
        inplace = True)
    
    save_to_rclif(labs_events_f, "labs")
    logging.info("output saved to a parquet file, everything completed for the labs table!")
    
if __name__ == "__main__":
    main()

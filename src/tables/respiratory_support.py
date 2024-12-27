import numpy as np
import pandas as pd
import logging
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging    

setup_logging()

RESP_COLUMNS = [
    "hospitalization_id", "recorded_dttm", "device_name", "device_category", "vent_brand_name", 
    "mode_name", "mode_category", "tracheostomy", "fio2_set", "lpm_set",
    "tidal_volume_set", "resp_rate_set", "pressure_control_set", "pressure_support_set",
    "flow_rate_set", "peak_inspiratory_pressure_set", "inspiratory_time_set",
    "peep_set", "tidal_volume_obs", "resp_rate_obs", "plateau_pressure_obs",
    "peak_inspiratory_pressure_obs", "peep_obs", "minute_vent_obs", "mean_airway_pressure_obs"
    ]
RESP_DEVICE_RANK = ["IMV", "NIPPV", "CPAP", "High Flow NC", "Face Mask", "Trach Collar", "Nasal Cannula", "Room Air", "Other"]

def clean_fio2_set(value: float) -> float:
    '''
    ref: https://github.com/MIT-LCP/mimic-code/blob/e39825259beaa9d6bc9b99160049a5d251852aae/mimic-iv/concepts/measurement/bg.sql#L130
    '''
    if value >= 20 and value <= 100:
        return value/100
    elif value > 1 and value < 20:
        return np.nan
    elif value > 0.2 and value <= 1:
        return value
    else:
        return np.nan

def main():
    logging.info("starting to build clif respiratory support table.")
    # load mapping 
    resp_mapping = load_mapping_csv("respiratory_support")
    resp_device_mapping = load_mapping_csv("device_category")
    resp_mode_mapping = load_mapping_csv("mode_category")

    resp_mapper = construct_mapper_dict(resp_mapping, "itemid", "variable")
    resp_device_mapper = construct_mapper_dict(
        resp_device_mapping, "device_name", "device_category", excluded_item_ids = ["223848"]
        )
    resp_mode_mapper = construct_mapper_dict(resp_mode_mapping, "mode_name", "mode_category")
    
    logging.info("parsing the mapping files to identify relevant items and fetch relevant events.")
    resp_item_ids = get_relevant_item_ids(
        mapping_df = resp_mapping, decision_col = "variable" # , excluded_item_ids=[223848] # remove the vent brand name
        ) 
    resp_events = fetch_mimic_events(resp_item_ids)
    resp_events["variable"] = resp_events["itemid"].map(resp_mapper)

    # dedup - remove duplicates to prepare for pivoting 
    # two kinds of duplicates to handle: by devices and other
    resp_duplicates: pd.DataFrame = find_duplicates(resp_events)

    logging.info(f"identified {len(resp_duplicates)} 'duplicated' events to be cleaned.")
    logging.info("first, removing lower-ranked devices.")
    # 1/ deal with dups over devices
    resp_duplicates_devices: pd.DataFrame = resp_duplicates.query("itemid == 226732").copy()
    resp_duplicates_devices["device_category"] = resp_duplicates_devices["value"].apply(
        lambda x: resp_device_mapper[x.strip()] if pd.notna(x) else None
        )
    resp_duplicates_devices.dropna(subset="device_category",inplace=True)
    resp_duplicates_devices["rank"] = resp_duplicates_devices["device_category"].apply(
        lambda x: RESP_DEVICE_RANK.index(x.strip()))

    # deal with the device case - find indices to drop
    top_ranked_device_indices = resp_duplicates_devices.groupby(["hadm_id", "time", "itemid"])["rank"].idxmin()
    # non top-ranked categories to be dropped
    lower_ranked_device_indices = resp_duplicates_devices.index.difference(top_ranked_device_indices)
    # drop the designated indices
    resp_events_clean = resp_events.drop(lower_ranked_device_indices)
    # drop None
    resp_events_clean.dropna(subset = "value", inplace=True)

    logging.info("second, removing duplicated device reads.")
    # 2/ deal with duplicate vent reads:
    setting_duplicate_indices_to_drop = find_duplicates(resp_events_clean).query("stay_id == 36123037").index
    resp_events_clean.drop(setting_duplicate_indices_to_drop, inplace = True)
    resp_events_clean.drop_duplicates(subset = ["hadm_id", "time", "itemid"], inplace = True)
    # NOTE: this approach drop one observation that has conflicting value

    # create two columns based on item_id:  TODO: retire one
    # resp_events_clean["label"] = resp_events_clean["itemid"].map(item_id_to_label)
    resp_events_clean["variable"] = resp_events_clean["itemid"].map(resp_mapper)

    ### pivot and coalesce
    # this is for EDA
    # resp_wider_in_lables = resp_events_clean.pivot(
    #     index = ["hadm_id", "time"], 
    #     columns = ["variable", "label"],
    #     values = "value" 
    # )
    
    logging.info("pivoting to a wide format and coalescing 'duplicate' columns.")
    # this is for actually cleaning based on item ids
    resp_wider_in_ids = resp_events_clean.pivot(
        index = ["hadm_id", "time"], 
        columns = ["itemid"],
        values = "value" 
    ).reset_index()
    resp_wider_in_ids = convert_and_sort_datetime(resp_wider_in_ids)
    # implement the coalease logic
    resp_wider_in_ids["tracheostomy"] = resp_wider_in_ids[225448].fillna(resp_wider_in_ids[226237])
    resp_wider_in_ids["lpm_set"] = resp_wider_in_ids[223834].fillna(resp_wider_in_ids[227287])
    resp_wider_in_ids["tidal_volume_obs"] = (
        resp_wider_in_ids[224685].fillna(resp_wider_in_ids[224686]).fillna(resp_wider_in_ids[224421])
        )
    resp_wider_in_ids["resp_rate_set"] = resp_wider_in_ids[224688].fillna(resp_wider_in_ids[227581])
    resp_wider_in_ids["resp_rate_obs"] = resp_wider_in_ids[224690].fillna(resp_wider_in_ids[224422])
    resp_wider_in_ids["flow_rate_set"] = resp_wider_in_ids[224691].fillna(resp_wider_in_ids[227582])
    resp_wider_in_ids["peep_set"] = resp_wider_in_ids[220339].fillna(resp_wider_in_ids[227579])
    resp_wider_in_ids["mode_name"] = (
        resp_wider_in_ids[223849].fillna(resp_wider_in_ids[229314].fillna(resp_wider_in_ids[227577])) # FIXME
        )
    # remove duplicate variable columns that were coaleased into one
    resp_wider_cleaned = resp_wider_in_ids.drop(
        columns = [225448, 226237, 223834, 227287, 224685, 224686, 224421, 224688, 227581, 224690, 
                224422, 224691, 227582, 220339, 227579, 223849, 229314, 227577]
        )
    resp_wider_cleaned.rename(columns=resp_mapper, inplace = True)

    logging.info("mapping device and mode names to categories.")
    # map _name to _category
    resp_wider_cleaned["device_category"] = resp_wider_cleaned["device_name"].map(
        lambda x: resp_device_mapper[x.strip()] if pd.notna(x) else None
        )
    resp_wider_cleaned["mode_category"] = resp_wider_cleaned["mode_name"].map(resp_mode_mapper)

    logging.info("renaming, reordering, and casting columns.")
    resp_final = rename_and_reorder_cols(
        resp_wider_cleaned, 
        rename_mapper_dict = {"hadm_id": "hospitalization_id", "time": "recorded_dttm"}, 
        new_col_order = RESP_COLUMNS
    )
    # convert dtypes:
    resp_float_cols = [col for col in resp_final.columns if "_set" in col or "_obs" in col]
    resp_final["hospitalization_id"] = resp_final["hospitalization_id"].astype(str)
    # resp_final["tracheostomy"] = resp_final["tracheostomy"].astype(bool)
    for col_name in resp_float_cols:
        resp_final[col_name] = resp_final[col_name].astype(float)

    logging.info("cleaning up `tracheostomy` and `fio2_set`.")
    # processing fio2_set
    resp_final["fio2_set"] = resp_final["fio2_set"].apply(clean_fio2_set)
    resp_fc = resp_final.copy() # fc stands for final, cleaned
    resp_fc.rename(columns={"tracheostomy": "trach_performed"}, inplace=True)
    # processing trach
    resp_fc["trach_implied"] = (resp_fc["device_name"].isin(["Tracheostomy tube","Trach mask"])) | (resp_fc["trach_performed"] == 1)
    resp_fc['trach_bool'] = resp_fc.groupby('hospitalization_id')['trach_implied'].transform(
        lambda x: x.cumsum().astype(bool))
    resp_fcf = rename_and_reorder_cols(resp_fc, {"trach_bool": "tracheostomy"}, RESP_COLUMNS)
    
    logging.info("saving to a parquet file.")
    save_to_rclif(resp_fcf, "respiratory_support")

if __name__ == "__main__":
    main()

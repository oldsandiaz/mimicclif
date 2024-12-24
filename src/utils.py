# src/utils.py
import logging
import os
import pandas as pd
import json
from pathlib import Path
from functools import cache

def load_config():
    json_path = SCRIPT_DIR / '../config/config.json'
    with open(json_path, 'r') as file:
        config = json.load(file)
    print("Loaded configuration from config.json")
    
    return config

config = load_config()

# Cache to store loaded tables
TABLE_CACHE = {}
SCRIPT_DIR = Path(__file__).resolve().parent
CLIF_DATA_DIR_NAME = config["clif_data_dir_name"]
CLIF_VERSION = config["clif_version"]
# FIXME: delete "ALREDAY MAPPED" at some pt
EXCLUDED_LABELS_DEFAULT = ["NO MAPPING", "UNSURE", "MAPPED ELSEWHERE", "SPECIAL CASE", "ALREADY MAPPED"] 

# FIXME: change the input arg to config
def setup_logging(log_file: str = "logs/test.log"):
    """
    Sets up logging for the ETL pipeline.

    Args:
        log_file (str): Path to the log file. Default is "logs/pipeline.log".
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging initialized.")

# -----------
#     I/O 
# -----------

def load_mimic_table(
    module: {"icu", "hosp"}, table, file_type: {"csv", "parquet", "pq"} = None,
    mimic_version = "3.1"
):
    """
    Loads a MIMIC-IV table from the specified module and caches it to avoid reloading.

    Args:
        module (str): The MIMIC module (e.g., "icu", "hosp").
        table (str): The table name (e.g., "patients", "admissions").
        file_type (str): The file type to load (e.g., "csv", "parquet").
        mimic_version (str): Version of MIMIC-IV to use (default is "3.1").

    Returns:
        pd.DataFrame: The loaded table as a Pandas DataFrame.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If an unsupported file type is provided.
    """
    global TABLE_CACHE
    cache_key = f"{module}_{table}"

    # Check if table is already in cache
    if cache_key in TABLE_CACHE:
        logging.info(f"Using cached data for {table} from module {module}.")
        return TABLE_CACHE[cache_key]

    # Define file paths
    parquet_path = SCRIPT_DIR / f"../data/mimic-data/mimic-iv-{mimic_version}/{module}/{table}.parquet"
    csv_path = SCRIPT_DIR / f"../data/mimic-data/mimic-iv-{mimic_version}/{module}/{table}.csv.gz"

    if file_type is not None:
        if file_type in ["pq", "parquet"]:
            if os.path.exists(parquet_path):
                logging.info(f"Loading {table} from {parquet_path}.")
                table_df = pd.read_parquet(parquet_path)
            else:
                raise FileNotFoundError(f"Parquet file not found at {parquet_path}")
        elif file_type == "csv":
            if os.path.exists(csv_path):
                logging.info(f"Loading {table} from {csv_path}.")
                table_df = pd.read_csv(csv_path, low_memory=False)
            else:
                raise FileNotFoundError(f"CSV file not found at {csv_path}")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    else:
        if os.path.exists(parquet_path):
            logging.info(f"Reading already converted parquet file for {module}/{table}")
            table_df = pd.read_parquet(parquet_path, engine="auto")
        else:
            logging.info(f"No parquet file exists yet for {module}/{table}, so reading from csv")
            table_df = pd.read_csv(csv_path, low_memory=False)
            table_df.to_parquet(parquet_path)

    # Cache the table
    TABLE_CACHE[cache_key] = table_df
    return table_df

def load_mimic_tables(tables, cache = True):
    for module, table in tables:
        logging.info(f"Loading {table} from module {module}")
        var_name = table
        if (cache) and (var_name in globals()):
            logging.info(f"Table {table} from module {module} is already loaded in memory")
            continue
        try:
            globals()[var_name] = load_mimic_table(module, table)
            logging.info(f"Successfully loaded table {table} from module {module}")
        except Exception as e:
            logging.error(f"Error loading table: {table} from module: {module}. Error: {e}")

def save_to_rclif(df: pd.DataFrame, table_name: str):
    global CLIF_DATA_DIR_NAME
    if not CLIF_DATA_DIR_NAME:
        # if it is an empty str (not specified by user), use the default syntax 
        CLIF_DATA_DIR_NAME = f"rclif-{CLIF_VERSION}"
    output_path = SCRIPT_DIR / f'../data/{CLIF_DATA_DIR_NAME}/clif_{table_name}.parquet'
    # e.g. '../data/rclif-2.0/clif_adt.parquet'
    return df.to_parquet(output_path, index = False)
        
def read_from_rclif(table_name, file_format = "pq"):
    # FIXME: update this when validation / testing
    if file_format in ["pq", "parquet"]:
        return pd.read_parquet(SCRIPT_DIR / f'../rclif/clif_{table_name}.parquet')

def load_mapping_csv(csv_name: str, dtype = None):
    return pd.read_csv(
        SCRIPT_DIR / f"../data/mappings/mimic-to-clif-mappings - {csv_name}.csv", dtype = dtype
        )

def construct_mapper_dict(
    mapping_df: pd.DataFrame, key_col: str, value_col: str, map_none_to_none = False,
    excluded_item_ids: list = None
    ):
    '''
    covert to a dict for df col renaming later
    '''
    if not excluded_item_ids:
        excluded_item_ids = []
    
    if "itemid" in mapping_df.columns:
        mapping_df = mapping_df.loc[
            ~mapping_df["itemid"].isin(excluded_item_ids)
            , 
            ]
    
    mapper_dict = dict(zip(mapping_df[key_col], mapping_df[value_col]))
    
    # Replace "NO MAPPING" with NA
    for key, value in mapper_dict.items():
        if value == "NO MAPPING":
            mapper_dict[key] = None
    
    # to enable a None -> None mapping
    if map_none_to_none:
        mapper_dict[None] = None
        
    return mapper_dict

# -------
#   ETL
# -------

def convert_and_sort_datetime(df: pd.DataFrame, additional_cols: list[str] = None):
    if not additional_cols:
        additional_cols = []
    # for procedure events
    if "starttime" in df.columns and "endtime" in df.columns:
        df["starttime"] = pd.to_datetime(df["starttime"])
        df["endtime"] = pd.to_datetime(df["endtime"])
        ordered_cols = ["hadm_id", "starttime", "endtime", "storetime"] + additional_cols
        df = df.sort_values(ordered_cols).reset_index(drop = True).reset_index()
    # for chart events
    elif "charttime" in df.columns:
        df["charttime"] = pd.to_datetime(df['charttime'])
        ordered_cols = ["hadm_id", "charttime", "storetime"] + additional_cols
        df = df.sort_values(ordered_cols).reset_index(drop = True).reset_index()
    elif "time" in df.columns:
        df["time"] = pd.to_datetime(df['time'])
        ordered_cols = ["hadm_id", "time"] + additional_cols
        df = df.sort_values(ordered_cols)
    return df

# find all the relevant item ids for a table
def get_relevant_item_ids(mapping_df: pd.DataFrame, decision_col: str, 
                          excluded_labels: list = EXCLUDED_LABELS_DEFAULT,
                          excluded_item_ids: list = None
                          ):
    '''
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
    
def rename_and_reorder_cols(df, rename_mapper_dict: dict, new_col_order: list) -> pd.DataFrame:
    baseline_rename_mapper = {
        "subject_id": "patient_id", "hadm_id": "hospitalization_id",
    }
    
    return (
        df.rename(columns = baseline_rename_mapper | rename_mapper_dict)
        .reindex(columns = new_col_order)
        )

def find_duplicates(df: pd.DataFrame, cols: list[str] = ["hadm_id", "time", "itemid"]):
    '''
    Check whether there are duplicates -- more than one populated value -- for what is supposed to be 
    unique combination of columns. That is, for the same measured variable (e.g. vital_category) at
    the same time during the same encounter, there should be only one corresponding value.
    
    Use this in pre-CLIFing EDA.
    '''
    return df[df.duplicated(subset = cols, keep = False)]

def check_duplicates(df: pd.DataFrame, additional_cols: list = None):
    '''
    Check whether there are duplicates -- more than one populated value -- for what is supposed to be 
    unique combination of columns. That is, for the same measured variable (e.g. vital_category) at
    the same time during the same hospitalization, there should be only one corresponding value.
    
    Use this in post-CLIFing validation.
    '''
    if not additional_cols:
        additional_cols = []
    cols_to_check = ["hospitalization_id", "recorded_dttm"] + additional_cols
    return df[df.duplicated(subset = cols_to_check, keep = False)]

@cache()
def item_id_to_feature_value(item_id: int, col: str = "label", df = d_items):
    '''
    Find the corresponding feature value of an item by id.
    i.e. find the label, or linksto, of item id 226732.
    '''
    row = df.loc[df["itemid"] == item_id, :]
    label = row["label"].values[0]
    if col == "label":
        logging.info(f"the {col} for item {item_id} is {label}")
        return label
    else:
        feature_value = row[col].values[0]
        logging.info(f"the {col} for item {item_id} ({label}) is {feature_value}")
        return feature_value

@cache()
def item_id_to_label(item_id: int) -> str:
    '''
    Helper function that returns the "label" string of an item given its item_id. 
    '''
    return item_id_to_feature_value(item_id)

def item_id_to_events_df(item_id: int, original: bool = False) -> pd.DataFrame:
    '''
    Return in a pandas df all the events associated with an item id.
    - original: whether to return the original df (True), or a simplified one 
    with some columns (particulary timestamps) renamed to support integration 
    between different events df.  
    '''
    # find whether it is chartevents, or procedure events, etc.
    linksto_table_name = item_id_to_feature_value(item_id, col = "linksto")
    # turn string into a dj object
    linksto_df: pd.DataFrame = globals()[linksto_table_name]
    events_df = linksto_df.loc[linksto_df["itemid"] == item_id, :]
    # return the original columns
    if original:
        return events_df
    # else = if not original, then return the simplified version
    elif linksto_table_name == "procedureevents": # FIXME: trach is complex and need additional attention
        events_df_simplified = events_df.loc[
            :, ['subject_id', 'hadm_id', 'stay_id', 'endtime', 'itemid', 'value', 'valueuom']
        ].rename(columns = {"endtime": "time"})
        return events_df_simplified
    elif linksto_table_name == "chartevents":
        events_df_simplified = events_df.loc[
            :, ['subject_id', 'hadm_id', 'stay_id', 'charttime', 'itemid', 'value', 'valueuom']
        ].rename(columns = {"charttime": "time"})
        return events_df_simplified
    # FIXME: likely an issue if data struct of different events table are different 

def item_ids_list_to_events_df(item_ids: list, original = False):
    df_list = [item_id_to_events_df(item_id, original = original) for item_id in item_ids]
    df_merged = pd.concat(df_list) #.head().assign(
        ## linksto = lambda df: df["itemid"].apply(lambda item_id: item_id_to_feature_value(item_id, col = "linksto"))
    # )
    return df_merged 
    # FIXME: automatically add the label and linksto table source columns -- create cache?

def item_finder_to_events(items: pd.DataFrame):
    items = items.dropna(subset = ["count"])
    itemids = items["itemid"].tolist()
    # itemid_to_label_mapper = dict(zip(items["itemid"], items["label"]))
    events = item_ids_list_to_events_df(itemids)
    events["label"] = events["itemid"].apply(item_id_to_label)
    return events

class ItemFinder():
    '''
    TODO likely replacing this with duckdb sql queries
    '''
    def __init__(self, kw = None, items_df = d_items, 
                 col: str = "label", case_sensitive: bool = False, 
                 for_labs: bool = False, report_na = True
                 ) -> pd.DataFrame:
        '''
        Look up an item by keyword from the `d_items` table of the `icu` module.
        - case: whether the search is case sensitive
        - report_na: whether to print when there is no match; or simply return a 
        '''
        self.kw = kw 
        self.df = items_df
        self.col = "abbreviation" if col == "abbr" else col
        self.for_labs = for_labs

        # df of items that match the key words -- a raw output
        self.items_select_df: pd.DataFrame = items_df[
            items_df[self.col].str.contains(kw, case = case_sensitive, na = False)
        ]
        
        # first check whether there is any return in the raw output
        if len(self.items_select_df) == 0:
            if report_na:
                raise Exception(f"No matching result found in column {col} with case sensitive being {case_sensitive}")
            else:
                logging.warning(f"No matching result for {kw} in column {col} with case sensitive being {case_sensitive}")
                self.candidate_table = pd.DataFrame()
        
        # ... only proceed when the return is not of zero length
        # and enhance the simple raw output with counts and value instances
        else:
            logging.info(f"{len(self.items_select_df)} matching item(s) found for {self.kw}.")
            # list of ids for items that match the key words
            self.items_select_ids = self.items_select_df["itemid"].values
            # a np array of non-duplicated events table names, e.g. ["chartevents", "procedureevents"]
            self.linksto_table_names = self.items_select_df["linksto"].unique()
            self.item_freq = self.generate_item_freq()
            # logging.info(f"type is {type(self.item_freq)}")
            self.candidate_table = self.make_candidate_table()

    def generate_item_freq(self):
        '''
        Iterative over each events table, find the items freq therein, and combine into one df.
        # FIXME - should maybe make this map style without loop
        '''
        freq_df_ls = [] # a list of df's
        for table_name in self.linksto_table_names:
            # fetch the object by name str, i.e. chartevents, procedureevents, etc.
            events_df: pd.DataFrame = globals()[table_name]
            # filter for all the selected events in that events table
            events_select_df = events_df.loc[
                events_df["itemid"].isin(self.items_select_ids), :
            ]
            # a df of item freq for one event type  
            item_freq = events_select_df.value_counts("itemid")
            item_freq.name = "count"

            # check if the df is empty -- there shouldn't be an empty one FIXME
            # if not item_freq_df.empty:
            freq_df_ls.append(item_freq)
        
        return pd.concat(freq_df_ls)

    def make_candidate_table(self):
        '''
        merge item freq and values instances to the raw output to generate the enhanced table of the 
        candidate items.
        '''
        cand_table = (
            self.items_select_df
            .loc[:, ["itemid", "label", "abbreviation", "linksto", "category", "unitname", "param_type"]]
            # FIXME
            .join(self.item_freq, on = "itemid", validate = "1:1")
            .sort_values(by = "count", ascending = False) 
            .assign(
                value_instances = lambda df: df["itemid"].apply(item_id_to_value_instances)
            )
        )
        if self.for_labs:
            return cand_table.reindex(
                columns = ["itemid", "label", "abbreviation", "linksto", "category", "count", "value_instances", "unitname"]
                )
        else:
            return cand_table

@cache()
def item_id_to_value_instances(item_id: int):
    '''
    Wrapper
    '''
    label = item_id_to_feature_value(item_id, "label")

    param_type = item_id_to_feature_value(item_id, "param_type")
    
    if param_type == "Numeric":
        val_instances = item_id_to_value_instances_numeric(item_id)
    elif param_type == "Text":
        val_instances = item_id_to_value_instances_categorical(item_id).to_dict()
    else:
        return param_type
    print(f"item label: {label}; value instances: {str(val_instances)}")
    return str(val_instances)

def item_id_to_value_instances_categorical(item_id: int, events: pd.DataFrame = chartevents):
    '''
    Return all the unique categories
    '''
    assoc_events = events.loc[events["itemid"] == item_id, :]
    categories: pd.Series = assoc_events.value_counts("value") 
    return categories
    
def item_id_to_value_instances_numeric(item_id: int, events: pd.DataFrame = chartevents):
    '''
    Find max, min, mean of a continuous, or numeric, item.
    '''
    valuenum_col = events.loc[events["itemid"] == item_id, :]["valuenum"]
    val_max, val_min, val_mean = valuenum_col.max(), valuenum_col.min(), round(valuenum_col.mean(), 2)
    return f"Max: {val_max}, Min: {val_min}, Mean: {val_mean}"


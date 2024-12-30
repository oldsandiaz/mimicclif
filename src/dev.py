from importlib import reload
import pandas as pd
import src.utils
from src.utils import setup_logging, fetch_mimic_events, get_relevant_item_ids, construct_mapper_dict, \
    load_mapping_csv, resave_mimic_table_from_csv_to_parquet, resave_select_mimic_tables_from_csv_to_parquet

setup_logging()

resave_select_mimic_tables_from_csv_to_parquet(tables = ["d_items", "chartevents"], overwrite = True)

chartevents = pd.read_parquet(src.utils.mimic_table_pathfinder("chartevents"))
d_items = pd.read_parquet(src.utils.mimic_table_pathfinder("d_items"))

resp_mapping = load_mapping_csv("respiratory_support")
resp_device_mapping = load_mapping_csv("device_category")
resp_mode_mapping = load_mapping_csv("mode_category")

resp_mapper_dict = construct_mapper_dict(resp_mapping, "itemid", "variable")
resp_device_mapper_dict = construct_mapper_dict(
    resp_device_mapping, "device_name", "device_category", excluded_item_ids = ["223848"]
    )
resp_mode_mapper_dict = construct_mapper_dict(resp_mode_mapping, "mode_name", "mode_category")

resp_item_ids = get_relevant_item_ids(
    mapping_df = resp_mapping, decision_col = "variable" # , excluded_item_ids=[223848] # remove the vent brand name
        ) 

# item_ids = [226732]
df = fetch_mimic_events(resp_item_ids)

# ------------------------------------------------------------------------------------------------
# use duckdb to resave csv into parquet
mimic_version, module, table = "3.1", "icu", "chartevents"
parquet_path = SCRIPT_DIR / f"../data/mimic-data/mimic-iv-{mimic_version}/{module}/{table}.csv.gz"
con = duckdb.connect()
# con.read_csv(str(parquet_path))

query = f"""
COPY (SELECT * FROM read_csv_auto('{str(parquet_path)}'))
TO 'test_output.parquet' (FORMAT 'PARQUET');
"""
con.execute(query)

# df = con.sql(query).fetchdf()

# ------------------------------------------------------------------------------------------------

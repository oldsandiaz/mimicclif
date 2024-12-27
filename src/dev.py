from src.utils import setup_logging, fetch_mimic_events, get_relevant_item_ids, construct_mapper_dict, load_mapping_csv

setup_logging()
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
from importlib import reload
import pandas as pd
import src.utils
reload(src.utils)
from src.utils import setup_logging, mimic_table_pathfinder, fetch_mimic_events, get_relevant_item_ids, construct_mapper_dict, \
    load_mapping_csv, resave_mimic_table_from_csv_to_parquet, resave_select_mimic_tables_from_csv_to_parquet, \
    generate_item_stats_by_eventtable, search_mimic_items, ItemFinder
search_mimic_items(kw = "Propofol")

df = generate_item_stats_by_eventtable(item_ids = [227523, 221744, 225942], table_name = "inputevents")
df


# search_mimic_items(kw = "temp")
search_mimic_items(kw = "dopamine")
search_mimic_items(kw = "fentanyl")

setup_logging()

eg_item_ids = [226732, 223848, 223849, 223835]

df = generate_item_stats_by_eventtable(item_ids = eg_item_ids, table_name = "chartevents")
df["value_instances"].to_dict()


resp_item_ids = [226732, 223835, 220339, 224685, 224687, 224697, 224695, 223834,
       223848, 224690, 223849, 224688, 224684, 224738, 224686, 224701,
       224696, 229314, 224691, 224700, 225448, 226237, 227287, 224422,
       224421, 227577, 227579, 227580, 227582, 224702, 227581]

resp_item_ids_chartevents_only = [226732, 223835, 220339, 224685, 224687, 224697, 224695, 223834,
       223848, 224690, 223849, 224688, 224684, 224738, 224686, 224701,
       224696, 229314, 224691, 224700, 227287, 224422,
       224421, 227577, 227579, 227580, 227582, 224702, 227581]

# ------------------------------------------------------------------------------------------------

resave_select_mimic_tables_from_csv_to_parquet(tables = ["d_items", "chartevents"], overwrite = True)

chartevents = pd.read_parquet(src.utils.mimic_table_pathfinder("chartevents"))

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

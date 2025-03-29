# src/tables/labs.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, List
from importlib import reload
import src.tables.base
# reload(src.tables.base)
from src.tables.base import MimicToClifBasePipeline
from src.utils import (
    construct_mapper_dict, fetch_mimic_events, load_mapping_csv,
    get_relevant_item_ids, rename_and_reorder_cols, save_to_rclif
)

class LabsPipeline(MimicToClifBasePipeline):
    """Pipeline to create the CLIF labs table from MIMIC-IV."""
    
    # Constants
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
    
    def __init__(self):
        super().__init__(clif_table_name="labs")
        
    def extract(self):
        """Extract labs data from MIMIC-IV tables."""
        logging.info("Starting data extraction")
        # Load and prepare mapping
        labs_mapping = load_mapping_csv("labs")
        # drop the row corresponding to procalcitonin which is not available in MIMIC
        labs_mapping.dropna(subset=["itemid"], inplace=True)
        labs_mapping["itemid"] = labs_mapping["itemid"].astype(int)
        
        # Create mappers
        self.id_to_name_mapper = construct_mapper_dict(
            labs_mapping, "itemid", "label", 
            excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
        )
        self.id_to_category_mapper = construct_mapper_dict(
            labs_mapping, "itemid", "lab_category", 
            excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
        )
        
        # Filter relevant items
        labs_items = labs_mapping.loc[
            labs_mapping["decision"].isin(["TO MAP, CONVERT UOM", "TO MAP, AS IS", "UNSURE"]),
            ["lab_category", "itemid", "label", "count"]
        ].copy()
        
        self.logger.info("Extracting from labevents table")
        # labevents table has itemids with 5 digits
        labs_items_le = labs_items[labs_items['itemid'].astype("string").str.len() == 5]
        df_le = fetch_mimic_events(labs_items_le['itemid'], original=True, for_labs=True)
        
        self.logger.info("Extracting from chartevents table")
        # chartevents table has itemids with 6 digits
        labs_items_ce = labs_items[labs_items['itemid'].astype("string").str.len() == 6]
        df_ce = fetch_mimic_events(labs_items_ce['itemid'], original=True, for_labs=False)
        
        self.data = {
            'df_le': df_le,
            'df_ce': df_ce,
            'labs_items': labs_items
        }
    
    def transform(self) -> pd.DataFrame:
        """Transform the extracted lab data."""
        
        # first process the labevents data
        df_le_c = self._transform_le(self.data['df_le'])
        
        # then process the chartevents data
        df_ce_c = self._transform_ce(self.data['df_ce'])
        
        # merge
        df_m = pd.concat([df_le_c, df_ce_c])
        df_m.drop(columns = "itemid", inplace = True)
        
        # final clean
        df_f = self._cast_columns(df_m)
        df_f = self._remove_duplicates(df_f)
        
        self.data['df_f'] = df_f

        return df_f

    def _transform_le(self, df_le: pd.DataFrame):
        """Process the labs data from the labevents table."""
        df_le = self._map_names_and_categories(df_le)
        df_le = rename_and_reorder_cols(df_le, self.COL_RENAME_MAPPER, self.COL_NAMES + ["itemid"])
        df_le = self._convert_units(df_le)
        return df_le
 
    def _transform_ce(self, df_ce: pd.DataFrame):
        """Process the labs data from the chartevents table."""
        df_ce = self._map_names_and_categories(df_ce)
        df_ce = rename_and_reorder_cols(df_ce, self.COL_RENAME_MAPPER, self.COL_NAMES + ["itemid"])
        return df_ce
 
    def _map_names_and_categories(self, df: pd.DataFrame):
        """Add lab names and categories to the dataframe."""
        df["lab_name"] = df["itemid"].map(self.id_to_name_mapper)
        df["lab_category"] = df["itemid"].map(self.id_to_category_mapper)
        return df
         
    def _convert_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert units of measurement for specific lab items."""
        # for ionized (free) calcium, to convert a result from mmol/L to mg/dL, multiply the mmol/L value by 4.  
        # https://www.abaxis.com/sites/default/files/resource-packages/Ionized%20Calcium%20CTI%20Sheete%20714179-00P.pdf
        mask_ca = df["itemid"].isin([50808, 51624])
        df.loc[mask_ca, "lab_value_numeric"] *= 4
        df.loc[mask_ca, "reference_unit"] = "mg/dL"
        df.loc[mask_ca, "lab_value"] = df.loc[mask_ca, "lab_value_numeric"].astype("string")
        
        # convert troponin_t and troponin_i from ng/mL to ng/L, which is to multiply by 1000
        mask_trop = df["itemid"].isin([51003, 52642])
        df.loc[mask_trop, "lab_value_numeric"] *= 1000
        df.loc[mask_trop, "reference_unit"] = "ng/L"
        df.loc[mask_trop, "lab_value"] = df.loc[mask_trop, "lab_value_numeric"].astype("string")
        
        return df
    
    def _cast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and recast columns to appropriate data types."""
        for col in df.columns:
            if "dttm" in col:
                df[col] = pd.to_datetime(df[col])
            elif any(x in col for x in ["order", "specimen", "loinc"]):
                df[col] = ""
            elif col == "hospitalization_id":
                df[col] = df[col].astype(int).astype("string")
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate lab results."""
        df.drop_duplicates(
            subset=["hospitalization_id", "lab_collect_dttm", "lab_result_dttm", 
                   "lab_category", "lab_value_numeric"],
            inplace=True
        )
        return df

def main():
    """Main entry point for the labs pipeline."""
    pipeline = LabsPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()

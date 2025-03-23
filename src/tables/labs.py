# src/tables/labs.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, List
from .base import MimicToClifBasePipeline
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
        
    @MimicToClifBasePipeline._track_step
    def extract(self) -> Dict[str, pd.DataFrame]:
        """Extract lab data from MIMIC-IV tables."""
        self.logger.info("Starting data extraction")
        
        # Load and prepare mapping
        labs_mapping = load_mapping_csv("labs")
        # drop the row corresponding to procalcitonin which is not available in MIMIC
        labs_mapping.dropna(subset=["itemid"], inplace=True)
        labs_mapping["itemid"] = labs_mapping["itemid"].astype(int)
        
        # Create mappers
        self.labs_id_to_name_mapper = construct_mapper_dict(
            labs_mapping, "itemid", "label", 
            excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
        )
        self.labs_id_to_category_mapper = construct_mapper_dict(
            labs_mapping, "itemid", "lab_category", 
            excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
        )
        
        # Filter relevant items
        labs_items = labs_mapping.loc[
            labs_mapping["decision"].isin(["TO MAP, CONVERT UOM", "TO MAP, AS IS", "UNSURE"]),
            ["lab_category", "itemid", "label", "count"]
        ].copy()
        
        # Extract from labevents
        self.logger.info("Extracting from labevents table")
        # labevents table has itemids with 5 digits
        labs_items_le = labs_items[labs_items['itemid'].astype("string").str.len() == 5]
        labs_events_le = fetch_mimic_events(labs_items_le['itemid'], original=True, for_labs=True)
        
        # Extract from chartevents
        self.logger.info("Extracting from chartevents table")
        # chartevents table has itemids with 6 digits
        labs_items_ce = labs_items[labs_items['itemid'].astype("string").str.len() == 6]
        labs_events_ce = fetch_mimic_events(labs_items_ce['itemid'], original=True, for_labs=False)
        
        self.data = {
            'labs_events_le': labs_events_le,
            'labs_events_ce': labs_events_ce,
            'labs_items': labs_items
        }
        return self.data
    
    @MimicToClifBasePipeline._track_step
    def transform(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Transform the extracted lab data."""
        self.logger.info("Starting data transformation")
        
        # Transform labevents data
        labs_events_le = data['labs_events_le']
        labs_events_le = self._add_names_and_categories(labs_events_le)
        
        labs_events_le_c = rename_and_reorder_cols(
            labs_events_le,
            self.COL_RENAME_MAPPER,
            self.COL_NAMES + ["itemid"]
        )
        
        # Convert units of measurement
        labs_events_le_c = self._convert_units(labs_events_le_c)
        
        # Transform chartevents data
        labs_events_ce = data['labs_events_ce']
        labs_events_ce = self._add_names_and_categories(labs_events_ce)
        
        labs_events_ce_c = rename_and_reorder_cols(
            labs_events_ce,
            self.COL_RENAME_MAPPER,
            self.COL_NAMES + ["itemid"]
        )
        
        # Combine and clean data
        self.logger.info("Combining and cleaning data")
        labs_events_f = pd.concat([labs_events_le_c, labs_events_ce_c])
        labs_events_f.drop(columns="itemid", inplace=True)
        
        # Clean and recast columns
        labs_events_f = self._clean_columns(labs_events_f)
        
        # Save intermediate result for debugging
        save_to_rclif(labs_events_f, "labs_intm")
        
        # Remove duplicates
        labs_events_f = self._remove_duplicates(labs_events_f)
        
        self.data = labs_events_f
        return self.data
    
    def _map_names_and_categories(self):
        """Map to lab names and categories for both labs events from labevents table and chartevents table."""
        # process labevents
        labs_events_le: pd.DataFrame = self.data['labs_events_le']
        labs_events_le["lab_name"] = labs_events_le["itemid"].map(self.labs_id_to_name_mapper)
        labs_events_le["lab_category"] = labs_events_le["itemid"].map(self.labs_id_to_category_mapper)
        labs_events_le_c = rename_and_reorder_cols(labs_events_le, self.COL_RENAME_MAPPER, self.COL_NAMES + ["itemid"])

        # process chartevents
        labs_events_ce: pd.DataFrame = self.data['labs_events_ce']
        labs_events_ce["lab_name"] = labs_events_ce["itemid"].map(self.labs_id_to_name_mapper)
        labs_events_ce["lab_category"] = labs_events_ce["itemid"].map(self.labs_id_to_category_mapper)
        labs_events_ce_c = rename_and_reorder_cols(labs_events_ce, self.COL_RENAME_MAPPER, self.COL_NAMES + ["itemid"])

        # update the data attribute
        self.data['labs_events_le'] = labs_events_le
        self.data['labs_events_ce'] = labs_events_ce
        self.data['labs_events_le_c'] = labs_events_le_c
        self.data['labs_events_ce_c'] = labs_events_ce_c
        return
        
    @MimicToClifBasePipeline._track_step
    def _convert_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert units of measurement for specific lab values."""
        # Convert ionized calcium from mmol/L to mg/dL
        mask_ca = df["itemid"].isin([50808, 51624])
        df.loc[mask_ca, "lab_value_numeric"] *= 4
        df.loc[mask_ca, "reference_unit"] = "mg/dL"
        df.loc[mask_ca, "lab_value"] = df.loc[mask_ca, "lab_value_numeric"].astype("string")
        
        # Convert troponin from ng/mL to ng/L
        mask_trop = df["itemid"].isin([51003, 52642])
        df.loc[mask_trop, "lab_value_numeric"] *= 1000
        df.loc[mask_trop, "reference_unit"] = "ng/L"
        df.loc[mask_trop, "lab_value"] = df.loc[mask_trop, "lab_value_numeric"].astype("string")
        
        return df
    
    @MimicToClifBasePipeline._track_step
    def _clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and recast columns to appropriate data types."""
        for col in df.columns:
            if "dttm" in col:
                df[col] = pd.to_datetime(df[col])
            elif any(x in col for x in ["order", "specimen", "loinc"]):
                df[col] = ""
            elif col == "hospitalization_id":
                df[col] = df[col].astype(int).astype("string")
        return df
    
    @MimicToClifBasePipeline._track_step
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

# src/tables/respiratory_support.py
import numpy as np
import pandas as pd
import logging
from importlib import reload
import src.utils
import duckdb

# reload(src.utils)

import src.tables.base

# reload(src.tables.base)
from src.tables.base import MimicToClifBasePipeline, intm_store_in_dev

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
)

setup_logging()


class RespPipeline(MimicToClifBasePipeline):
    """Pipeline to create the CLIF respiratory support table from MIMIC-IV."""

    # Constants
    RESP_COLUMNS = [
        "hospitalization_id",
        "recorded_dttm",
        "device_name",
        "device_category",
        "vent_brand_name",
        "mode_name",
        "mode_category",
        "tracheostomy",
        "fio2_set",
        "lpm_set",
        "tidal_volume_set",
        "resp_rate_set",
        "pressure_control_set",
        "pressure_support_set",
        "flow_rate_set",
        "peak_inspiratory_pressure_set",
        "inspiratory_time_set",
        "peep_set",
        "tidal_volume_obs",
        "resp_rate_obs",
        "plateau_pressure_obs",
        "peak_inspiratory_pressure_obs",
        "peep_obs",
        "minute_vent_obs",
        "mean_airway_pressure_obs",
    ]

    RESP_DEVICE_RANK = [
        "IMV",
        "NIPPV",
        "CPAP",
        "High Flow NC",
        "Face Mask",
        "Trach Collar",
        "Nasal Cannula",
        "Room Air",
        "Other",
    ]

    def __init__(self, dev_mode: bool = True):
        super().__init__(clif_table_name="respiratory_support")
        self.dev_mode = dev_mode
        self.data = {}

    @intm_store_in_dev
    def extract(self):
        """Extract respiratory support data from MIMIC-IV tables."""
        logging.info(
            "starting to build clif respiratory support table -- extracting source data from MIMIC-IV..."
        )
        # load mapping
        resp_mapping = load_mapping_csv("respiratory_support")
        resp_device_mapping = load_mapping_csv("device_category")
        resp_mode_mapping = load_mapping_csv("mode_category")

        self.resp_mapper = construct_mapper_dict(
            resp_mapping,
            key_col="itemid",
            value_col="variable",
            map_none_to_none=True,
            decision_col="variable",
        )
        self.resp_device_mapper = construct_mapper_dict(
            mapping_df=resp_device_mapping,
            key_col="device_name",
            value_col="device_category",
            map_none_to_none=True,
            excluded_item_ids=["223848"],
            decision_col="device_category",
        )
        self.resp_mode_mapper = construct_mapper_dict(
            mapping_df=resp_mode_mapping,
            key_col="mode_name",
            value_col="mode_category",
            map_none_to_none=False,
            decision_col="mode_category",
        )

        logging.info(
            "parsing the mapping files to identify relevant items and fetch corresponding events..."
        )
        resp_item_ids = get_relevant_item_ids(
            mapping_df=resp_mapping,
            decision_col="variable",  # , excluded_item_ids=[223848] # remove the vent brand name
        )
        resp_events = fetch_mimic_events(resp_item_ids)
        resp_events["variable"] = resp_events["itemid"].map(self.resp_mapper)

        return resp_events

    @intm_store_in_dev
    def transform(self, df=None):
        """Transform the extracted respiratory support data."""
        # Get the input data
        df_in: pd.DataFrame = self.data[self.extract.__name__] if df is None else df

        # Chain the transformations
        df_out = (
            df_in.pipe(self._remove_none_value_rows)
            .pipe(self._clean_fio2_set)
            .pipe(self._remove_duplicates)
            .pipe(self._pivot_and_coalesce)
            .pipe(self._rename_reorder_recast_cols)
            .pipe(self._clean_tracheostomy)
        )
        return df_out

    @intm_store_in_dev
    def _remove_none_value_rows(self, df: pd.DataFrame = None):
        """Remove rows where value is the string 'None'."""
        logging.info("removing rows where O2 Delivery Device(s) is the string 'None'...")
        mask = df["value"] == "None"
        none_value_rows = df[mask] # df to drop
        if (
            none_value_rows["itemid"].nunique() == 1
            and none_value_rows["itemid"].iloc[0] == 226732
        ):
            # drop all rows where value is the string 'None'
            return df[~mask]
        else:
            raise ValueError(
                "The rows with 'None' value have itemid other than 226732 (O2 Delivery Device(s))."
            )

    @intm_store_in_dev
    def _remove_duplicates(self, df: pd.DataFrame = None):
        """Remove duplicates to support long-to-wide pivoting.
        two kinds of duplicates to handle: by devices and other
        """
        resp_events: pd.DataFrame = df
        resp_duplicates: pd.DataFrame = find_duplicates(resp_events)

        logging.info(
            f"identified {len(resp_duplicates)} 'duplicated' events to be cleaned."
        )
        logging.info(
            "removing the first type of duplicates: lower-ranked 'duplicated' devices..."
        )
        # 1/ deal with dups over devices
        resp_duplicates_devices: pd.DataFrame = resp_duplicates.query(
            "itemid == 226732"
        ).copy()
        resp_duplicates_devices["device_category"] = resp_duplicates_devices[
            "value"
        ].apply(lambda x: self.resp_device_mapper[x.strip()] if pd.notna(x) else None)
        resp_duplicates_devices.dropna(subset="device_category", inplace=True)
        resp_duplicates_devices["rank"] = resp_duplicates_devices[
            "device_category"
        ].apply(lambda x: self.RESP_DEVICE_RANK.index(x.strip()))

        # deal with the device case - find indices to drop
        top_ranked_device_indices = resp_duplicates_devices.groupby(
            ["hadm_id", "time", "itemid"]
        )["rank"].idxmin()
        # non top-ranked categories to be dropped
        lower_ranked_device_indices = resp_duplicates_devices.index.difference(
            top_ranked_device_indices
        )
        # drop the designated indices
        resp_events_clean = resp_events.drop(lower_ranked_device_indices)
        # drop None
        resp_events_clean.dropna(subset="value", inplace=True)

        logging.info(
            "removing the second type of duplicates: duplicated device reads..."
        )
        # 2/ deal with duplicate vent reads:
        setting_duplicate_indices_to_drop = (
            find_duplicates(resp_events_clean).query("stay_id == 36123037").index
        )
        resp_events_clean.drop(setting_duplicate_indices_to_drop, inplace=True)
        resp_events_clean.drop_duplicates(
            subset=["hadm_id", "time", "itemid"], inplace=True
        )
        # NOTE: this approach drop one observation that has conflicting value

        # create two columns based on item_id:  TODO: retire one
        # resp_events_clean["label"] = resp_events_clean["itemid"].map(item_id_to_label)
        resp_events_clean["variable"] = resp_events_clean["itemid"].map(
            self.resp_mapper
        )

        return resp_events_clean

    @intm_store_in_dev
    def _pivot_and_coalesce(self, df: pd.DataFrame = None):
        logging.info("pivoting to a wide format and coalescing duplicate columns...")
        resp_events_clean = df
        # this is for EDA
        # resp_wider_in_lables = resp_events_clean.pivot(
        #     index = ["hadm_id", "time"],
        #     columns = ["variable", "label"],
        #     values = "value"
        # )

        # this is for actually cleaning based on item ids
        resp_wider_in_ids = (
            resp_events_clean.pivot(
                index=["hadm_id", "time"], columns=["itemid"], values="value"
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        resp_wider_in_ids = convert_and_sort_datetime(resp_wider_in_ids)
        # implement the coalease logic
        resp_wider_in_ids["tracheostomy"] = resp_wider_in_ids[225448].fillna(
            resp_wider_in_ids[226237]
        )
        resp_wider_in_ids["lpm_set"] = resp_wider_in_ids[223834].fillna(
            resp_wider_in_ids[227287]
        )
        resp_wider_in_ids["tidal_volume_obs"] = (
            resp_wider_in_ids[224685]
            .fillna(resp_wider_in_ids[224686])
            .fillna(resp_wider_in_ids[224421])
        )
        resp_wider_in_ids["resp_rate_set"] = resp_wider_in_ids[224688].fillna(
            resp_wider_in_ids[227581]
        )
        resp_wider_in_ids["resp_rate_obs"] = resp_wider_in_ids[224690].fillna(
            resp_wider_in_ids[224422]
        )
        resp_wider_in_ids["flow_rate_set"] = resp_wider_in_ids[224691].fillna(
            resp_wider_in_ids[227582]
        )
        resp_wider_in_ids["peep_set"] = resp_wider_in_ids[220339].fillna(
            resp_wider_in_ids[227579]
        )
        resp_wider_in_ids["mode_name"] = resp_wider_in_ids[223849].fillna(
            resp_wider_in_ids[229314].fillna(resp_wider_in_ids[227577])
        )  # FIXME
        # remove duplicate variable columns that were coaleased into one
        resp_wider_cleaned = resp_wider_in_ids.drop(
            columns=[
                225448,
                226237,
                223834,
                227287,
                224685,
                224686,
                224421,
                224688,
                227581,
                224690,
                224422,
                224691,
                227582,
                220339,
                227579,
                223849,
                229314,
                227577,
            ]
        )
        resp_wider_cleaned.rename(columns=self.resp_mapper, inplace=True)

        logging.info("mapping device and mode names to categories...")
        # map _name to _category
        resp_wider_cleaned["device_category"] = resp_wider_cleaned["device_name"].map(
            lambda x: self.resp_device_mapper[x.strip()] if pd.notna(x) else None
        )
        resp_wider_cleaned["mode_category"] = resp_wider_cleaned["mode_name"].map(
            self.resp_mode_mapper
        )
        return resp_wider_cleaned

    @intm_store_in_dev
    def _rename_reorder_recast_cols(self, df: pd.DataFrame = None):
        resp_wider_cleaned = df
        logging.info("renaming, reordering, and re-casting columns...")
        resp_final = rename_and_reorder_cols(
            resp_wider_cleaned,
            rename_mapper_dict={
                "hadm_id": "hospitalization_id",
                "time": "recorded_dttm",
            },
            new_col_order=self.RESP_COLUMNS,
        )
        # convert dtypes:
        resp_float_cols = [
            col for col in resp_final.columns if "_set" in col or "_obs" in col
        ]
        resp_final["hospitalization_id"] = resp_final["hospitalization_id"].astype(
            "string"
        )
        # resp_final["tracheostomy"] = resp_final["tracheostomy"].astype(bool)
        for col_name in resp_float_cols:
            resp_final[col_name] = resp_final[col_name].astype(float)
        return resp_final

    def _clean_fio2_set_helper(self, value: float) -> float:
        """
        This is deprecated and kept only for reference.
        """
        value = float(value)
        if value >= 20 and value <= 100:
            return value / 100
        elif value > 1 and value < 20:
            return np.nan
        elif value > 0.2 and value <= 1:
            return value
        else:
            return np.nan

    @intm_store_in_dev
    def _clean_fio2_set(self, df: pd.DataFrame = None):
        '''
        Apply outlier handling and drop the nulls thus generated.
        ref: https://github.com/MIT-LCP/mimic-code/blob/e39825259beaa9d6bc9b99160049a5d251852aae/mimic-iv/concepts/measurement/bg.sql#L130
        '''     
        logging.info("cleaning fio2_set...")
        query = '''
        UPDATE df
        SET value = CASE
            WHEN value >= 20 AND value <= 100 THEN value / 100
            WHEN value > 1 AND value < 20 THEN NULL
            WHEN value > 0.2 AND value <= 1 THEN value  
            ELSE NULL 
        END
        WHERE variable = 'fio2_set'
        '''
        df = duckdb.query(query).df()
        # df.dropna(subset=['value'], inplace=True)
        return df

    @intm_store_in_dev
    def _clean_tracheostomy(self, df: pd.DataFrame = None):
        resp_fc = df
        logging.info(
            "imputing whether tracheostomy had been performed at the time of observation..."
        )
        resp_fc.rename(columns={"tracheostomy": "trach_performed"}, inplace=True)
        resp_fc["trach_implied"] = (
            resp_fc["device_name"].isin(["Tracheostomy tube", "Trach mask"])
        ) | (resp_fc["trach_performed"] == 1)
        resp_fc["trach_bool"] = resp_fc.groupby("hospitalization_id")[
            "trach_implied"
        ].transform(lambda x: x.cumsum().astype(bool))
        resp_fcf = rename_and_reorder_cols(
            resp_fc, {"trach_bool": "tracheostomy"}, self.RESP_COLUMNS
        )
        return resp_fcf

    # TODO: add a validate step
    def validate(self, df: pd.DataFrame = None):
        """
        check for no more null.
        """
        pass


def main():
    pipeline = RespPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()

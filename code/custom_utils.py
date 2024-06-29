import pandas as pd
import gzip

def load_gz_csv(file_path):
    with gzip.open(file_path, 'rt', encoding = 'utf-8') as f:
        df = pd.read_csv(f)
    return df

def lookup_item(df, kw: str, col: str = "label", case: bool = False, cleaner: bool = False):
    '''
    Look up an item by keyword from the `d_items` table of the `icu` module.
    - col = {"label", "abbr"}
    '''
    if col == "abbr": col == "abbreviation"
    out = df[
        df[col].str.contains(kw, case = case, na = False)
    ]
    # original output
    if not cleaner:
        return out
    # cleaner output:
    else:
        cleaner_out = (
            out
            .loc[:, ["itemid", "label", "abbreviation", "category", "unitname", "param_type"]]
            .reset_index(drop = True)
        )
        return cleaner_out
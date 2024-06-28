import pandas as pd
import gzip

def load_gz_csv(file_path):
    with gzip.open(file_path, 'rt', encoding = 'utf-8') as f:
        df = pd.read_csv(f)
    return df

def lookup_item(df, kw: str, col: str = "label", case: bool = False):
    '''
    col = {"label", "abbr"}
    '''
    if col == "abbr": col == "abbreviation"
    output = df[
        df[col].str.contains(kw, case = case, na = False)
    ]
    return output
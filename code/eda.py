import pandas as pd
import gzip

def load_gz_csv(file_path):
    with gzip.open(file_path, 'rt', encoding = 'utf-8') as f:
        df = pd.read_csv(f)
    return df

chartevents = load_gz_csv("../mimic-iv-2.2/icu/chartevents.csv.gz")
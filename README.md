# CLIF-MIMIC
This repository provides the ETL pipeline to convert the MIMIC dataset into the Common Longitudinal ICU data Format (CLIF).

## Instructions
### Add your configuration
#### Required
Navigate to `/config/config.json` and customize a few things:
1. On the backend, the pipeline requires a copy of the MIMIC data in the parquet format for much faster processing. 
    - If you have already created a parquet copy of MIMIC before, you can set `"create_mimic_parquet_from_csv": 0` and provide the path at which you store your MIMIC parquet files at `"mimic_parquet_dir"`.
    - otherwise, if you do not have a copy of MIMIC in parquet yet, set `"create_mimic_parquet_from_csv": 1` and provide the path at which you store the compressed csv files (.csv.gz) you downloaded from PhysioNet under `"mimic_csv_dir"` in the config file. Optionally, you can also specify a path at which you want to save the converted parquet files, but this it not required. You can leave it blank as a empty string `""`, in which case the program would default to creating a `/parquet` subdirectory under your `"mimic_csv_dir"`. 


2. Specify the CLIF tables you want in the next run, by setting the value of tables you want to be 1 (otherwise 0) under `"clif_tables"`. 

```python
# for example, specify the json object this way when there are updates to the `vitals` and `labs` table only, so you only need to update these two tables.
{   ...
    "clif_tables": {
        "patient": 0,
        "hospitalization": 0,
        "adt": 0,
        "vitals": 1,
        "labs": 1,
        "patient_assessments": 0,
        "respiratory_support": 0,
        "medication_admin_continuous": 0,
        "position": 0
    }
}
```

#### Optional
3. To enable working across multiple devices or workspaces, you can add more "workspace" along with their respective csv and parquet directory paths. For more details, you can refer to `/config/config_example.json` for an example of how I personally specify file paths under three different workspace set-up: "local," "hpc," and "local_test." This would allow you to seamlessly switch between different devices or environments without having to update file paths every time you do so. Whenever you switch, you just need to update the name of the `"current_workspace"` accordingly, e.g. specify that `"current_workspace": "hpc"` as long as you have specified a set of directory paths under a key of the same name, i.e. `"hpc": {...}`. 

The following example shows I have specified two sets of paths corresponding to two workspaces I work at: "local" and "hpc", and I'm currently operating in my "local" workspace. 
- Since I had already created MIMIC parquet files in my HPC environment, I left `"mimic_csv_dir"` as blank "" and only provided where I had already stored my parquet files, at `"mimic_parquet_dir"`.
- Meanwhile, I didn't copy these parquet files from my HPC environment to my local device, so I elected to convert them again from csv by specifying their local location at `"mimic_csv_dir"`, while leaving `"mimic_parquet_dir"` blank to indicate I'm happy to use the default setting which is to create a subdirectory `/parquet` under the csv directory, at `"/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1/parquet"`.

```python
# 
{   "current_workspace": "local",
    "local": {
        "mimic_csv_dir": "/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1",
        "mimic_parquet_dir": ""
    },
    "hpc": {
        "mimic_csv_dir": "",
        "mimic_parquet_dir":"/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1/parquet"
    },
```

4. You can also store multiple versions of the CLIF table outputs by customizing `clif_output_dir_name`. If you leave it blank with "", automatic naming would be triggered and give f"rclif-{CLIF_VERSION}". 

### Run the pipeline
After you navigated to the project directory, run the following line by line:

```python
# create a virtual environment
python3 -m venv mimic_to_clif_venv/
# activate the virtual environment
source mimic_to_clif_venv/bin/activate
# install the dependencies
pip install -r requirements.txt
# run the pipeline
python3 main.py
```

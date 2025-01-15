# CLIF-MIMIC
This repository provides the ETL pipeline to convert the MIMIC dataset into the Common Longitudinal ICU data Format (CLIF).

## Instructions
### Add your configuration
#### Required
Navigate to `/config/config.json` and customize a few things:
1. On the backend, the pipeline requires a copy of the MIMIC data in the parquet format for much faster processing. 
    - If you have already created a parquet copy of MIMIC before, you can set `"create_mimic_parquet_from_csv": 0` and provide the path at which you store your MIMIC parquet files at `"mimic_parquet_dir"`.
    - otherwise, if you do not have a copy of MIMIC in parquet yet, set `"create_mimic_parquet_from_csv": 1` and provide both the path at which you store the zipped csv files you downloaded from PhysioNet, as well as where you want to store your parquet files. They should be specified at `"mimic_parquet_dir"` and `"mimic_csv_dir"` in the config file, respectively.


2. Specify the CLIF tables you want in the next run, by setting the value of tables you want to be 1 (otherwise 0) under `"clif_tables"`. 

```
// for example, specify the json object this way when there are updates to the `vitals` and `labs` table only, so you only need to update these two tables.
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
3. To enable working across multiple devices or workspaces, you can add more "workspace" and their respective csv and parquet paths. For more details, you can refer to `/config/config_example.json` where I have specified file paths under three different workspace set-up: "local," "hpc," and "local_test." This would allow you to seamlessly switch between different devices or environments without worrying about having to constantly update file paths or risk overwriting certain CLIF output. Whenever you switch, you just need to change the name of the `"current_workspace"` accordingly, e.g. `"current_workspace": "hpc"` as long as you have specified the directory paths under a key of the same name: "hpc". 

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

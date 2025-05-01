# MIMIC-IV to CLIF ETL Pipeline

This repository provides the ETL pipeline to transform the MIMIC-IV database into the Common Longitudinal ICU data Format (CLIF).

The latest release is [v0.1.0](CHANGELOG.md#v010---2025-04-27), transforming [MIMIC-IV 3.1](https://physionet.org/content/mimiciv/3.1/) to [CLIF 2.0.0](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.0.0.html). 


#### Table of contents
- [Documentation](#documentation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Documentation
For recent updates and planned future releases, see the [change log](CHANGELOG.md).

For the mapping of data elements from MIMIC-IV to CLIF, see [this spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?usp=sharing.) for details.

## Usage
Fork your own copy of this repository, and clone to your local directory. 

### Add your configuration

#### Required

Find `/config/config_template.json`, rename it to `config.json`, and customize a few things in this file:

1. On the backend, the pipeline requires a copy of the MIMIC data in the parquet format for much faster processing. 
    - If you have already created a parquet copy of MIMIC before, you can set `"create_mimic_parquet_from_csv": 0` and provide the *absolute* path at which you store your MIMIC parquet files, at `"mimic_parquet_dir"`.
    - otherwise, if you do not have a copy of MIMIC in parquet yet, set `"create_mimic_parquet_from_csv": 1` and change the `"mimic_csv_dir"` under `"default"` to the *absolute* path at which you store the compressed csv files (.csv.gz) you downloaded from PhysioNet. By default, if you leave `"mimic_parquet_dir"` as a blank `"`, the program would create a `/parquet` subdirectory under your `"mimic_csv_dir"`. Optionally, you can also elect to store it anywhere else and the program would create a directory at the alternative path you provided. 

2. Specify the CLIF tables you want in the next run, by setting the value of tables you want to be 1 (otherwise 0) under `"clif_tables"`. 
    - for example, specify the json object in the following way to recreate two tables (`vitals` and `labs`) that were recently updated:

```python
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
3. To enable working across multiple devices or workspaces, you can add more "workspace" along with their respective csv and parquet directory paths. For more details, you can refer to the example below or `/config/config_example.json` for how I personally specify file paths under three different workspace set-up: "local," "hpc," and "local_test." This would allow you to seamlessly switch between different devices or environments without having to update file paths every time you do so. Whenever you switch, you just need to update the name of the `"current_workspace"` accordingly, e.g. specify that `"current_workspace": "hpc"` as long as you have specified a set of directory paths under a key of the same name, i.e. `"hpc": {...}`. 

The following example shows I have specified two sets of paths corresponding to two workspaces I work at: "local" and "hpc", and I'm currently operating in my "local" workspace. 
- Since I had already created MIMIC parquet files in my HPC environment, I left `"mimic_csv_dir"` as blank `""` and only provided the location of my parquet files, at `"mimic_parquet_dir"`.
- Meanwhile, I didn't copy these parquet files from my HPC environment to my local device, so I elected to convert them again from csv by specifying their local location at `"mimic_csv_dir"`, while leaving `"mimic_parquet_dir"` blank to indicate I'm happy to use the default setting which is to create a subdirectory `/parquet` under the csv directory, i.e. at `"/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1/parquet"`.

```python
{   "current_workspace": "local",
    "hpc": {
        "mimic_csv_dir": "",
        "mimic_parquet_dir":"/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1/parquet"
    },
    "local": {
        "mimic_csv_dir": "/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1",
        "mimic_parquet_dir": ""
    }
```

4. You can also store multiple versions of the CLIF table outputs by customizing `clif_output_dir_name`. If you leave it blank with `""`, the program would default to naming it `f"rclif-{CLIF_VERSION}"`. 

### Run the pipeline
After you navigated to the project directory, run the following *line by line*:

```sh
# create a virtual environment
python3 -m venv .venv/

# activate the virtual environment
source .venv/bin/activate

# install the dependencies
pip install -r requirements.txt

# run the pipeline
python3 main.py
```

## Contributing
To contribute to this open-source project, feel free to :
1. Open an issue for any bug or new data request.
2. Follow some branch naming conventions (e.g. `new-table/dialysis`).
3. Submit a pull request (PR) for review.

## License
This project is licensed under the MIT License. 

**Note:** The MIMIC-IV dataset is subject to the **PhysioNet data use agreement**, and users must obtain access through PhysioNet before processing.
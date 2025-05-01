# Changelog

| MIMIC version | CLIF version | Status | Latest CLIF-MIMIC release |
|------|--------|----------|-----------|  
| IV-3.1 | [2.1.0](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.1.0.html) | 🚀 |  Arriving soon!   |
| IV-3.1 | [2.0.0](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.0.0.html)  | ✅ |  [v0.1.0](#v010---2025-04-27)  |

## LATEST! [v0.1.0] - 2025-05-01
### Readme
This is the last CLIF-MIMIC release for [CLIF's 2.0.0 version](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.0.0.html). It is a major update that introduces new data elements as well as fixes that yield much cleaner data with fewer nulls. 

That said, the CLIF-MIMIC release for [CLIF 2.1.0]((https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.1.0.html)) is scheduled next week, so **unless you are interested in obtaining the new SBT data elements right now, you are welcomed to wait for the next release, where more data elements will be added** (and are currently withheld to align with 2.0.0's schema).

If you are using this release, please update all CLIF tables as they are all impacted by the time-zone update. You should also re-install or update the virtual environment following the [README](README.md#run-the-pipeline) since new packages were added to validate data quality.

### New
- add Spontaneous Breathing Trial (SBT) data elements to the CLIF `patient_assessments` table (specifically `sbt_delivery_pass_fail` and `sbt_fail_reason`)
- add outlier labs values parsed from MIMIC comments
- add [`hamilton`](https://hamilton.dagworks.io/en/latest/) and [`pandera`](https://www.union.ai/pandera) as light-weight back-end frameworks for pipeline orchestration and data quality validation respectively

### Changed
- convert all date-time fields (ending in `dttm`) from UTC−05:00 (U.S. Eastern Time Zone) to UTC, following a [CLIF-wide design](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF/issues/40) to standardize timezone representation

### Fixed
- remove unintended nulls from the `labs` table ([#2](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/2)), `patient` table ([#3](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/3)), `respiratory_support` table ([#5](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/5)), `hospitalization` table ([#4](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/4))


## [v0.0.1] - 2025-01-31
### Readme
CLIF tables updated: `respiratory_support`, `patient`

### Changed
- update the `device_category` mapping of "T-piece" from "IMV" to "Others" pending further review

### Fixed
- correct typo in the config files that mistakenly suggested the latest CLIF version is 2.1 (it should be 2.0)
- remove two duplicate rows in the CLIF `patient` output table 


## [v0.0.0] - 2025-01-21
First release!

## [Planned]
- [add](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/1) mapping for `location_type` 

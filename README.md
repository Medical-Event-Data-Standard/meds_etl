# meds_etl

A collection of ETLs from common data formats to Medical Event Data Standard (MEDS)

This package library currently supports:

- MIMIC-IV
- OMOP v5
- MEDS FLAT, a flat version of MEDS

## MIMIC-IV

In order to run the MIMIC-IV ETL, simply run the following command:

`meds_etl_mimic mimiciv mimiciv_meds` where mimiciv is a download of MIMIC-IV and mimiciv-esds will be the destination path for the dataset.

## OMOP

In order to run the MIMIC-IV ETL, simply run the following command:

`meds_etl_omop omop omop_meds` where omop is a folder containing csv files (optionally gzipped) for an OMOP dataset. Each table should either be a csv file with the table name (such as person.csv) or a folder with the table name containing csv files.

## Unit tests

Tests can be run from the project root with the following command:

```
pytest -v
```

Tests requiring data will be skipped unless the `tests/data/` folder is populated first.

To download the testing data, run the following command/s from project root:

```
# Download the MIMIC-IV-Demo dataset (v2.2) to a tests/data/ directory
wget -r -N -c --no-host-directories --cut-dirs=1 -np -P tests/data https://physionet.org/files/mimic-iv-demo/2.2/
```

## MEDS Flat

The MEDS schema can be a bit tricky to use as it is a nested parquet schema and nested schemas are not as widely supported as flat schemas. For example, it's not possible to represent a nested schema with CSV files. In additional, the implicit global join within MEDS in order to combine all of a patient's data into a single value can be difficult to implement.

In order to make things simpler for users, this package provides a special MEDS Flat schema and ETLs that transform between MEDS Flat and MEDS.

This MEDS CSV schema is 1-1 compatible with MEDS. It is a simple tabular schema that has four core columns: patient_id, time, code, value. These columns correspond to the core ESDS columns, with the one exception that all the value columns have been combined into a single one. Additional columns can be present, which correspond to event metadata.

For MEDS Flat, we support both CSV and Parquet input files for transforming into MEDS.

In order to convert a MEDS Flat dataset into MEDS, simple run the following command:

`meds_etl_flat flat flat_meds` where flat is a folder containing MEDS Flat data and `flat_meds` is the target folder to store the results in.

The expected input format is a folder with a metadata.json file and a flat_data subfolder. The flat_data subfolder can contain any number of csv or Parquet files.

For example, the following CSV would be converted into the following MEDS patient:

Input CSV:
```
patient_id,time,code,text_value,numeric_value,datetime_value,arbitrary_metadata_column
100,1990-11-30,Birth/Birth,,,,a string
100,1990-11-30,Gender/Gender,Male,,,another string
100,1990-11-30,Labs/SystolicBloodPressure,,100,,
100,1990-12-28,ICD10CM/E11.4,,,,anything
```

Output MEDS Patient:
```
{
  'person_id': 100,
  'events': [
    {
      'time': 1990-11-30,
      'measurements': [
        {'code': 'Birth/Birth', 'metadata': {'arbitrary_metadata_column': 'a string'}},
        {'code': 'Gender/Gender', 'text_value': 'Male', 'metadata': {'arbitrary_metadata_column': 'another string'}},
        {'code': 'Labs/SystolicBloodPressure', 'numeric_value': 100, 'metadata': {'arbitrary_metadata_column': None}},
      ],
    },
    {
      'time': 1990-12-28,
      'measurements': [{'code': 'ICD10CM/E11.4', 'metadata': {'arbitrary_metadata_column': 'anything'}}],
    },
  ]
}

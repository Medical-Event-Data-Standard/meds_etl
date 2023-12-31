# meds_etl

A collection of ETLs from common data formats to Medical Event Data Standard (MEDS)

This package library currently supports:

- MIMIC-IV
- OMOP v5
- MEDS CSV, a simple CSV format

## MIMIC-IV

In order to run the MIMIC-IV ETL, simply run the following command:

`meds_etl_mimic mimiciv mimiciv-esds` where mimiciv is a download of MIMIC-IV and mimiciv-esds will be the destination path for the dataset.

## OMOP

In order to run the MIMIC-IV ETL, simply run the following command:

`meds_etl_omop omop omop-esds` where omop is a folder containing csv files (optionally gzipped) for an OMOP dataset. Each table should either be a csv file with the table name (such as person.csv) or a folder with the table name containing csv files.

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

The MEDS schema can be a bit tricky to directly ETL into as it is a nested parquet schema and nested schemas are not supported by many pieces of software. In additional, the global join of combining all of a patient's data into a single value can be difficult to implement.

In order to make things simpler for users, we have a special MEDS Flat schema, and ETLs back and forth between MEDS and MEDS Flat.

This MEDS CSV schema is 1-1 compatible with MEDS. It is a simple tabular schema that has four core columns: patient_id, time, code, value. These columns correspond to the core ESDS columns, with the one exception that all the value columns have been combined into a single one.

patient_id is a 64-bit integer, time is an ISO 8601 datetime, and code is a string. value is either a number, an ISO 8601 datetime, or arbitrary text. 

These columns get converted into the standard ESDS columns, with value converted to either numeric_value, datetime_value or text_value as necessary. 

The simple CSV format also supports arbitrary additional columns, that get converted into a metadata struct.

For example, the following CSV would be converted into the following MEDS patient:

Input CSV:
```
patient_id,time,code,value,arbitrary_metadata_column
100,1990-11-30,Birth/Birth,,a string
100,1990-11-30,Gender/Gender,Male,another string
100,1990-11-30,Labs/SystolicBloodPressure,100,
100,1990-12-28,ICD10CM/E11.4,,anything
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

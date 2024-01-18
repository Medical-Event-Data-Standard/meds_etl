# meds_etl

A collection of ETLs from common data formats to Medical Event Data Standard (MEDS)

This package library currently supports:

- MIMIC-IV
- OMOP v5
- MEDS FLAT, a flat version of MEDS

## MIMIC-IV

In order to run the MIMIC-IV ETL, simply run the following command:

`meds_etl_mimic mimiciv mimiciv_meds` where mimiciv is a download of MIMIC-IV and mimiciv_meds will be the destination path for the dataset.

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

MEDS Flat schema is a flattened version of MEDS. MEDS Flat data consists of a folder with a metadata.json file (from the [MEDS metadata schema](https://github.com/Medical-Event-Data-Standard/meds/blob/main/src/meds/__init__.py#L99)) and a "flat_data" folder with MEDS Flat data files. MEDS Flat data files must be either csvs (optionally gzipped) or parquet files that contain three core columns: "patient_id", "time", and "code". These columns correspond to the core MEDS columns. In addition, "datetime_value", "numeric_value" and/or "text_value" can be provided to match those columns in MEDS. Alternatively, a single "value" column can be provided, which will then be transformed into  "datetime_value", "numeric_value" and "text_value" as appropriate.

Arbitrary additional columns can be added, each of which will become MEDS metadata columns.

In order to convert a MEDS Flat dataset into MEDS, simply run the following command:

`meds_etl_from_flat meds_flat meds` where meds_flat is a folder containing MEDS Flat data and `meds` is the target folder to store the MEDS dataset in.

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
  'patient_id': 100,
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

We also support a reverse ETL, converting from MEDS to MEDS Flat.

The command for this is `meds_etl_to_flat`. For example:

`meds_etl_to_flat meds meds_flat` where meds is a folder containing a MEDS dataset and meds_flat is the folder that will store the resulting MEDS Flat dataset.

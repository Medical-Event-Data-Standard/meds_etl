# meds_etl

A collection of ETLs from common data formats to Medical Event Data Standard (MEDS)

This package library currently supports:

- MIMIC-IV
- OMOP v5

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
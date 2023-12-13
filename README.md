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

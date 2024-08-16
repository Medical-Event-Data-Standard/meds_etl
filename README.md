# meds_etl

A collection of ETLs from common data formats to Medical Event Data Standard (MEDS)

This package library currently supports:

- MIMIC-IV
- OMOP v5
- MEDS Unsorted, an unsorted version of MEDS

## Setup
Install the package

```bash
pip install meds_etl
```

## Backends

ETLs are one of the most computationally heavy components of MEDS, so efficiency is very important.

MEDS-ETL has several parallel implementations of core algorithms to balance the tradeoff between efficiency and ease of use.

All commands generally take an additional parameter --backend, that allows users to switch between different backends.

We currently support two backends: polars (the default) and cpp.

Backend information:

- polars (default backend): A Python only implementation that only requires polars to run. The main issue with this implementation is that it is rather inefficient. It's recommended to use as few shards as possible while still avoiding out of memory errors.

- cpp: A custom C++ backend. This backend is very efficient, but might not run on all platforms and has a limited feature set. It's recommended to use the same number of shards as you have CPUs available.

If you want to use either the cpp backend, make sure to install meds_etl with the correct optional dependencies.

```bash
# For the cpp backend
pip install "meds_etl[cpp]"
```

## MIMIC-IV

In order to run the MIMIC-IV ETL, simply run the following command:

```bash
meds_etl_mimic [PATH_TO_SOURCE_MIMIC] [PATH_TO_OUTPUT]
```

where `[PATH_TO_SOURCE_MIMIC]` is a download of MIMIC-IV and `[PATH_TO_OUTPUT]` will be the destination path for the MEDS dataset.

## OMOP

In order to run the OMOP ETL, simply run the following command:

```bash
meds_etl_omop [PATH_TO_SOURCE_OMOP] [PATH_TO_OUTPUT]
```

where `[PATH_TO_SOURCE_OMOP]` is a folder containing csv files (optionally gzipped) for an OMOP dataset and `[PATH_TO_OUTPUT]` will be the destination path for the MEDS dataset. Each OMOP table should either be a csv file with the table name (such as person.csv) or a folder with the table name containing csv files.

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

## MEDS Unsorted

MEDS itself can be a bit tricky to generate as it has ordering and shard location requirements for events (events for a particular subject must be sorted by time and can only be in one shard).

In order to make things simpler for users, this package provides a special MEDS Unsorted schema and ETLs that transform between MEDS Unsorted and MEDS.

MEDS Unsorted is simply MEDS without the ordering and shard requirements for events, with the name of the data folder changed from "data" to "unsorted_data".

In order to convert a MEDS Unsorted dataset into MEDS, simply run the following command:

`meds_etl_sort meds_unsorted meds` where meds_unsorted is a folder containing MEDS Unsorted data and `meds` is the target folder to store the MEDS dataset in.

## Troubleshooting

**Polars incompatible with Mac M1**

If you get this error when running `meds_etl`:

```bash
RuntimeWarning: Missing required CPU features.

The following required CPU features were not detected:
    avx, fma
Continuing to use this version of Polars on this processor will likely result in a crash.
Install the `polars-lts-cpu` package instead of `polars` to run Polars with better compatibility.
```

Then you'll need to install the run the following:

```bash
pip uninstall polars
pip install polars-lts-cpu
```

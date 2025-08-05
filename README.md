# meds_etl
[![PyPI - Version](https://img.shields.io/pypi/v/meds_etl)](https://pypi.org/project/meds_etl/)
[![tests](https://github.com/Medical-Event-Data-Standard/meds_etl/actions/workflows/python-test.yml/badge.svg)](https://github.com/actions/workflows/python-test.yml)
![python](https://img.shields.io/badge/-Python_3.10-blue?logo=python&logoColor=white)
![Static Badge](https://img.shields.io/badge/MEDS-0.3.3-blue)

A collection of ETLs from common data formats to Medical Event Data Standard (MEDS).

This package library currently supports:

- MIMIC-IV 2.2
- OMOP v5.4/5.3
- MEDS Unsorted, an unsorted version of MEDS

Currently the package converts to MEDS 0.3.3.
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

If you want to use the cpp backend, make sure to install meds_etl with the correct optional dependencies.

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

Note that this code only supports MIMIC 2.2 currently, but PRs are welcome to add support for other MIMIC versions.

## OMOP

In order to run the OMOP ETL, simply run the following command:

```bash
meds_etl_omop [PATH_TO_SOURCE_OMOP] [PATH_TO_OUTPUT]
```

where `[PATH_TO_SOURCE_OMOP]` is a folder containing csv files (optionally gzipped) for an OMOP dataset and `[PATH_TO_OUTPUT]` will be the destination path for the MEDS dataset. Each OMOP table should either be a csv file with the table name (such as person.csv) or a folder with the table name containing csv files.

This ETL currently operates on the tables: 
`person, drug_exposure, visit, condition, death, procedure, device_exposure, measurement, observation, note, visit_detail`
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

In order to make it easier to generate MEDS, this package provides a special MEDS Unsorted schema and ETLs that transform between MEDS Unsorted and MEDS. The idea is that instead of writing a complex MEDS ETL, users can instead write a simpler ETL to MEDS Unsorted and then use this package as a final stage.

MEDS Unsorted is simply MEDS without the ordering and shard requirements for events, with the name of the data folder changed from "data" to "unsorted_data".

In order to convert a MEDS Unsorted dataset into MEDS, simply run the following command:

`meds_etl_sort meds_unsorted meds` where meds_unsorted is a folder containing MEDS Unsorted data and `meds` is the target folder to store the MEDS dataset in.
## Arguments
The following command-line arguments available for the `meds_etl_omop` command:

| Argument                  | Type    | Default   | Description                                                                                                                                                                                                                  |
|---------------------------|---------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path_to_src_omop_dir`    | str     | —         | Path to the OMOP source directory, e.g. `~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_2023_11_19` for STARR-OMOP full or `~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_1pcent_2024_02_09`. |
| `path_to_dest_meds_dir`   | str     | —         | Path to where the output MEDS files will be stored.                                                                                                                                                                          |
| `--num_shards`            | int     | 100       | Number of shards to use for converting MEDS from the unsorted format to MEDS (subjects are distributed approximately uniformly at random across shards and collation/joining of OMOP tables is performed on a shard-by-shard basis). |
| `--num_proc`              | int     | 1         | Number of vCPUs to use for performing the MEDS ETL.                                                                                                                                                                          |
| `--backend`               | str     | polars    | The backend to use when converting from MEDS Unsorted to MEDS in the ETL. See the README for a discussion on possible backends.                                                                                              |
| `--verbose`               | int     | 0         | Verbosity level.                                                                                                                                                                                                             |
| `--continue_job`          | flag    | False     | If set, the job continues from a previous run, starting after the conversion to MEDS Unsorted but before converting from MEDS Unsorted to MEDS.                                                                              |
| `--force_refresh`         | flag    | False     | If set, this will overwrite all previous MEDS data in the output dir.                                                                                                                                                        |
| `--omop_version`          | str     | 5.4       | Switch between OMOP 5.3/5.4, default 5.4.                                                                                                                                                                                    |

The following command-line arguments available for the `meds_etl_mimic` command:
| Argument         | Type   | Default  | Description                                              |
|------------------|--------|----------|----------------------------------------------------------|
| `src_mimic`      | str    | —        | Path to the source MIMIC\_IV directory                   |
| `destination`    | str    | —        | Path to the output MEDS directory                        |
| `--num_shards`   | int    | 100      | Number of shards for processing                          |
| `--num_proc`     | int    | 1        | Number of processes (vCPUs) to use                       |
| `--backend`               | str     | polars    | The backend to use when converting from MEDS Unsorted to MEDS in the ETL. See the README for a discussion on possible backends.                                                                                              |
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

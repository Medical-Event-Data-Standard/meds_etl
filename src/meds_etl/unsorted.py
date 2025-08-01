from __future__ import annotations

import argparse
import functools
import glob
import logging
import multiprocessing
import os
import random
import shutil
from typing import Dict, List, Mapping, Tuple

import meds
import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm

mp = multiprocessing.get_context("forkserver")
if mp is None:
    # Could not get forkserver, just use the default
    mp = multiprocessing.get_context()

KNOWN_COLUMNS = {"subject_id", "numeric_value", "time", "code"}


def get_columns(event_file: str) -> Mapping[str, pl.DataType]:
    """
    Extracts column names and their data types from a Parquet file schema.

    This function reads the schema of a Parquet file to get column names and their respective data types.
    It maps the Parquet data types to their string representations to facilitate processing in other parts
    of the ETL pipeline.

    Args:
        event_file (str): The file path to the Parquet file.

    Returns:
        dict: A dictionary where keys are column names from the Parquet file schema, and values are the
        data type of each column represented as a string.

    Raises:
        FileNotFoundError: If the specified Parquet file does not exist or cannot be opened.
        IOError: If there's an error reading the file schema.

    Example:
        Suppose we have a Parquet file 'subject_data.parquet' with the following schema:
            - subject_id (int64)
            - name (string)
            - visit_date (date32)

        Calling `get_parquet_columns('subject_data.parquet')` would return:
            {'subject_id': 'int64', 'name': 'string', 'visit_date': 'date32'}
    """
    schema = pl.scan_parquet(event_file).collect_schema()
    return dict(schema)


def get_property_columns(table, properties_columns):
    cols = []
    for column, dtype in properties_columns:
        if column in table.collect_schema():
            cols.append((column, pl.col(column)))
        else:
            cols.append((column, pl.lit(None, dtype=dtype)))

    return cols


def verify_shard(shard, event_file, important_columns=("subject_id", "code")):
    """Verify that every shard has non-null important columns

    Args:
        shard (pl.DataFrame): Polars dataframe where each row represents a measurement
        event_file (str): Path to the source data for the shard, used only for logging/reporting
        important_columns (Tuple[str]): Columns that should not have any null values

    Raises:
        ValueError if any entries in any of the important columns are null
    """
    for important_column in important_columns:
        rows_with_invalid_code = shard.filter(pl.col(important_column).is_null())
        if len(rows_with_invalid_code) != 0:
            error_message = f"Have rows with invalid {important_column} in {event_file}"
            logging.error(error_message)
            for row in rows_with_invalid_code:
                logging.error(str(row))
            raise ValueError(error_message)


def create_and_write_shards_from_table(
    table, temp_dir: str, num_shards: int, property_columns: List[Tuple[str, pl.DataType]], filename: str
):
    # Convert all the column names to lower case
    table = table.rename({c: c.lower() for c in table.collect_schema().names()})

    # Convert subject IDs into integers
    subject_id = pl.col("subject_id").cast(pl.Int64())

    # Convert time column into polars Datetime/`datetime[μs]` type
    time = pl.col("time")

    # If it's not a UTF-8 string, we try to just use polars native `Datetime()` handling
    time = time.cast(pl.Datetime(time_unit="us"))

    # Codes should be UTF-8 strings
    code = pl.col("code").cast(pl.Utf8())

    numeric_value = pl.col("numeric_value").cast(pl.Utf8())

    columns = (
        [
            ("subject_id", subject_id),
            ("time", time),
            ("code", code),
            ("numeric_value", numeric_value),
        ]
        + get_property_columns(table, property_columns)
        + [("shard", subject_id.hash(213345) % num_shards)]
    )

    exprs = [b.alias(a) for a, b in columns]

    # Actually execute the typecast conversion, partition table into
    # shards based on hashed subject ID
    event_data = table.select(*exprs).collect().partition_by(["shard"], as_dict=True, maintain_order=False)

    # Validate/verify each shard's important columns and write to disk
    for (shard_index,), shard in event_data.items():
        verify_shard(shard, filename)
        fname = os.path.join(temp_dir, str(shard_index), filename)
        shard.write_parquet(fname, compression="uncompressed")


def process_file(event_file: str, *, temp_dir: str, num_shards: int, property_columns: List[Tuple[str, pl.DataType]]):
    """Partition MEDS Unsorted files into shards based on subject ID and write to disk"""
    logging.info("Working on ", event_file)

    table = pl.scan_parquet(event_file)

    cleaned_event_file = os.path.basename(event_file).split(".")[0]
    fname = f"{cleaned_event_file}.parquet"
    create_and_write_shards_from_table(table, temp_dir, num_shards, property_columns, fname)


def sort_polars(
    source_unsorted_path: str,
    target_meds_path: str,
    num_shards: int = 100,
    num_proc: int = 1,
) -> None:
    """A polars implementation of the core sorting algorithm"""
    if not os.path.exists(source_unsorted_path):
        raise ValueError(f'The source MEDS Unsorted folder ("{source_unsorted_path}") does not seem to exist?')

    os.makedirs(target_meds_path, exist_ok=True)

    events = []

    decompressed_dir = os.path.join(target_meds_path, "decompressed")
    os.mkdir(decompressed_dir)

    temp_dir = os.path.join(target_meds_path, "temp")
    os.mkdir(temp_dir)

    # Make a folder called `{shard_index}` for each `shard_index` within `temp_dir`
    for shard_index in range(num_shards):
        os.mkdir(os.path.join(temp_dir, str(shard_index)))

    shutil.copytree(os.path.join(source_unsorted_path, "metadata"), os.path.join(target_meds_path, "metadata"))

    tasks = list(glob.glob(os.path.join(source_unsorted_path, "unsorted_data", "*")))

    rng = random.Random(3422342)
    rng.shuffle(tasks)

    def update_types(type_dict, new_type_dict):
        for k, v in new_type_dict.items():
            if k not in type_dict:
                type_dict[k] = v
            else:
                assert v == type_dict[k], f"We got different types for column {k}, {v} vs {type_dict[k]}"

    property_columns_info: Dict[str, pl.DataType] = dict()
    if num_proc != 1:
        with mp.Pool(num_proc) as pool:
            for columns_info in pool.imap_unordered(get_columns, tasks):
                update_types(property_columns_info, columns_info)

            property_columns = sorted([(k, v) for k, v in property_columns_info.items() if k not in KNOWN_COLUMNS])

            processor = functools.partial(
                process_file,
                temp_dir=temp_dir,
                num_shards=num_shards,
                property_columns=property_columns,
            )

            print("Partitioning MEDS Unsorted files into shards based on subject ID and writing to disk...")
            with tqdm(total=len(tasks)) as pbar:
                for _ in pool.imap_unordered(processor, tasks):
                    pbar.update()
    else:
        for task in tasks:
            update_types(property_columns_info, get_columns(task))

        property_columns = sorted([(k, v) for k, v in property_columns_info.items() if k not in KNOWN_COLUMNS])

        processor = functools.partial(
            process_file,
            temp_dir=temp_dir,
            num_shards=num_shards,
            property_columns=property_columns,
        )

        for task in tqdm(tasks, total=len(tasks)):
            processor(task)

    shutil.rmtree(decompressed_dir)

    logging.info("Processing each shard")

    data_dir = os.path.join(target_meds_path, "data")
    os.mkdir(data_dir)

    print("Collating source table data, shard by shard, to create subject timelines...")
    print("(Gathering events into timelines)")
    for shard_index in tqdm(range(num_shards), total=num_shards):
        logging.info("Processing shard ", shard_index)
        shard_dir = os.path.join(temp_dir, str(shard_index))

        if len(os.listdir(shard_dir)) == 0:
            continue
        events = [pl.read_parquet(os.path.join(shard_dir, a)) for a in os.listdir(shard_dir)]

        all_events = pl.concat(events)

        sorted_events = all_events.drop("shard").sort(by=(pl.col("subject_id"), pl.col("time"), pl.col("code")), nulls_last=False)

        # We now have our data in the final form, grouped_by_subject, but we have to do one final transformation
        # We have to convert from polar's large_list to list because large_list is not supported by huggingface

        # We do this conversion using the pyarrow library

        # Save and load our data in order to convert to pyarrow library
        converted = sorted_events.to_arrow()

        fields = []

        # Now we need to reconstruct the schema
        for i, name in enumerate(converted.schema.names):
            if name not in KNOWN_COLUMNS:
                fields.append((name, converted.schema.field(i).type))
        desired_schema = meds.data_schema(fields)

        # All the large_lists are now converted to lists
        casted = converted.cast(desired_schema)

        pq.write_table(casted, os.path.join(data_dir, f"data_{shard_index}.parquet"))

    shutil.rmtree(temp_dir)


def sort(
    source_unsorted_path: str,
    target_meds_path: str,
    num_shards: int = 100,
    num_proc: int = 1,
    backend: str = "polars",
) -> None:
    """
    Args:
        source_unsorted_path (str): Path to directory where MEDS Unsorted files are stored, structured as follows:
            {source_unsorted_path}
            |-- metadata
            |-- unsorted_data
                |-- *.parquet
        target_meds_path (str): Path to directory the MEDS files (converted from MEDS Unsorted) should be stored
        num_shards (str): Number of subject shards, more shards -> fewer subjects per join, only relevant for polars
        num_proc (int): Number of parallel processes
        time_formats (str): Expected possible datetime formats in the `time` column.

    Returns:
        None
    """
    if not os.path.exists(source_unsorted_path):
        raise ValueError(f'The source MEDS Unsorted folder ("{source_unsorted_path}") does not seem to exist?')

    if not os.path.exists(os.path.join(source_unsorted_path, "metadata", "dataset.json")):
        raise ValueError(
            f'The source MEDS Unsorted folder ("{source_unsorted_path}") does not have a dataset metadata file?'
        )

    if not os.path.exists(os.path.join(source_unsorted_path, "unsorted_data")):
        raise ValueError(
            f'The source MEDS Unsorted folder ("{source_unsorted_path}") does not have a unsorted_data folder'
        )

    if backend == "cpp":
        import meds_etl_cpp

        meds_etl_cpp.perform_etl(source_unsorted_path, target_meds_path, num_shards, num_proc)
    else:
        sort_polars(source_unsorted_path, target_meds_path, num_shards, num_proc)


def sort_main():
    parser = argparse.ArgumentParser(prog="meds_etl_sort", description="Performs an ETL from MEDS Unsorted to MEDS")
    parser.add_argument("source_unsorted_path", type=str)
    parser.add_argument("target_meds_path", type=str)
    parser.add_argument("--num_shards", type=int, default=100)
    parser.add_argument("--num_proc", type=int, default=1)
    parser.add_argument("--backend", type=str, default="polars")
    args = parser.parse_args()
    sort(**vars(args))

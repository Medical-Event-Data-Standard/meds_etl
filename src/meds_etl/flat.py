from __future__ import annotations

import argparse
import csv
import functools
import gzip
import logging
import multiprocessing
import os
import pathlib
import random
import shutil
import subprocess
import tempfile
import warnings
from typing import Iterable, List, Literal, Optional, TextIO, Tuple, cast

import psutil

try:
    import duckdb
except ImportError:
    duckdb = None
import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

mp = multiprocessing.get_context("forkserver")
if mp is None:
    # Could not get forkserver, just use the default
    mp = multiprocessing.get_context()

Format = Literal["csv", "parquet", "compressed_csv"]
KNOWN_COLUMNS = {"patient_id", "numeric_value", "datetime_value", "text_value", "value", "time", "code"}


def convert_file_to_flat(source_file: str, *, target_flat_data_path: str, format: Format, time_format: str) -> None:
    """Convert a single MEDS file to MEDS Flat"""
    table = pl.scan_parquet(source_file)

    table = table.drop("static_measurements")

    table = table.explode("events")
    table = table.unnest("events")

    table = table.explode("measurements")
    table = table.unnest("measurements")

    if table.schema["metadata"] == pl.Null():
        table = table.drop("metadata")
    else:
        table = table.unnest("metadata")

    if format == "parquet":
        table.sink_parquet(os.path.join(target_flat_data_path, pathlib.Path(source_file).with_suffix(".parquet").name))
    elif format.endswith("csv"):
        table = table.with_columns(
            time=pl.col("time").dt.strftime(time_format),
            datetime_value=pl.col("datetime_value").dt.strftime(time_format),
        )
        target = os.path.join(target_flat_data_path, pathlib.Path(source_file).with_suffix(".csv").name)
        table.sink_csv(target)
        if format == "compressed_csv":
            subprocess.run(["gzip", "-f", target])


def convert_meds_to_flat(
    source_meds_path: str,
    target_flat_path: str,
    num_proc: int = 1,
    format: Format = "compressed_csv",
    time_format: str = "%Y-%m-%d %H:%M:%S%.f",
) -> None:
    """Convert an entire MEDS dataset to MEDS Flat"""

    if not os.path.exists(source_meds_path):
        raise ValueError(f'The source MEDS folder ("{source_meds_path}") does not seem to exist?')

    os.mkdir(target_flat_path)

    source_meds_data_path = os.path.join(source_meds_path, "data")
    if not os.path.exists(source_meds_data_path):
        raise ValueError(
            f'The source MEDS folder ("{source_meds_path}") '
            f'does not seem to contain a data folder ("{source_meds_data_path}")?'
        )

    source_files = os.listdir(source_meds_data_path)
    tasks = [os.path.join(source_meds_data_path, source_file) for source_file in source_files]

    if os.path.exists(os.path.join(source_meds_path, "metadata.json")):
        shutil.copyfile(
            os.path.join(source_meds_path, "metadata.json"), os.path.join(target_flat_path, "metadata.json")
        )

    target_flat_data_path = os.path.join(target_flat_path, "flat_data")
    os.mkdir(target_flat_data_path)

    converter = functools.partial(
        convert_file_to_flat,
        target_flat_data_path=target_flat_data_path,
        format=format,
        time_format=time_format,
    )

    if num_proc != 1:
        with mp.Pool(processes=num_proc) as pool:
            for _ in pool.imap_unordered(converter, tasks):
                pass
    else:
        for task in tasks:
            converter(task)


def convert_meds_to_flat_main():
    parser = argparse.ArgumentParser(prog="meds_reverse_etl_flat", description="Performs an ETL from MEDS to MEDS Flat")
    parser.add_argument("source_meds_path", type=str)
    parser.add_argument("target_flat_path", type=str)
    parser.add_argument("--num_proc", type=int, default=1)
    parser.add_argument("--format", type=str, default="compressed_csv", choices=["csv", "parquet", "compressed_csv"])
    args = parser.parse_args()
    convert_meds_to_flat(**vars(args))


def load_file(decompressed_dir: str, fname: str) -> TextIO:
    if fname.endswith(".gz"):
        file = tempfile.NamedTemporaryFile(dir=decompressed_dir)
        subprocess.run(["gunzip", "-c", fname], stdout=file)
        return cast(TextIO, file)
    else:
        return open(fname)


def get_csv_columns(event_file: str) -> dict:
    """
    Reads the first line of a CSV file to extract column names, assuming all columns as strings.

    This function opens a CSV file, which can be optionally compressed with gzip, reads the first line to get
    the column names, and assumes all columns to be of string data type since CSV files do not contain explicit
    type information.

    Args:
        event_file (str): The file path to the CSV file. It supports both regular and gzipped CSV files.

    Returns:
        dict: A dictionary where keys are column names and values are assumed data types ('string').

    Raises:
        FileNotFoundError: If the specified file does not exist or cannot be opened.
        IOError: If there's an error reading from the file.
    """
    if event_file.endswith(".gz"):
        with gzip.open(event_file, "rt") as f:
            reader = csv.reader(f)
            columns = next(reader)
    else:
        with open(event_file, "r") as f:
            reader = csv.reader(f)
            columns = next(reader)

    return {col: "string" for col in columns}


def get_parquet_columns(event_file: str) -> dict:
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
        Suppose we have a Parquet file 'patient_data.parquet' with the following schema:
            - patient_id (int64)
            - name (string)
            - visit_date (date32)

        Calling `get_parquet_columns('patient_data.parquet')` would return:
            {'patient_id': 'int64', 'name': 'string', 'visit_date': 'date32'}
    """
    schema = pq.read_schema(event_file)
    return {name: str(schema.field(name).type) for name in schema.names}


def transform_metadata(d, metadata_columns):
    if len(metadata_columns) == 0:
        return pl.lit(None)

    cols = []
    for column in metadata_columns:
        val = d.get(column, pl.lit(None, dtype=pl.Utf8))
        cols.append(val.alias(column))

    return pl.struct(cols)


def verify_shard(shard, event_file, important_columns=("patient_id", "time", "code")):
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


def parse_time(time: pl.Expr, time_formats: Iterable[str]) -> pl.Expr:
    return pl.coalesce(
        [time.str.to_datetime(time_format, strict=False, time_unit="us") for time_format in time_formats]
    )


def convert_generic_value_to_specific(generic_value: pl.Expr) -> Tuple[pl.Expr, pl.Expr, pl.Expr]:
    generic_value = generic_value.str.strip_chars()

    # It's fundamentally very difficult to infer datetime values from strings
    datetime_value = pl.lit(None, dtype=pl.Datetime(time_unit="us"))

    numeric_value = (
        pl.when(datetime_value.is_null())
        .then(generic_value.cast(pl.Float32, strict=False))
        .otherwise(pl.lit(None, dtype=pl.Float32))
    )

    text_value = (
        pl.when(datetime_value.is_null() & numeric_value.is_null())
        .then(generic_value)
        .otherwise(pl.lit(None, dtype=pl.Utf8()))
    )

    return datetime_value, numeric_value, text_value


def create_and_write_shards_from_table(
    table, temp_dir: str, num_shards: int, time_formats: Iterable[str], metadata_columns: List[str], filename: str
):
    # Convert all the column names to lower case
    table = table.rename({c: c.lower() for c in table.columns})

    # Convert patient IDs into integers
    patient_id = pl.col("patient_id").cast(pl.Int64())

    # Convert time column into polars Datetime/`datetime[μs]` type
    time = pl.col("time")
    if table.schema["time"] == pl.Utf8():
        time = parse_time(time, time_formats)
    else:
        # If it's not a UTF-8 string, we try to just use polars native `Datetime()` handling
        time = time.cast(pl.Datetime(time_unit="us"))

    # Codes should be UTF-8 strings
    code = pl.col("code").cast(pl.Utf8())

    # Deterimine the type of value in `value` column, split into `datetime_value`, `numeric_value`, and
    # `text_value` into their appropriate types (or convert these three columns into appropriate types
    # if they already exist).
    if "value" in table.columns:
        for sub_value in ("datetime_value", "numeric_value", "text_value"):
            if sub_value in table.columns:
                raise ValueError("Cannot have both generic value and specific value columns")
        datetime_value, numeric_value, text_value = convert_generic_value_to_specific(pl.col("value"))
    else:
        if "datetime_value" in table.columns:
            datetime_value = pl.col("datetime_value")
            if table.schema["datetime_value"] == pl.Utf8():
                datetime_value = pl.coalesce(
                    [
                        datetime_value.str.to_datetime(time_format, strict=False, time_unit="us")
                        for time_format in time_formats
                    ]
                )
            else:
                datetime_value = datetime_value.cast(pl.Datetime())
        else:
            datetime_value = pl.lit(None, dtype=pl.Datetime())

        if "numeric_value" in table.columns:
            numeric_value = pl.col("numeric_value").cast(pl.Float32())
        else:
            numeric_value = pl.lit(None, dtype=pl.Float32())

        if "text_value" in table.columns:
            text_value = pl.col("text_value").cast(pl.Utf8())
        else:
            text_value = pl.lit(None, dtype=pl.Utf8())

    # Cast all metadata keys and values to UTF-8 strings
    metadata = {}
    for colname in table.columns:
        if colname not in KNOWN_COLUMNS:
            metadata[colname] = pl.col(colname).cast(pl.Utf8())

    metadata = transform_metadata(metadata, metadata_columns)

    # Actually execute the typecast conversion, partition table into
    # shards based on hashed patient ID
    event_data = (
        table.select(
            patient_id=patient_id,
            time=time,
            code=code,
            text_value=text_value,
            numeric_value=numeric_value,
            datetime_value=datetime_value,
            metadata=metadata,
            shard=patient_id.hash(213345) % num_shards,
        )
        .collect()
        .partition_by(["shard"], as_dict=True, maintain_order=False)
    )

    # Validate/verify each shard's important columns and write to disk
    for (shard_index,), shard in event_data.items():
        verify_shard(shard, filename)
        fname = os.path.join(temp_dir, str(shard_index), filename)
        shard.write_parquet(fname, compression="uncompressed")


def process_parquet_file(
    event_file: str, *, temp_dir: str, num_shards: int, time_formats: Iterable[str], metadata_columns: List[str]
):
    """Partition MEDS Flat files into shards based on patient ID and write to disk"""
    logging.info("Working on ", event_file)

    table = pl.scan_parquet(event_file)

    cleaned_event_file = os.path.basename(event_file).split(".")[0]
    fname = f"{cleaned_event_file}.parquet"
    create_and_write_shards_from_table(table, temp_dir, num_shards, time_formats, metadata_columns, fname)


def process_csv_file(
    event_file: str,
    *,
    decompressed_dir: str,
    temp_dir: str,
    num_shards: int,
    time_formats: Iterable[str],
    metadata_columns: List[str],
):
    """Convert a MEDS Flat CSV file to MEDS."""
    logging.info("Working on ", event_file)

    with load_file(decompressed_dir, event_file) as temp_f:
        table = pl.read_csv_batched(temp_f.name, infer_schema_length=0, batch_size=1_000_000)

        batch_index = 0

        while True:
            batch_index += 1

            batch = table.next_batches(1)
            if batch is None:
                break

            batch = batch[0]

            batch = batch.lazy()

            cleaned_event_file = os.path.basename(event_file).split(".")[0]
            fname = f"{cleaned_event_file}_{batch_index}.parquet"

            create_and_write_shards_from_table(batch, temp_dir, num_shards, time_formats, metadata_columns, fname)


def convert_flat_to_meds_polars(
    source_flat_path: str,
    target_meds_path: str,
    num_shards: int = 100,
    num_proc: int = 1,
    time_formats: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d"),
) -> None:
    """A polars implementation of convert_flat_to_meds"""
    if not os.path.exists(source_flat_path):
        raise ValueError(f'The source MEDS Flat folder ("{source_flat_path}") does not seem to exist?')

    os.makedirs(target_meds_path, exist_ok=True)

    events = []

    decompressed_dir = os.path.join(target_meds_path, "decompressed")
    os.mkdir(decompressed_dir)

    temp_dir = os.path.join(target_meds_path, "temp")
    os.mkdir(temp_dir)

    # Make a folder called `{shard_index}` for each `shard_index` within `temp_dir`
    for shard_index in range(num_shards):
        os.mkdir(os.path.join(temp_dir, str(shard_index)))

    if os.path.exists(os.path.join(source_flat_path, "metadata.json")):
        shutil.copyfile(
            os.path.join(source_flat_path, "metadata.json"), os.path.join(target_meds_path, "metadata.json")
        )

    csv_tasks = []
    parquet_tasks = []

    source_flat_data_path = os.path.join(source_flat_path, "flat_data")

    for flat_file in os.listdir(source_flat_data_path):
        full_flat_file = os.path.join(source_flat_data_path, flat_file)
        if full_flat_file.endswith(".csv") or full_flat_file.endswith(".csv.gz"):
            csv_tasks.append(full_flat_file)
        else:
            parquet_tasks.append(full_flat_file)

    random.seed(3422342)
    random.shuffle(csv_tasks)
    random.shuffle(parquet_tasks)

    if num_proc != 1:
        with mp.get_context("spawn").Pool(num_proc) as pool:
            metadata_columns_info = dict()
            for columns_info in pool.imap_unordered(get_csv_columns, csv_tasks):
                # TODO: Need to verify that types are consistent across files
                metadata_columns_info.update(columns_info)
            for columns_info in pool.imap_unordered(get_parquet_columns, parquet_tasks):
                metadata_columns_info.update(columns_info)

            metadata_columns_set = metadata_columns_info.keys() - KNOWN_COLUMNS
            # TODO: Ideally we'd support non-text metdata columns but for now we assert all metadata are strings
            assert all(
                [metadata_columns_info[k] in ("string", "large_string") for k in metadata_columns_set]
            ), "All metadata must be string type"
            metadata_columns = sorted(list(metadata_columns_set))

            csv_processor = functools.partial(
                process_csv_file,
                decompressed_dir=decompressed_dir,
                temp_dir=temp_dir,
                num_shards=num_shards,
                time_formats=time_formats,
                metadata_columns=metadata_columns,
            )

            parquet_processor = functools.partial(
                process_parquet_file,
                temp_dir=temp_dir,
                num_shards=num_shards,
                time_formats=time_formats,
                metadata_columns=metadata_columns,
            )

            print("Partitioning MEDS Flat files into shards based on patient ID and writing to disk...")
            with tqdm(total=len(csv_tasks)) as pbar:
                for _ in pool.imap_unordered(csv_processor, csv_tasks):
                    pbar.update()
            with tqdm(total=len(parquet_tasks)) as pbar:
                for _ in pool.imap_unordered(parquet_processor, parquet_tasks):
                    pbar.update()
    else:
        metadata_columns_info = dict()
        for task in csv_tasks:
            metadata_columns_info.update(get_csv_columns(task))
        for task in parquet_tasks:
            metadata_columns_info.update(get_parquet_columns(task))

        metadata_columns_set = metadata_columns_info.keys() - KNOWN_COLUMNS
        # TODO: Ideally we'd support non-text metdata columns but for now we assert all metadata are strings
        assert all(
            [metadata_columns_info[k] in ("string", "large_string") for k in metadata_columns_set]
        ), "All metadata must be string type"
        metadata_columns = sorted(list(metadata_columns_set))

        csv_processor = functools.partial(
            process_csv_file,
            decompressed_dir=decompressed_dir,
            temp_dir=temp_dir,
            num_shards=num_shards,
            time_formats=time_formats,
            metadata_columns=metadata_columns,
        )
        parquet_processor = functools.partial(
            process_parquet_file,
            temp_dir=temp_dir,
            num_shards=num_shards,
            time_formats=time_formats,
            metadata_columns=metadata_columns,
        )

        for task in tqdm(csv_tasks, total=len(csv_tasks)):
            csv_processor(task)
        for task in tqdm(parquet_tasks, total=len(parquet_tasks)):
            parquet_processor(task)

    shutil.rmtree(decompressed_dir)

    logging.info("Processing each shard")

    data_dir = os.path.join(target_meds_path, "data")
    os.mkdir(data_dir)

    print("Collating source table data, shard by shard, to create patient timelines...")
    print("(Gathering measurements into events, events into timelines)")
    for shard_index in tqdm(range(num_shards), total=num_shards):
        logging.info("Processing shard ", shard_index)
        shard_dir = os.path.join(temp_dir, str(shard_index))

        if len(os.listdir(shard_dir)) == 0:
            continue

        events = [pl.scan_parquet(os.path.join(shard_dir, a)) for a in os.listdir(shard_dir)]

        all_events = pl.concat(events)

        measurement = pl.struct(
            code=pl.col("code"),
            text_value=pl.col("text_value"),
            numeric_value=pl.col("numeric_value"),
            datetime_value=pl.col("datetime_value"),
            metadata=pl.col("metadata"),
        )

        grouped_by_time = all_events.group_by("patient_id", "time").agg(measurements=measurement)

        event = pl.struct(
            pl.col("time"),
            pl.col("measurements"),
        )

        grouped_by_patient = grouped_by_time.group_by("patient_id").agg(events=event.sort_by(pl.col("time")))

        # We need to add a dummy column for static_measurements
        grouped_by_patient = grouped_by_patient.with_columns(static_measurements=pl.lit(None)).select(
            ["patient_id", "static_measurements", "events"]
        )

        # We now have our data in the final form, grouped_by_patient, but we have to do one final transformation
        # We have to convert from polar's large_list to list because large_list is not supported by huggingface

        # We do this conversion using the pyarrow library

        # Save and load our data in order to convert to pyarrow library
        converted = grouped_by_patient.collect().to_arrow()

        # Now we need to reconstruct the schema
        if len(metadata_columns) == 0:
            metadata_schema = pa.null()
        else:
            metadata_schema = pa.struct([(column, pa.string()) for column in metadata_columns])

        desired_schema = meds.patient_schema(metadata_schema)

        # All the larg_lists are now converted to lists, so we are good to load with huggingface
        casted = converted.cast(desired_schema)

        pq.write_table(casted, os.path.join(data_dir, f"data_{shard_index}.parquet"))

    shutil.rmtree(temp_dir)


def convert_flat_to_meds_duckdb(
    source_flat_path: str,
    target_meds_path: str,
    num_shards: int = 1,
    num_proc: int = 1,
    memory_limit_GB: Optional[int] = 16,
    time_formats: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d"),
) -> None:
    """A duckdb implementation of convert_flat_to_meds"""

    print("Converting from MEDS Flat to MEDS using DuckDB with sharding...")

    try:
        conn = duckdb.connect()
    except AttributeError as e:
        raise ImportError("You invoked `duckdb` as backend but duckdb is not installed. Install it using pip.") from e

    if not os.path.exists(source_flat_path):
        raise ValueError(f'The source MEDS Flat folder ("{source_flat_path}") does not seem to exist?')

    os.makedirs(target_meds_path, exist_ok=True)

    temp_dir = os.path.join(target_meds_path, "temp")
    os.mkdir(temp_dir)

    data_dir = os.path.join(target_meds_path, "data")
    os.mkdir(data_dir)

    if memory_limit_GB is None:
        # Set the memory limit to 95% of the available virtual memory by default
        memory_limit_GB = int(psutil.virtual_memory().total / 1e9 * 0.95)

    conn.sql(f"SET threads to {num_proc}")
    conn.sql("SET enable_progress_bar = true;")
    conn.sql("SET preserve_insertion_order = false;")
    # See https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html#spilling-to-disk
    conn.sql(f"SET temp_directory = '{temp_dir}';")
    # See https://duckdb.org/docs/configuration/pragmas.html#memory-limit
    conn.sql(f"SET memory_limit = '{memory_limit_GB}GB';")

    if os.path.exists(os.path.join(source_flat_path, "metadata.json")):
        shutil.copyfile(
            os.path.join(source_flat_path, "metadata.json"), os.path.join(target_meds_path, "metadata.json")
        )

    csv_tasks = []
    parquet_tasks = []

    source_flat_data_path = os.path.join(source_flat_path, "flat_data")

    for flat_file in os.listdir(source_flat_data_path):
        full_flat_file = os.path.join(source_flat_data_path, flat_file)
        if full_flat_file.endswith(".csv") or full_flat_file.endswith(".csv.gz"):
            csv_tasks.append(full_flat_file)
        else:
            parquet_tasks.append(full_flat_file)

    random.seed(3422342)
    random.shuffle(csv_tasks)
    random.shuffle(parquet_tasks)

    all_views = []

    # Collect all columns with their types
    tasks = []
    all_columns_with_types = []
    for task in csv_tasks:
        all_columns_with_types.append(get_csv_columns(task))
        tasks.append(task)

    for task in parquet_tasks:
        all_columns_with_types.append(get_parquet_columns(task))
        tasks.append(task)

    metadata_columns_set = {cols for table in all_columns_with_types for cols in table}

    metadata_columns_set = metadata_columns_set - KNOWN_COLUMNS
    metadata_columns = sorted(list(metadata_columns_set))

    for i, (task, columns_dtypes) in enumerate(zip(tasks, all_columns_with_types)):
        select_parts = []

        select_parts.append("cast(time as timestamp) as time")
        select_parts.append("cast(patient_id as int64) as patient_id")
        select_parts.append("cast(code as string) as code")

        if "value" not in columns_dtypes.keys():
            select_parts.append("cast(datetime_value as timestamp) as datetime_value")
            select_parts.append("cast(numeric_value as float4) as numeric_value")
            select_parts.append("cast(text_value as string) as text_value")
        else:
            select_parts.append("try_cast(value as timestamp) as datetime_value")

            select_parts.append(
                """
                try_cast(
                    (case when (try_cast(value as timestamp) is null) then value else null end)
                    as float4
                ) as numeric_value
            """
            )

            select_parts.append(
                """
                cast(
                    (
                        case when (
                            (try_cast(value as timestamp) is null ) AND (try_cast(value as float4) is null)
                        ) then value else null end
                    )
                as string) as text_value
            """
            )

        for column in metadata_columns:
            if column in columns_dtypes.keys():
                # TODO: Support casting based on pyarrow type, currently breaks with type eg `large_string`
                # because `large_string` is not a supported duckdb datatype. Just need to add a mapping from
                # parquet-supported datatypes to duckdb-supported datatypes and cast accordingly. Then use:
                # select_parts.append(f'cast("{column}" as {columns_dtypes[column]}) as "{column}"')
                assert columns_dtypes[column] in ("string", "large_string"), "All metadata must be string type"
                select_parts.append(f'"{column}"')
            else:
                select_parts.append(f'cast(null as string) as "{column}"')

        full_query = ", ".join(select_parts)

        all_views.append(f"SELECT * FROM v{i}")
        conn.sql(f"CREATE VIEW v{i} as SELECT {full_query} FROM '{task}'")

    if len(all_views) > 500:
        # We need to compress down to less than 500 to avoid file issues
        print("There are too many source files, we need to consolidate some of them")

        new_views = []

        num_chunks = (len(all_views) + 500 - 1) // 500
        for chunk_index in tqdm(range(num_chunks)):
            partial_views = all_views[chunk_index * 500 : (chunk_index + 1) * 500]
            conn.sql(f"CREATE TABLE t{chunk_index} as {' UNION ALL '.join(partial_views)}")
            new_views.append(f"SELECT * FROM t{chunk_index}")

        all_views = new_views

    conn.sql(f"CREATE VIEW all_events as {' UNION ALL '.join(all_views)}")

    if len(metadata_columns) != 0:
        metadata_str = "{" + ", ".join(f""" '{k}': "{k}" """ for k in metadata_columns) + "}"
    else:
        metadata_str = "null"

    print(f"Using {num_shards} shards to collate patient timelines with DuckDB...")

    for shard_index in tqdm(range(num_shards), total=num_shards):
        # Could create a new column or view with precomputed hash(patient_id), but it's fast so ignoring for now
        shard_filter_query = f"""
            CREATE TEMPORARY VIEW shard_{shard_index}_events AS
            SELECT *
            FROM all_events
            WHERE hash(patient_id) % {num_shards} = {shard_index}
        """
        conn.sql(shard_filter_query)

        create_joined_view_for_shard_query = f"""
            CREATE VIEW shard_{shard_index}_joined AS
            SELECT patient_id, null as static_measurements,
                list({{'time': time, 'measurements': measurements}} ORDER BY time ASC) as events
            FROM (
                SELECT patient_id, time,
                    list({{
                        'code': code,
                        'text_value': text_value,
                        'numeric_value': numeric_value,
                        'datetime_value': datetime_value,
                        'metadata': {metadata_str}}}
                    ) as measurements
                FROM shard_{shard_index}_events
                GROUP BY patient_id, time
            )
            GROUP BY patient_id
        """
        conn.sql(create_joined_view_for_shard_query)
        shard_output_path = os.path.join(data_dir, f"data_{shard_index}.parquet")

        # See https://duckdb.org/docs/guides/import/parquet_export.html
        write_result_to_disk_query = f"COPY shard_{shard_index}_joined TO '{shard_output_path}' "
        # See https://duckdb.org/docs/sql/statements/copy.html#copy--to-options and
        # https://duckdb.org/docs/data/parquet/tips.html
        write_result_to_disk_query += "(FORMAT PARQUET)"
        # We could use PER_THREAD_OUTPUT true but then it creates directories for each shard_output_path
        # rather than just single file names and that introduces other complexities
        # write_result_to_disk_query += "(FORMAT PARQUET, PER_THREAD_OUTPUT true, FILE_SIZE_BYTES 1e6)"
        conn.sql(write_result_to_disk_query)

    shutil.rmtree(temp_dir)


def convert_flat_to_meds(
    source_flat_path: str,
    target_meds_path: str,
    num_shards: int = 100,
    num_proc: int = 1,
    memory_limit_GB: int = 16,
    time_formats: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d"),
    backend: str = "polars",
) -> None:
    """
    Args:
        source_flat_path (str): Path to directory where MEDS Flat files are stored, structured as follows:
            {source_flat_path}
            |-- metadata.json [OPTIONAL]
            |-- flat_data
                |-- {table_name}_{map_index}.parquet
        target_meds_path (str): Path to directory the MEDS files (converted from MEDS Flat) should be stored
        num_shards (str): Number of patient shards, more shards -> fewer patients per join, only relevant for polars
        num_proc (int): Number of parallel processes
        time_formats (str): Expected possible datetime formats in the `time` column.

    Returns:
        None
    """
    if backend == "polars":
        warnings.warn(
            "The Polars backend in MEDS-ETL works, but is very slow."
            + " We recommend users use either the cpp or duckdb backend. See the README for details."
        )

    if not os.path.exists(source_flat_path):
        raise ValueError(f'The source MEDS Flat folder ("{source_flat_path}") does not seem to exist?')

    if not os.path.exists(os.path.join(source_flat_path, "metadata.json")):
        raise ValueError(f'The source MEDS Flat folder ("{source_flat_path}") does not have a metadata file?')

    if not os.path.exists(os.path.join(source_flat_path, "flat_data")):
        raise ValueError(f'The source MEDS Flat folder ("{source_flat_path}") does not have a flat_data folder')

    if backend == "cpp":
        import meds_etl_cpp

        meds_etl_cpp.perform_etl(source_flat_path, target_meds_path, num_shards)
    elif backend == "duckdb":
        assert duckdb is not None
        convert_flat_to_meds_duckdb(
            source_flat_path,
            target_meds_path,
            num_shards=num_shards,
            num_proc=num_proc,
            time_formats=time_formats,
            memory_limit_GB=memory_limit_GB,
        )
    else:
        convert_flat_to_meds_polars(source_flat_path, target_meds_path, num_shards, num_proc, time_formats)


def convert_flat_to_meds_main():
    parser = argparse.ArgumentParser(prog="meds_etl_flat", description="Performs an ETL from MEDS Flat to MEDS")
    parser.add_argument("source_flat_path", type=str)
    parser.add_argument("target_meds_path", type=str)
    parser.add_argument("--num_shards", type=int, default=100)
    parser.add_argument("--num_proc", type=int, default=1)
    parser.add_argument("--memory_limit_GB", type=int, default=16)
    parser.add_argument("--backend", type=str, default="polars")
    args = parser.parse_args()
    convert_flat_to_meds(**vars(args))

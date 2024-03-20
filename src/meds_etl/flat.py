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
from typing import Iterable, List, Literal, Set, TextIO, Tuple, cast

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

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
    os.mkdir(target_flat_path)

    source_meds_data_path = os.path.join(source_meds_path, "data")
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


def get_csv_columns(event_file: str) -> Set[str]:
    if event_file.endswith(".gz"):
        with gzip.open(event_file, "rt") as f:
            reader = csv.reader(f)
            return set(next(reader))
    else:
        with open(event_file, "r") as f:
            reader = csv.reader(f)
            return set(next(reader))


def get_parquet_columns(event_file: str) -> Set[str]:
    schema = pq.read_schema(event_file)
    return set(schema.names)


def transform_metadata(d, metadata_columns):
    if len(metadata_columns) == 0:
        return pl.lit(None)

    cols = []
    for column in metadata_columns:
        val = d.get(column, pl.lit(None, dtype=pl.Utf8))
        cols.append(val.alias(column))

    return pl.struct(cols)


def verify_shard(shard, event_file):
    for important_column in ("patient_id", "time", "code"):
        rows_with_invalid_code = shard.filter(pl.col(important_column).is_null())
        if len(rows_with_invalid_code) != 0:
            error_message = f"Have rows with invalid {important_column} in {event_file}"
            logging.error(error_message)
            for row in rows_with_invalid_code:
                logging.error(str(row))
            raise ValueError(error_message)


def convert_generic_value_to_specific(generic_value: pl.Expr, time_formats) -> Tuple[pl.Expr, pl.Expr, pl.Expr]:
    generic_value = generic_value.str.strip_chars()

    datetime_value = pl.coalesce(
        [generic_value.str.to_datetime(time_format, strict=False, time_unit="us") for time_format in time_formats]
    )

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
    table = table.rename({c: c.lower() for c in table.columns})

    patient_id = pl.col("patient_id").cast(pl.Int64())
    time = pl.col("time")
    if table.schema["time"] == pl.Utf8():
        time = pl.coalesce(
            [time.str.to_datetime(time_format, strict=False, time_unit="us") for time_format in time_formats]
        )
    else:
        time = time.cast(pl.Datetime())

    code = pl.col("code").cast(pl.Utf8())

    if "value" in table.columns:
        for sub_value in ("datetime_value", "numeric_value", "text_value"):
            if sub_value in table.columns:
                raise ValueError("Cannot have both generic value and specific value columns")

        datetime_value, numeric_value, text_value = convert_generic_value_to_specific(pl.col("value"), time_formats)
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

    metadata = {}

    for colname in table.columns:
        if colname not in KNOWN_COLUMNS:
            metadata[colname] = pl.col(colname).cast(pl.Utf8())

    metadata = transform_metadata(metadata, metadata_columns)

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

    for (shard_index,), shard in event_data.items():
        verify_shard(shard, filename)

        fname = os.path.join(temp_dir, str(shard_index), filename)
        shard.write_parquet(fname, compression="uncompressed")


def process_parquet_file(
    event_file: str, *, temp_dir: str, num_shards: int, time_formats: Iterable[str], metadata_columns: List[str]
):
    """Convert a MEDS Flat parquet file to MEDS."""
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


def convert_flat_to_meds(
    source_flat_path: str,
    target_meds_path: str,
    num_shards: int = 100,
    num_proc: int = 1,
    time_formats: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d"),
) -> None:
    if not os.path.exists(source_flat_path):
        raise ValueError(f'The source MEDS Flat folder ("{source_flat_path}") does not seem to exist?')

    os.makedirs(target_meds_path, exist_ok=True)

    events = []

    decompressed_dir = os.path.join(target_meds_path, "decompressed")
    os.mkdir(decompressed_dir)

    temp_dir = os.path.join(target_meds_path, "temp")
    os.mkdir(temp_dir)

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
            metadata_columns_set = set()
            for columns in pool.imap_unordered(get_csv_columns, csv_tasks):
                metadata_columns_set |= columns
            for columns in pool.imap_unordered(get_parquet_columns, parquet_tasks):
                metadata_columns_set |= columns

            metadata_columns_set -= KNOWN_COLUMNS
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

            for _ in pool.imap_unordered(csv_processor, csv_tasks):
                pass
            for _ in pool.imap_unordered(parquet_processor, parquet_tasks):
                pass
    else:
        metadata_columns_set = set()
        for task in csv_tasks:
            metadata_columns_set |= get_csv_columns(task)
        for task in parquet_tasks:
            metadata_columns_set |= get_parquet_columns(task)

        metadata_columns_set -= KNOWN_COLUMNS
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

        for task in csv_tasks:
            csv_processor(task)
        for task in parquet_tasks:
            parquet_processor(task)

    shutil.rmtree(decompressed_dir)

    logging.info("Processing each shard")

    data_dir = os.path.join(target_meds_path, "data")
    os.mkdir(data_dir)

    for shard_index in range(num_shards):
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


def convert_flat_to_meds_main():
    parser = argparse.ArgumentParser(prog="meds_etl_flat", description="Performs an ETL from MEDS Flat to MEDS")
    parser.add_argument("source_flat_path", type=str)
    parser.add_argument("target_meds_path", type=str)
    parser.add_argument("--num_shards", type=int, default=100)
    parser.add_argument("--num_proc", type=int, default=1)
    args = parser.parse_args()
    convert_flat_to_meds(**vars(args))

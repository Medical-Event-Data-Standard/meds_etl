import argparse
import json
import multiprocessing
import os
import pickle
import random
import shutil
import subprocess
import tempfile
import functools

import jsonschema
import meds
import polars as pl
import pyarrow.parquet as pq

import meds_etl

from typing import Literal, Iterable, TextIO

def convert_meds_to_flat(source_meds_path: str, target_flat_path: str, num_proc: int = 1, format: Literal["csv", "parquet", "compressed_csv"] = "compressed_csv", time_format: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d")) -> None:

def convert_meds_to_flat_main():
    parser = argparse.ArgumentParser(prog="meds_reverse_etl_flat", description="Performs an ETL from MEDS to MEDS Flat")
    parser.add_argument("source_meds_path", type=str)
    parser.add_argument("target_flat_path", type=str)
    parser.add_argument("--num_proc", type=int, default=1)
    parser.add_argument("--format", type=str, default="compressed_csv", choices=["csv", "parquet", "compressed_csv"])
    args = parser.parse_args()
    convert_meds_to_flat(**args)

def load_file(decompressed_dir: str, fname: str) -> TextIO:
    if fname.endswith(".gz"):
        file = tempfile.NamedTemporaryFile(dir=decompressed_dir)
        subprocess.run(["gunzip", "-c", fname], stdout=file)
        return file
    else:
        return open(fname)

def process_parquet_file(event_file: str, *, temp_dir: str, num_shards: int):
    print("Working on ", event_file)

    table = pl.scan_parquet(event_file)

    table = table.rename({c: c.lower() for c in table.columns})

    patient_id = pl.col("patient_id")
    time = pl.col("time")
    
    code = pl.col("code")

    datetime_value = pl.col("datetime_value")

    numeric_value = pl.col("numeric_value")

    text_value = pl.col("text_value")

    metadata = {}

    for colname in table.columns:
        if colname in ("patient_id", "time", "code", "datetime_value", "numeric_value", "text_value"):
            continue
        metadata[colname] = pl.col(colname)

    def transform_metadata(d):
        return pl.struct([v.alias(k) for k, v in d.items()])

    metadata = transform_metadata(metadata)

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
        .partition_by("shard", as_dict=True, maintain_order=False)
    )

    for shard_index, shard in event_data.items():
        cleaned_event_file = os.path.basename(event_file).split('.')[0]
        fname = os.path.join(
            temp_dir, str(shard_index), f'{cleaned_event_file}.parquet'
        )
        shard.write_parquet(fname, compression="uncompressed")



def process_csv_file(event_file: str, *, decompressed_dir: str, temp_dir: str, num_shards: int, time_formats: Iterable[str]):
    print("Working on ", event_file)

    with load_file(decompressed_dir, event_file) as temp_f:
        table = pl.read_csv_batched(temp_f.name, infer_schema_length=0, batch_size=1_000_000)

        batch_index = 0

        while True:
            batch_index += 1

            batch = table.next_batches(1)
            if batch is None:
                break

            batch = batch[0]

            batch = batch.lazy().rename({c: c.lower() for c in batch.columns})

            patient_id = pl.col("patient_id").cast(pl.Int64)
            time = pl.col("time")
            
            
            time = pl.coalesce([
                time.str.to_datetime(time_format, strict=False) for time_format in time_formats
            ])

            code = pl.col("code")

            datetime_value = pl.coalesce([
                pl.col("datetime_value").str.to_datetime(time_format, strict=False) for time_format in time_formats
            ])

            numeric_value = pl.col("numeric_value").cast(pl.Float32, strict=False)

            text_value = pl.col("text_value")

            metadata = {}

            for colname in batch.columns:
                if colname in ("patient_id", "time", "code", "datetime_value", "numeric_value", "text_value"):
                    continue
                metadata[colname] = pl.col(colname)

            def transform_metadata(d):
                return pl.struct([v.alias(k) for k, v in d.items()])

            metadata = transform_metadata(metadata)

            batch = batch.filter(code.is_not_null())

            event_data = (
                batch.select(
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
                .partition_by("shard", as_dict=True, maintain_order=False)
            )

            for shard_index, shard in event_data.items():
                cleaned_event_file = os.path.basename(event_file).split('.')[0]
                fname = os.path.join(
                    temp_dir, str(shard_index), f'{cleaned_event_file}_{batch_index}.parquet'
                )
                shard.write_parquet(fname, compression="uncompressed")


def convert_flat_to_meds(source_flat_path: str, target_meds_path: str, num_shards: int = 100, num_proc: int = 1, time_formats: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d")) -> None:
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

    shutil.copyfile(os.path.join(source_flat_path, 'metadata.json'), os.path.join(target_meds_path, 'metadata.json'))

    csv_tasks = []
    parquet_tasks = []

    source_flat_data_path = os.path.join(source_flat_path, 'data')

    for flat_file in os.listdir(source_flat_data_path):
        full_flat_file = os.path.join(source_flat_data_path, flat_file)
        if full_flat_file.endswith('.csv') or full_flat_file.endswith('.csv.gz'):
            csv_tasks.append(full_flat_file)
        else:
            parquet_tasks.append(full_flat_file)

    random.seed(3422342)
    random.shuffle(csv_tasks)
    random.shuffle(parquet_tasks)

    csv_processor = functools.partial(process_csv_file, decompressed_dir=decompressed_dir, temp_dir=temp_dir, num_shards=num_shards, time_formats=time_formats)

    with multiprocessing.get_context("spawn").Pool(num_proc) as pool:
        for _ in pool.imap_unordered(csv_processor, csv_tasks):
            pass
    
    parquet_processor = functools.partial(process_parquet_file, temp_dir=temp_dir, num_shards=num_shards)

    with multiprocessing.get_context("spawn").Pool(num_proc) as pool:
        for _ in pool.imap_unordered(parquet_processor, parquet_tasks):
            pass

    shutil.rmtree(decompressed_dir)

    print("Processing each shard")

    data_dir = os.path.join(target_meds_path, "data")
    os.mkdir(data_dir)

    for shard_index in range(num_shards):
        print("Processing shard ", shard_index)
        shard_dir = os.path.join(temp_dir, str(shard_index))

        events = [pl.scan_parquet(os.path.join(shard_dir, a)) for a in os.listdir(shard_dir)]

        all_events = pl.concat(events, how="diagonal_relaxed")

        for important_column in ("patient_id", "time", "code"):
            rows_with_invalid_code = all_events.filter(pl.col(important_column).is_null()).collect()
            if len(rows_with_invalid_code) != 0:
                print("Have rows with invalid " + important_column)
                for row in rows_with_invalid_code:
                    print(row)
                raise ValueError("Cannot have rows with invalid " + important_column)

        measurement = pl.struct(
            code=pl.col("code"),
            text_value=pl.col("text_value"),
            numeric_value=pl.col("numeric_value"),
            datetime_value=pl.col("datetime_value"),
            metadata=pl.col("metadata"),
        )

        grouped_by_time = all_events.groupby("patient_id", "time").agg(measurements=measurement)

        event = pl.struct(
            pl.col("time"),
            pl.col("measurements"),
        )

        grouped_by_patient = grouped_by_time.groupby("patient_id").agg(events=event.sort_by(pl.col("time")))

        # We now have our data in the final form, grouped_by_patient, but we have to do one final transformation
        # We have to convert from polar's large_list to list because large_list is not supported by huggingface

        # We do this conversion using the pyarrow library

        # Save and load our data in order to convert to pyarrow library
        converted = grouped_by_patient.collect().to_arrow()

        # Now we need to reconstruct the schema
        # We do this by pulling the metadata schema and then using meds.patient_schema
        event_schema = converted.schema.field("events").type.value_type
        measurement_schema = event_schema.field("measurements").type.value_type
        metadata_schema = measurement_schema.field("metadata").type

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
    convert_flat_to_meds(**args)


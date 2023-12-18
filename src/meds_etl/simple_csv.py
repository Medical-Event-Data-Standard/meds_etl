import argparse
import json
import multiprocessing
import os
import pickle
import random
import shutil
import subprocess
import tempfile

import jsonschema
import meds
import polars as pl
import pyarrow.parquet as pq

import meds_etl


def load_file(decompressed_dir, fname):
    if fname.endswith(".gz"):
        file = tempfile.NamedTemporaryFile(dir=decompressed_dir)
        subprocess.run(["gunzip", "-c", fname], stdout=file)
        return file
    else:
        return open(fname)


def process_file(args):
    event_file, num_shards, temp_dir, decompressed_dir, index = args
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
            
            
            time = pl.coalesce(
                time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
                time.str.to_datetime("%Y-%m-%d", strict=False),
            )

            value = pl.col("value")
            code = pl.col("code")

            datetime_value = pl.coalesce(
                value.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
                value.str.to_datetime("%Y-%m-%d", strict=False),
            )
            numeric_value = value.cast(pl.Float32, strict=False)

            text_value = (
                pl.when(datetime_value.is_null() & numeric_value.is_null() & (value != pl.lit("")))
                .then(value)
                .otherwise(pl.lit(None, dtype=str))
            )

            metadata = {}

            for colname in batch.columns:
                if colname in ("patient_id", "time", "code", "value"):
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
                fname = os.path.join(
                    temp_dir, str(shard_index), f'{index}_{batch_index}.parquet'
                )
                shard.write_parquet(fname, compression="uncompressed")


def main():
    parser = argparse.ArgumentParser(prog="meds_etl_simple_csv", description="Performs an ETL from a simple flat CSV format to MEDS")
    parser.add_argument("src_csv", type=str)
    parser.add_argument("destination", type=str)
    parser.add_argument("--num_shards", type=int, default=100)
    parser.add_argument("--num_threads", type=int, default=1)
    args = parser.parse_args()

    if not os.path.exists(args.src_csv):
        raise ValueError(f'The source CSV folder ("{args.src_csv}") does not seem to exist?')

    os.makedirs(args.destination, exist_ok=True)

    events = []

    decompressed_dir = os.path.join(args.destination, "decompressed")
    os.mkdir(decompressed_dir)

    temp_dir = os.path.join(args.destination, "temp")
    os.mkdir(temp_dir)

    for shard_index in range(args.num_shards):
        os.mkdir(os.path.join(temp_dir, str(shard_index)))

    metadata = {
        "dataset_name": args.src_csv,
        "dataset_version": "unknown",
        "etl_name": "meds_etl.simple_csv",
        "etl_version": meds_etl.__version__,
    }

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata)

    with open(os.path.join(args.destination, "metadata.json"), "w") as f:
        json.dump(metadata, f)

    all_tasks = []

    random.seed(3422342)
    random.shuffle(all_tasks)

    for i, csv_file in enumerate(os.listdir(args.src_csv)):
        full_csv_file = os.path.join(args.src_csv, csv_file)
        all_tasks.append(
            (full_csv_file, args.num_shards, temp_dir, decompressed_dir, i)
        )

    with multiprocessing.get_context("spawn").Pool(args.num_threads) as pool:
        for _ in pool.imap_unordered(process_file, all_tasks):
            pass

    shutil.rmtree(decompressed_dir)

    print("Processing each shard")

    data_dir = os.path.join(args.destination, "data")
    os.mkdir(data_dir)

    for shard_index in range(args.num_shards):
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

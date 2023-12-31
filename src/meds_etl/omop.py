import argparse
import json
import multiprocessing
import os
import pickle
import random
import shutil
import subprocess
import tempfile
import sys

import jsonschema
import meds
import polars as pl
import pyarrow.parquet as pq

import meds_etl


def get_table_files(src_omop, table_name, table_details={}):
    if table_details.get("file_suffix"):
        table_name += "_" + table_details["file_suffix"]

    folder_name = os.path.join(src_omop, table_name)

    if os.path.exists(folder_name):
        # Is a folder
        return [os.path.join(folder_name, a) for a in os.listdir(folder_name)]
    elif os.path.exists(folder_name + ".csv"):
        return [folder_name + ".csv"]
    elif os.path.exists(folder_name + ".csv.gz"):
        return [folder_name + ".csv.gz"]
    else:
        return []


def load_file(decompressed_dir, fname):
    if fname.endswith(".gz"):
        file = tempfile.NamedTemporaryFile(dir=decompressed_dir)
        subprocess.run(["gunzip", "-c", fname], stdout=file)
        return file
    else:
        return open(fname)


def process_table(args):
    (
        table_file,
        table_name,
        all_table_details,
        num_shards,
        concept_id_map_data,
        concept_name_map_data,
        temp_dir,
        decompressed_dir,
        index,
    ) = args
    concept_id_map = pickle.loads(concept_id_map_data)
    concept_name_map = pickle.loads(concept_name_map_data)
    print("Working on ", table_file, table_name, all_table_details)

    if not isinstance(all_table_details, list):
        all_table_details = [all_table_details]

    with load_file(decompressed_dir, table_file) as temp_f:
        table = pl.read_csv_batched(temp_f.name, infer_schema_length=0, batch_size=1_000_000)

        batch_index = 0

        while True:
            batch_index += 1

            batch = table.next_batches(1)
            if batch is None:
                break

            batch = batch[0]

            batch = batch.lazy().rename({c: c.lower() for c in batch.columns})

            for i, table_details in enumerate(all_table_details):
                patient_id = pl.col("person_id").cast(pl.Int64)

                if table_name == "person":
                    time = pl.coalesce(
                        pl.col("birth_datetime").str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit='ms'),
                        pl.datetime(
                            pl.col("year_of_birth"),
                            pl.coalesce(pl.col("month_of_birth"), 1),
                            pl.coalesce(pl.col("day_of_birth"), 1),
                        ),
                    )
                else:
                    options = ["_start_datetime", "_start_date", "_datetime", "_date"]
                    options = [
                        pl.col(table_name + option) for option in options if table_name + option in batch.columns
                    ]
                    assert len(options) > 0, f"Could not find the time column {batch.columns}"
                    time = pl.coalesce(options)
                    time = pl.coalesce(
                        time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit='ms'),
                        time.str.to_datetime("%Y-%m-%d", strict=False, time_unit='ms').dt.offset_by("1d").dt.offset_by("-1s"),
                    )

                if table_details.get("force_concept_id"):
                    concept_id = pl.lit(table_details["force_concept_id"], dtype=pl.Int64)
                    source_concept_id = pl.lit(0, dtype=pl.Int64)
                else:
                    concept_id_field = table_details.get("concept_id_field", table_name + "_concept_id")
                    concept_id = pl.col(concept_id_field).cast(pl.Int64)
                    source_concept_id = pl.col(concept_id_field.replace("_concept_id", "_source_concept_id")).cast(
                        pl.Int64
                    )

                    fallback_concept_id = pl.lit(table_details.get("fallback_concept_id", None), dtype=pl.Int64)

                    concept_id = (
                        pl.when(source_concept_id != 0)
                        .then(source_concept_id)
                        .when(concept_id != 0)
                        .then(concept_id)
                        .otherwise(fallback_concept_id)
                    )

                code = concept_id.map_dict(concept_id_map)

                value = pl.lit(None, dtype=str)

                if table_details.get("string_value_field"):
                    value = pl.col(table_details["string_value_field"])
                if table_details.get("numeric_value_field"):
                    value = pl.coalesce(pl.col(table_details["numeric_value_field"]), value)

                if table_details.get("concept_id_value_field"):
                    concept_id_value = pl.col(table_details["concept_id_value_field"]).cast(pl.Int64)

                    # Normally we would prefer string or numeric value.
                    # But sometimes we get a value_as_concept_id with no string or numeric value.
                    # So we want to define a backup value here
                    #
                    # There are two reasons for this, each with different desired behavior:
                    # 1. OMOP defines a code with a maps to value relationship.
                    #      See https://www.ohdsi.org/web/wiki/doku.php?id=documentation:vocabulary:mapping
                    #      In this cases we generally just want to drop the value, as the data is in source_concept_id
                    # 2. The ETL has decided to put non-maps to value codes in observation for various reasons.
                    #      For instance STARR-OMOP puts shc_medical_hx in here
                    #      In this case, we generally want to create a string value with the source code value.

                    backup_value = (
                        pl.when((source_concept_id == 0) & (concept_id_value != 0))
                        .then(
                            # Source concept 0 indicates we need a backup value since it's not captured by the source
                            "SOURCE_CODE/"
                            + pl.col(concept_id_field.replace("_concept_id", "_source_value"))
                        )
                        .otherwise(
                            # Should be captured by the source concept id, so just map the value to a string.
                            concept_id_value.map_dict(concept_name_map)
                        )
                    )

                    value = pl.coalesce(value, backup_value)

                datetime_value = pl.coalesce(
                    value.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit='ms'),
                    value.str.to_datetime("%Y-%m-%d", strict=False, time_unit='ms'),
                )
                numeric_value = value.cast(pl.Float32, strict=False)

                text_value = (
                    pl.when(datetime_value.is_null() & numeric_value.is_null() & (value != pl.lit("")))
                    .then(value)
                    .otherwise(pl.lit(None, dtype=str))
                )

                metadata = {
                    "table": pl.lit(table_name, dtype=str),
                }

                if "visit_occurrence_id" in batch.columns:
                    metadata["visit_id"] = pl.col("visit_occurrence_id")

                if "unit_source_value" in batch.columns:
                    metadata["unit"] = pl.col("unit_source_value")

                if "load_table_id" in batch.columns:
                    metadata["clarity_table"] = pl.col("load_table_id")

                if "note_id" in batch.columns:
                    metadata["note_id"] = pl.col("note_id")

                if (table_name + "_end_datetime") in batch.columns:
                    end = pl.col(table_name + "_end_datetime")
                    end = pl.coalesce(
                        end.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit='ms'),
                        end.str.to_datetime("%Y-%m-%d", strict=False, time_unit='ms').dt.offset_by("1d").dt.offset_by("-1s"),
                    )
                    metadata["end"] = end

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
                        datetime_value=datetime_value,
                        numeric_value=numeric_value,
                        metadata=metadata,
                        shard=patient_id.hash(213345) % num_shards,
                    )
                    .collect()
                    .partition_by("shard", as_dict=True, maintain_order=False)
                )

                for shard_index, shard in event_data.items():
                    fname = os.path.join(
                        temp_dir, str(shard_index), f'{table_name.replace("/", "_")}_{index}_{batch_index}_{i}.parquet'
                    )
                    shard.write_parquet(fname, compression="uncompressed")


def main():
    parser = argparse.ArgumentParser(prog="meds_etl_omop", description="Performs an ETL from OMOP v5 to MEDS")
    parser.add_argument("src_omop", type=str)
    parser.add_argument("destination", type=str)
    parser.add_argument("--num_shards", type=int, default=100)
    parser.add_argument("--num_proc", type=int, default=1)
    args = parser.parse_args()

    if not os.path.exists(args.src_omop):
        raise ValueError(f'The source OMOP folder ("{args.src_omop}") does not seem to exist?')

    os.makedirs(args.destination, exist_ok=True)

    events = []

    decompressed_dir = os.path.join(args.destination, "decompressed")
    os.mkdir(decompressed_dir)

    temp_dir = os.path.join(args.destination, "temp")
    os.mkdir(temp_dir)

    for shard_index in range(args.num_shards):
        os.mkdir(os.path.join(temp_dir, str(shard_index)))

    concept_id_map = {}
    concept_name_map = {}

    code_metadata = {}

    for concept_file in get_table_files(args.src_omop, "concept"):
        with load_file(decompressed_dir, concept_file) as f:
            concept = pl.read_csv(f.name)
            concept_id = pl.col("concept_id").cast(pl.Int64)
            code = pl.col("vocabulary_id") + "/" + pl.col("concept_code")
            result = concept.select(concept_id=concept_id, code=code, name=pl.col("concept_name")).to_dict(
                as_series=False
            )
            concept_id_map |= dict(zip(result["concept_id"], result["code"]))
            concept_name_map |= dict(zip(result["concept_id"], result["name"]))

            custom_concepts = (
                concept.filter(concept_id > 2_000_000_000)
                .select(code=code, description=pl.col("concept_name"))
                .to_dict()
            )
            for i in range(len(custom_concepts["code"])):
                code_metadata[custom_concepts["code"][i]] = {
                    "description": custom_concepts["description"][i],
                    "parent_codes": [],
                }
    omop_birth = None
    omop_death = None

    for concept_id, code in concept_id_map.items():
        if code == meds.birth_code:
            omop_birth = concept_id
        elif code == meds.death_code:
            omop_death = concept_id

    for concept_relationship_file in get_table_files(args.src_omop, "concept_relationship"):
        with load_file(decompressed_dir, concept_relationship_file) as f:
            concept_relationship = pl.read_csv(f.name)

            concept_id_1 = pl.col("concept_id_1").cast(pl.Int64)
            concept_id_2 = pl.col("concept_id_2").cast(pl.Int64)

            custom_relationships = (
                concept_relationship.filter(
                    concept_id_1 > 2_000_000_000,
                    pl.col("relationship_id") == "Maps to",
                    concept_id_1 != concept_id_2,
                )
                .select(concept_id_1=concept_id_1, concept_id_2=concept_id_2)
                .to_dict(as_series=False)
            )

            for concept_id_1, concept_id_2 in zip(
                custom_relationships["concept_id_1"], custom_relationships["concept_id_2"]
            ):
                if concept_id_1 in concept_id_map and concept_id_2 in concept_id_map:
                    code_metadata[concept_id_map[concept_id_1]]["parent_codes"].append(concept_id_map[concept_id_2])

    datasets = []
    dataset_versions = []
    for cdm_source_file in get_table_files(args.src_omop, "cdm_source"):
        with load_file(decompressed_dir, cdm_source_file) as f:
            cdm_source = pl.read_csv(f.name)
            cdm_source = cdm_source.rename({c: c.lower() for c in cdm_source.columns})
            cdm_source = cdm_source.to_dict(as_series=False)

            datasets.extend(cdm_source["cdm_source_name"])
            dataset_versions.extend(cdm_source["cdm_release_date"])

    metadata = {
        "dataset_name": "|".join(datasets),
        "dataset_version": "|".join(dataset_versions),
        "etl_name": "meds_etl.omop",
        "etl_version": meds_etl.__version__,
        "code_metadata": code_metadata,
    }

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata)

    with open(os.path.join(args.destination, "metadata.json"), "w") as f:
        json.dump(metadata, f)

    all_tasks = []

    tables = {
        "person": [
            {
                "force_concept_id": omop_birth,
            },
            {
                "concept_id_field": "gender_concept_id",
            },
            {
                "concept_id_field": "race_concept_id",
            },
            {
                "concept_id_field": "ethnicity_concept_id",
            },
        ],
        "drug_exposure": {
            "concept_id_field": "drug_concept_id",
        },
        "visit": {"fallback_concept_id": 8, "file_suffix": "occurrence"},
        "condition": {
            "file_suffix": "occurrence",
        },
        "death": {
            "force_concept_id": omop_death,
        },
        "procedure": {
            "file_suffix": "occurrence",
        },
        "device_exposure": {
            "concept_id_field": "device_concept_id",
        },
        "measurement": {
            "string_value_field": "value_source_value",
            "numeric_value_field": "value_as_number",
            "concept_id_value_field": "value_as_concept_id",
        },
        "observation": {
            "string_value_field": "value_as_string",
            "numeric_value_field": "value_as_number",
            "concept_id_value_field": "value_as_concept_id",
        },
        "note": {
            "fallback_concept_id": 46235038,
            "concept_id_field": "note_class_concept_id",
            "string_value_field": "note_text",
        },
        "visit_detail": {
            "fallback_concept_id": 4203722,
        },
    }

    concept_id_map_data = pickle.dumps(concept_id_map)
    concept_name_map_data = pickle.dumps(concept_name_map)

    for table_name, table_details in tables.items():
        table_files = get_table_files(
            args.src_omop, table_name, table_details[0] if isinstance(table_details, list) else table_details
        )

        all_tasks.extend(
            (
                table_file,
                table_name,
                table_details,
                args.num_shards,
                concept_id_map_data,
                concept_name_map_data,
                temp_dir,
                decompressed_dir,
                i,
            )
            for i, table_file in enumerate(table_files)
        )

    random.seed(3422342)
    random.shuffle(all_tasks)

    if True:
        with multiprocessing.get_context("spawn").Pool(args.num_proc) as pool:
            for _ in pool.imap_unordered(process_table, all_tasks):
                pass
    else:
        for task in all_tasks[:100]:
            process_table(task)

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

        # All the large_lists are now converted to lists, so we are good to load with huggingface
        casted = converted.cast(desired_schema)

        pq.write_table(casted, os.path.join(data_dir, f"data_{shard_index}.parquet"))

    shutil.rmtree(temp_dir)

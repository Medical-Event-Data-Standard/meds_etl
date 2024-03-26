import argparse
import json
import multiprocessing
import os
import pickle
import random
import shutil
import subprocess
import tempfile
from typing import Any, Dict, List, Tuple

import jsonschema
import meds
import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm

import meds_etl
import meds_etl.flat

RANDOM_SEED: int = 3422342
# OMOP constants
CUSTOMER_CONCEPT_ID_START: int = 2_000_000_000
DEFAULT_VISIT_CONCEPT_ID: int = 8
DEFAULT_NOTE_CONCEPT_ID: int = 46235038
DEFAULT_VISIT_DETAIL_CONCEPT_ID: int = 4203722

def get_table_files(path_to_src_omop_dir: str, table_name: str, table_details={}) -> List[str]:
    """Retrieve all .csv/.csv.gz files for the OMOP table given by `table_name` in `path_to_src_omop_dir`

    Because OMOP tables can be quite large for datasets comprising millions
    of patients, those tables are often split into compressed shards. So
    the `measurements` "table" might actually be a folder containing files
    `000000000000.csv.gz` up to `000000000152.csv.gz`. This function
    takes a path corresponding to an OMOP table with its standard name (e.g.,
    `condition_occurrence`, `measurement`, `observation`) and returns a list
    of paths, each of which represents a file e.g., 
    [`measurement/000000000000.csv.gz`, `000000000001.csv.gz`, ..., `000000000152.csv.gz`]
    """
    if table_details.get("file_suffix"):
        table_name += "_" + table_details["file_suffix"]

    path_to_table: str = os.path.join(path_to_src_omop_dir, table_name)

    if os.path.exists(path_to_table) and os.path.isdir(path_to_table):
        return [os.path.join(path_to_table, a) 
                for a in os.listdir(path_to_table)
                if a.endswith(".csv") or a.endswith(".csv.gz")]
    elif os.path.exists(path_to_table + ".csv"):
        return [path_to_table + ".csv"]
    elif os.path.exists(path_to_table + ".csv.gz"):
        return [path_to_table + ".csv.gz"]
    else:
        return []


def load_file(path_to_decompressed_dir: str, fname: str) -> Any:
    """Load a file from disk, unzip into temporary decompressed directory if needed

    Args: 
        path_to_decompressed_dir (str): Path where (temporary) decompressed file should be stored
        fname (str): Path to the (input) file that should be loaded from disk

    Returns:
        Opened file object
    """
    if fname.endswith(".gz"):
        file = tempfile.NamedTemporaryFile(dir=path_to_decompressed_dir)
        result = subprocess.run(["gunzip", "-c", fname], stdout=file)
        if result.returncode != 0:
            raise ValueError(f"Failed to decompress the file: `{fname}`. Please double check that the CLI utility `gunzip` is installed.")
        return file
    else:
        # If the file isn't compressed, we don't write anything to `path_to_decompressed_dir`
        return open(fname)


def process_table(args):
    (
        table_file,
        table_name,
        all_table_details,
        concept_id_map_data,
        concept_name_map_data,
        path_to_MEDS_flat_dir,
        path_to_decompressed_dir,
        map_index,
        verbose
    ) = args
    """

    This function is designed to be called through parallel processing utilities
    such as `pool.imap_unordered(process_table, [args1, args2, ..., argsN])

    Args:
        args (tuple): A tuple with the following elements:
            table_file (str): Path to the raw source table file (can be compressed)
            table_name (str): Name of the source table. Used for table-specific preprocessing
                and selecting columns which often have the table name embedded within them
                such as `measurement_datetime` in the `measurement` table.
            all_table_details (List[Dict[str, Any]]): A list of details about the particular
                table being processed that help determine how to map from the raw OMOP data
                to the MEDS standard.
        
    """
    concept_id_map = pickle.loads(concept_id_map_data)  # 0.25 GB for STARR-OMOP
    concept_name_map = pickle.loads(concept_name_map_data)  # 0.5GB for STARR-OMOP
    if verbose:
        print("Working on ", table_file, table_name, all_table_details)

    if not isinstance(all_table_details, list):
        all_table_details = [all_table_details]

    # Load the source table, decompress if needed
    with load_file(path_to_decompressed_dir, table_file) as temp_f:
        # Read it in batches so we don't max out RAM
        table = pl.read_csv_batched(
            temp_f.name,  # The decompressed source table
            infer_schema_length=0,  # Don't try to automatically infer schema
            batch_size=1_000_000  # Read in 1M rows at a time
        )

        batch_index = 0

        while True:  # We read until there are no more batches
            batch_index += 1
            batch = table.next_batches(1)  # Returns a list of length 1 containing a table for the batch
            if batch is None:
                break

            batch = batch[0]  # (Because `table.next_batches` returns a list)

            batch = batch.lazy().rename({c: c.lower() for c in batch.columns})

            for i, table_details in enumerate(all_table_details):
                if verbose:
                    print(f"Batch {i}")

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # Determine what to use for the `patient_id` column in MEDS Flat  #
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                
                # Define the MEDS Flat `patient_id` (left) to be the `person_id` columns in OMOP (right)
                patient_id = pl.col("person_id").cast(pl.Int64)

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # Determine what to use for the `time` column in MEDS Flat  #
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

                # Use special processing for the `PERSON` OMOP table
                if table_name == "person":
                    # Take the `birth_datetime` if its available, otherwise 
                    # construct it from `year_of_birth`, `month_of_birth`, `day_of_birth`
                    time = pl.coalesce(
                        pl.col("birth_datetime").str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="ms"),
                        pl.datetime(
                            pl.col("year_of_birth"),
                            pl.coalesce(pl.col("month_of_birth"), 1),
                            pl.coalesce(pl.col("day_of_birth"), 1),
                        ),
                    )
                else:
                    # Use the OMOP table name + `_start_datetime` as the `time` column 
                    # if it's available otherwise `_start_date`, `_datetime`, `_date` 
                    # in that order of preference
                    options = ["_start_datetime", "_start_date", "_datetime", "_date"]
                    options = [
                        pl.col(table_name + option) for option in options if table_name + option in batch.columns
                    ]
                    assert len(options) > 0, f"Could not find the time column {batch.columns}"
                    time = pl.coalesce(options)

                    # Try to cast time to a datetime but if only the date is available, then use
                    # that date with a timestamp of 23:59:59
                    time = pl.coalesce(
                        time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="ms"),
                        time.str.to_datetime("%Y-%m-%d", strict=False, time_unit="ms")
                        .dt.offset_by("1d")
                        .dt.offset_by("-1s"),
                    )

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # Determine what to use for the `code` column in MEDS Flat  #
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                
                if table_details.get("force_concept_id"):
                    # Rather than getting the concept ID from the source table, we use the concept ID
                    # passed in by the user eg `4083587` for `Birth`
                    concept_id = pl.lit(table_details["force_concept_id"], dtype=pl.Int64)
                    source_concept_id = pl.lit(0, dtype=pl.Int64)
                else:
                    # Try using the source concept ID, but if it's not available then use the concept ID 
                    concept_id_field = table_details.get("concept_id_field", table_name + "_concept_id")
                    concept_id = pl.col(concept_id_field).cast(pl.Int64)
                    if concept_id_field.replace("_concept_id", "_source_concept_id") in batch.columns:
                        source_concept_id = pl.col(concept_id_field.replace("_concept_id", "_source_concept_id")).cast(
                            pl.Int64
                        )
                    else:
                        source_concept_id = pl.lit(0, dtype=pl.Int64)
                    
                    # And if the source concept ID and concept ID aren't available, use `fallback_concept_id`
                    fallback_concept_id = pl.lit(table_details.get("fallback_concept_id", None), dtype=pl.Int64)

                    concept_id = (
                        pl.when(source_concept_id != 0)
                        .then(source_concept_id)
                        .when(concept_id != 0)
                        .then(concept_id)
                        .otherwise(fallback_concept_id)
                    )

                # Replace values in `concept_id` with the normalized concepts to which they are mapped
                # based on the `concept_id_map`
                code = concept_id.replace(concept_id_map, default=None)
                
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # Determine what to use for the `value` column in MEDS Flat   #
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

                # By default, if the user doesn't specify in `table_details` 
                # what the 
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
                            concept_id_value.replace(concept_name_map, default=None)
                        )
                    )

                    value = pl.coalesce(value, backup_value)

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # Determine the metadata columns to be stored in MEDS Flat  #
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                
                # Every key in the `metadata` dictionary will become a distinct
                # column in the MEDS Flat file; for each event, the metadata column
                # and its corresponding value for a given patient will be stored
                # as event metadata in the MEDS representation
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
                        end.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="ms"),
                        end.str.to_datetime("%Y-%m-%d", strict=False, time_unit="ms")
                        .dt.offset_by("1d")
                        .dt.offset_by("-1s"),
                    )
                    metadata["end"] = end

                batch = batch.filter(code.is_not_null())

                columns = {
                    "patient_id": patient_id,
                    "time": time,
                    "code": code,
                    "value": value,
                }

                # Add metadata columns to the set of MEDS flat dataframe columns
                for k, v in metadata.items():
                    columns[k] = v.alias(k)

                # We don't worry about partitioning here; let `flat_to_meds` handle that
                event_data = batch.select(**columns).collect()

                # Write this part of the MEDS Flat file to disk
                fname = os.path.join(
                    path_to_MEDS_flat_dir, 
                    f'{table_name.replace("/", "_")}_{map_index}_{batch_index}.parquet'
                )
                event_data.write_parquet(fname)


def process_shard(args: tuple[int, str, str]):
    """Collect measurements across OMOP table shards into MEDS patient timelines

    Args:
        args (tuple): A tuple which contains the following positional variables:
            shard_index (int): The shard index to which a subset of patient ID's are mapped,
                used in `path_to_temp_dir` (see  below)
            path_to_temp_dir (str): Path to the directory where sharded patient measurements are stored.
                Measurements will be *read* from files in the shard subfolders in this directory.
            path_to_data_dir (str): Path to the directory where MEDS timelines should be *written*.
                See below for expected file naming convention.

    Returns:
        Nothing, but MEDS timelines for the given `shard_index` are written 
        to "{path_to_data_dir}/data_{shard_index}.parquet" and can be read in eg
        as HuggingFace datasets.

    Raises:
        ValueError: If any important columns contain null/invalid values
    """
    shard_index, path_to_temp_dir, path_to_data_dir = args
    path_to_shard_dir: str = os.path.join(path_to_temp_dir, str(shard_index))

    # (Lazily) concatenate all OMOP table entries for the patients represented in the patient shard
    events = [pl.scan_parquet(os.path.join(path_to_shard_dir, a)) for a in os.listdir(path_to_shard_dir)]
    all_events = pl.concat(events, how="diagonal_relaxed")

    # Verify that all important columns have no null entries
    for important_column in ("patient_id", "time", "code"):
        rows_with_invalid_code = all_events.filter(pl.col(important_column).is_null()).collect()
        if len(rows_with_invalid_code) != 0:
            print("Have rows with invalid " + important_column)
            for row in rows_with_invalid_code:
                print(row)
            raise ValueError("Cannot have rows with invalid " + important_column)

    # Aggregate measurements into events (lazy, not executed until `collect()`)
    measurement = pl.struct(
        code=pl.col("code"),
        text_value=pl.col("text_value"),
        numeric_value=pl.col("numeric_value"),
        datetime_value=pl.col("datetime_value"),
        metadata=pl.col("metadata"),
    )

    grouped_by_time = all_events.groupby("patient_id", "time").agg(measurements=measurement)

    # Aggregate events into patient timelines (lazy, not executed until `collect()`)
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
 
    pq.write_table(casted, os.path.join(path_to_data_dir, f"data_{shard_index}.parquet"))


def extract_metadata(
    path_to_src_omop_dir: str,
    path_to_decompressed_dir: str,
    verbose: int = 0
) -> Tuple:
    concept_id_map: Dict[int, str] = {} # [key] concept_id -> [value] concept_code
    concept_name_map: Dict[int, str] = {} # [key] concept_id -> [value] concept_name
    code_metadata: Dict[str, Any] = {} # [key] concept_code -> [value] metadata

    # Read in the OMOP `CONCEPT` table from disk
    # (see https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html#concepts)
    # and use it to generate metadata file as well as populate maps
    # from (concept ID -> concept code) and (concept ID -> concept name)
    for concept_file in tqdm(get_table_files(path_to_src_omop_dir, "concept"), desc="Generating metadata from OMOP `concept` table"):
        # Note: Concept table is often split into gzipped shards by default
        if verbose:
            print(f"----> Processing file: `{concept_file}`")
        with load_file(path_to_decompressed_dir, concept_file) as f:
            # Read the contents of the `concept` table shard
            # `load_file` will unzip the file into `path_to_decompressed_dir` if needed
            concept: pl.DataFrame = pl.read_csv(f.name)
            concept_id = pl.col("concept_id").cast(pl.Int64)
            code = pl.col("vocabulary_id") + "/" + pl.col("concept_code")

            # Convert the table into a dictionary
            result = concept.select(
                concept_id=concept_id, 
                code=code, 
                name=pl.col("concept_name")
            )
            
            result = result.to_dict(as_series=False)

            # Update our running dictionary with the concepts we read in from 
            # the concept table shard
            concept_id_map |= dict(zip(result["concept_id"], result["code"]))
            concept_name_map |= dict(zip(result["concept_id"], result["name"]))

            # Assuming custom concepts have concept_id > 2000000000 we create a
            # record for them in `code_metadata` with no parent codes. Such a
            # custom code could be eg `STANFORD_RACE/Black or African American`
            # with `concept_id` 2000039197
            custom_concepts = (
                concept.filter(concept_id > CUSTOMER_CONCEPT_ID_START)
                .select(concept_id=concept_id, code=code, description=pl.col("concept_name"))
                .to_dict()
            )
            for i in range(len(custom_concepts["code"])):
                code_metadata[custom_concepts["code"][i]] = {
                    "description": custom_concepts["description"][i],
                    "parent_codes": [],
                }

    # Find and store the Concept ID's used for Birth and Death
    omop_birth_concept_id = None
    omop_death_concept_id = None
    for concept_id, code in tqdm(concept_id_map.items(), desc="Finding birth and death codes"):
        if code == meds.birth_code:
            omop_birth_concept_id = concept_id
        elif code == meds.death_code:
            omop_death_concept_id = concept_id
        if omop_birth_concept_id is not None and omop_death_concept_id is not None:
            break

    # Include map from custom concepts to normalized (ie standard ontology) 
    # parent concepts, where possible, in the code_metadata dictionary
    for concept_relationship_file in get_table_files(path_to_src_omop_dir, "concept_relationship"):
        with load_file(path_to_decompressed_dir, concept_relationship_file) as f:
            # This table has `concept_id_1`, `concept_id_2`, `relationship_id` columns
            concept_relationship = pl.read_csv(f.name)

            concept_id_1 = pl.col("concept_id_1").cast(pl.Int64)
            concept_id_2 = pl.col("concept_id_2").cast(pl.Int64)

            custom_relationships = (
                concept_relationship.filter(
                    concept_id_1 > CUSTOMER_CONCEPT_ID_START,
                    pl.col("relationship_id") == "Maps to",
                    concept_id_1 != concept_id_2,
                )
                .select(concept_id_1=concept_id_1, concept_id_2=concept_id_2)
                .to_dict(as_series=False)
            )

            for concept_id_1, concept_id_2 in zip(
                custom_relationships["concept_id_1"], 
                custom_relationships["concept_id_2"]
            ):
                if concept_id_1 in concept_id_map and concept_id_2 in concept_id_map:
                    code_metadata[concept_id_map[concept_id_1]]["parent_codes"].append(concept_id_map[concept_id_2])

    # Extract dataset metadata e.g., the CDM source name and its release date
    datasets: List[str] = []
    dataset_versions: List[str] = []
    for cdm_source_file in get_table_files(path_to_src_omop_dir, "cdm_source"):
        with load_file(path_to_decompressed_dir, cdm_source_file) as f:
            cdm_source = pl.read_csv(f.name)
            cdm_source = cdm_source.rename({c: c.lower() for c in cdm_source.columns})
            cdm_source = cdm_source.to_dict(as_series=False)

            datasets.extend(cdm_source["cdm_source_name"])
            dataset_versions.extend(cdm_source["cdm_release_date"])

    metadata = {
        "dataset_name": "|".join(datasets),  # eg 'Epic Clarity SHC|Epic Clarity LPCH'
        "dataset_version": "|".join(dataset_versions),  # eg '2024-02-01|2024-02-01'
        "etl_name": "meds_etl.omop",
        "etl_version": meds_etl.__version__,
        "code_metadata": code_metadata,  # `code_metadata` could have >100k keys
    }
    # At this point metadata['code_metadata']['STANFORD_MEAS/AMOXICILLIN/CLAVULANATE']
    # should give a dictionary like
    # {'description': 'AMOXICILLIN/CLAVULANATE', 'parent_codes': ['LOINC/18862-3']}
    # where LOINC Code '18862-3' is a standard concept representing a lab test 
    # measurement determining microorganism susceptibility to Amoxicillin+clavulanic acid

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata)

    return metadata, concept_id_map, concept_name_map, omop_birth_concept_id, omop_death_concept_id


def main():
    parser = argparse.ArgumentParser(
        prog="meds_etl_omop", 
        description="Performs an ETL from OMOP v5 to MEDS"
    )
    parser.add_argument(
        "path_to_src_omop_dir", 
        type=str,
        help="Path to the OMOP source directory, e.g. "
            "`~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_2023_11_19` "
            " for STARR-OMOP full or "
            "`~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_1pcent_2024_02_09`"
    )
    parser.add_argument(
        "path_to_dest_meds_dir",
        type=str,
        help="Path to where the output MEDS files will be stored"
    )
    parser.add_argument(
        "--num_shards", 
        type=int, 
        default=100,
        help="Number of shards to use for converting MEDS from the flat format "
             "to MEDS (patients are distributed approximately uniformly at "
             "random across shards and collation/joining of OMOP tables is "
             "performed on a shard-by-shard basis)."
    )
    parser.add_argument(
        "--num_proc", 
        type=int, 
        default=1,
        help="Number of vCPUs to use for performing the MEDS ETL"
    )
    parser.add_argument(
        "--engine", 
        type=str, 
        default='polars',
        choices=['polars', 'duckdb'],
        help="The engine to use for doing the conversion from MEDS Flat => MEDS (a large GROUP BY => AGG query). Current options are 'polars' or 'duckdb'. See the Github repo for a comparison between these approaches (i.e. memory v. speed tradeoffs)."
    )
    parser.add_argument(
        "--verbose", 
        type=int, 
        default=0
    )
    parser.add_argument(
        "--continue_job", 
        dest="continue_job", 
        action="store_true",
        help="If set, the job continues from a previous run, starting after the "
             "conversion to MEDS Flat but before converting from MEDS Flat to MEDS."
    )

    args = parser.parse_args()

    if not os.path.exists(args.path_to_src_omop_dir):
        raise ValueError(f'The source OMOP folder ("{args.path_to_src_omop_dir}") does not seem to exist?')

    # Create the directory where final MEDS files will go, if doesn't already exist
    os.makedirs(args.path_to_dest_meds_dir, exist_ok=True)
    
    # Within the target directory, create temporary subfolder for holding files
    # that need to be decompressed as part of the ETL (via eg `load_file`)
    path_to_decompressed_dir = os.path.join(args.path_to_dest_meds_dir, "decompressed")

    # Create temporary folder for storing MEDS Flat data within target directory
    path_to_temp_dir = os.path.join(args.path_to_dest_meds_dir, "temp")
    path_to_MEDS_flat_dir = os.path.join(path_to_temp_dir, "flat_data")

    if not args.continue_job or not (os.path.exists(path_to_MEDS_flat_dir) and os.path.exists(path_to_temp_dir)):
        if os.path.exists(path_to_temp_dir):
            if args.verbose: print(f"Deleting and recreating {path_to_temp_dir}")
            shutil.rmtree(path_to_temp_dir)
        os.mkdir(path_to_temp_dir)
        
        if os.path.exists(path_to_MEDS_flat_dir):
            if args.verbose: print(f"Deleting and recreating {path_to_MEDS_flat_dir}")
            shutil.rmtree(path_to_MEDS_flat_dir)
        os.mkdir(path_to_MEDS_flat_dir)

        if os.path.exists(path_to_decompressed_dir):
            if args.verbose: print(f"Deleting and recreating {path_to_decompressed_dir}")
            shutil.rmtree(path_to_decompressed_dir)
        os.mkdir(path_to_decompressed_dir)

        # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Generate metadata.json from OMOP concept table  #
        # # # # # # # # # # # # # # # # # # # # # # # # # #

        metadata, concept_id_map, concept_name_map, omop_birth_concept_id, omop_death_concept_id = extract_metadata(
            path_to_src_omop_dir=args.path_to_src_omop_dir,
            path_to_decompressed_dir=path_to_decompressed_dir,
            verbose=args.verbose
        )

        # Save the extracted metadata file to disk...
        # We save one copy in the MEDS Flat data directory
        with open(os.path.join(path_to_temp_dir, "metadata.json"), "w") as f:
            json.dump(metadata, f)
        # And we save another copy in the final/target MEDS directory
        with open(os.path.join(args.path_to_dest_meds_dir, "metadata.json"), "w") as f:
            json.dump(metadata, f)

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
        # Convert all "measurements" to MEDS Flat, write to disk  #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        # tables = retrieve_table_specifications(omop_birth_concept_id)

        tables: Dict[str, Any] = {
            # Each key is a `table_name`
            "person": [  # `table_name`s map to `table_details`
                {
                    "force_concept_id": omop_birth_concept_id,
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
            "visit": {
                "fallback_concept_id": DEFAULT_VISIT_CONCEPT_ID, 
                "file_suffix": "occurrence"
            },
            "condition": {
                "file_suffix": "occurrence",
            },
            "death": {
                "force_concept_id": omop_death_concept_id,
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
                "fallback_concept_id": DEFAULT_NOTE_CONCEPT_ID,
                "concept_id_field": "note_class_concept_id",
                "string_value_field": "note_text",
            },
            "visit_detail": {
                "fallback_concept_id": DEFAULT_VISIT_DETAIL_CONCEPT_ID,
            },
        }
        
        # Prepare concept_id_map and concept_name_map for parallel processing
        concept_id_map_data = pickle.dumps(concept_id_map)
        concept_name_map_data = pickle.dumps(concept_name_map)

        # Create a separate task for each table
        # Each subprocess will read in a decompressed file and put all measurements for a given patient
        # into that patient's corresponding shard. This makes creating patient timelines downstream
        # (where timelines incorporate measurements from across different tables) much less RAM intensive.
        all_tasks = []
        for table_name, table_details in tables.items():
            table_files = get_table_files(
                path_to_src_omop_dir=args.path_to_src_omop_dir, 
                table_name=table_name, 
                table_details=table_details[0] if isinstance(table_details, list) else table_details
            )

            all_tasks.extend(
                (
                    table_file,
                    table_name,
                    table_details,
                    concept_id_map_data,
                    concept_name_map_data,
                    path_to_MEDS_flat_dir,
                    path_to_decompressed_dir,
                    map_index,
                    args.verbose
                )
                for map_index, table_file in enumerate(table_files)
            )
        random.seed(RANDOM_SEED)
        random.shuffle(all_tasks)

        if args.num_proc > 1:
            with multiprocessing.get_context("spawn").Pool(args.num_proc, maxtasksperchild=1) as pool:
                # Wrap all tasks with tqdm for a progress bar
                total_tasks = len(all_tasks)
                with tqdm(total=total_tasks, desc="Converting OMOP tables => MEDS Flat") as pbar:
                    for _ in pool.imap_unordered(process_table, all_tasks):
                        pbar.update()

        else:
            # Useful for debugging `process_table` without multiprocessing
            for task in tqdm(all_tasks, desc="Converting OMOP tables => MEDS Flat"):
                process_table(task)

        # TODO: Do we need to do this more often so as to reduce maximum disk storage?
        shutil.rmtree(path_to_decompressed_dir)

        print("Finished converting OMOP => MEDS Flat.")

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Collate measurements into timelines for each patient, by shard
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    print("Start | Converting MEDS Flat => MEDS")
    meds_etl.flat.convert_flat_to_meds(
        source_flat_path=path_to_temp_dir,
        target_meds_path=os.path.join(args.path_to_dest_meds_dir, "result"),
        num_shards=args.num_shards,
        num_proc=args.num_proc,
        engine=args.engine,
    )
    print("Done | Converting MEDS Flat => MEDS")
    shutil.move(
        src=os.path.join(args.path_to_dest_meds_dir, "result", "data"), 
        dst=os.path.join(args.path_to_dest_meds_dir, "data")
    )
    shutil.rmtree(path=os.path.join(args.path_to_dest_meds_dir, "result"))

    print(f"Deleting temporary directory `{path_to_temp_dir}`")
    shutil.rmtree(path_to_temp_dir)

if __name__ == '__main__':
    main()

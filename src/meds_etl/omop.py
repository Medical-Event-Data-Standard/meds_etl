from __future__ import annotations

import argparse
import itertools
import json
import multiprocessing
import os
import pickle
import random
import shutil
import subprocess
import tempfile
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Tuple

import jsonschema
import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

import meds_etl
import meds_etl.unsorted
import meds_etl.utils

RANDOM_SEED: int = 3422342
# OMOP constants
CUSTOMER_CONCEPT_ID_START: int = 2_000_000_000
DEFAULT_VISIT_CONCEPT_ID: int = 8
DEFAULT_NOTE_CONCEPT_ID: int = 46235038
DEFAULT_VISIT_DETAIL_CONCEPT_ID: int = 4203722

OMOP_TIME_FORMATS: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d")


def split_list(ls: List[Any], parts: int) -> List[List[Any]]:
    per_list = (len(ls) + parts - 1) // parts
    result = []

    for i in range(parts):
        sublist = ls[i * per_list : (i + 1) * per_list]
        if len(sublist) > 0:
            result.append(sublist)

    return result


def get_table_files(path_to_src_omop_dir: str, table_name: str, table_details={}) -> Tuple[List[str], List[str]]:
    """Retrieve all .csv/.csv.gz/.parquet files for the OMOP table given by `table_name` in `path_to_src_omop_dir`

    Because OMOP tables can be quite large for datasets comprising millions
    of subjects, those tables are often split into compressed shards. So
    the `measurements` "table" might actually be a folder containing files
    `000000000000.csv.gz` up to `000000000152.csv.gz`. This function
    takes a path corresponding to an OMOP table with its standard name (e.g.,
    `condition_occurrence`, `measurement`, `observation`) and returns two list
    of paths.

    The first list contains all the csv files. The second list contains all parquet files.
    """
    if table_details.get("file_suffix"):
        table_name += "_" + table_details["file_suffix"]

    path_to_table: str = os.path.join(path_to_src_omop_dir, table_name)

    if os.path.exists(path_to_table) and os.path.isdir(path_to_table):
        csv_files = []
        parquet_files = []

        for a in os.listdir(path_to_table):
            fname = os.path.join(path_to_table, a)
            if a.endswith(".csv") or a.endswith(".csv.gz"):
                csv_files.append(fname)
            elif a.endswith(".parquet"):
                parquet_files.append(fname)

        return csv_files, parquet_files
    elif os.path.exists(path_to_table + ".csv"):
        return [path_to_table + ".csv"], []
    elif os.path.exists(path_to_table + ".csv.gz"):
        return [path_to_table + ".csv.gz"], []
    elif os.path.exists(path_to_table + ".parquet"):
        return [], [path_to_table + ".parquet"]
    else:
        return [], []


def read_polars_df(fname: str) -> pl.DataFrame:
    """Read a file that might be a CSV or Parquet file"""
    if fname.endswith(".csv"):
        # Don't try to infer schema because it can cause errors with columns that look like ints but aren't
        return pl.read_csv(fname, infer_schema=False)
    elif fname.endswith(".parquet"):
        return pl.read_parquet(fname)
    else:
        raise RuntimeError("Found file of unknown type " + fname + " expected parquet or csv")


def load_file(path_to_decompressed_dir: str, fname: str) -> Any:
    """Load a file from disk, unzip into temporary decompressed directory if needed

    Args:
        path_to_decompressed_dir (str): Path where (temporary) decompressed file should be stored
        fname (str): Path to the (input) file that should be loaded from disk

    Returns:
        Opened file object
    """
    if fname.endswith(".gz"):
        file = tempfile.NamedTemporaryFile(dir=path_to_decompressed_dir, suffix=".csv")
        subprocess.run(["gunzip", "-c", fname], stdout=file)
        return file
    else:
        # If the file isn't compressed, we don't write anything to `path_to_decompressed_dir`
        return open(fname)


def cast_to_datetime(schema: Any, column: str, move_to_end_of_day: bool = False):
    if schema[column] == pl.Utf8():
        if not move_to_end_of_day:
            return meds_etl.utils.parse_time(pl.col(column), OMOP_TIME_FORMATS)
        else:
            # Try to cast time to a datetime but if only the date is available, then use
            # that date with a timestamp of 23:59:59
            time = pl.col(column)
            time = pl.coalesce(
                time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="us"),
                time.str.to_datetime("%Y-%m-%d", strict=False, time_unit="us").dt.offset_by("1d").dt.offset_by("-1s"),
            )
            return time
    elif schema[column] == pl.Date():
        time = pl.col(column).cast(pl.Datetime(time_unit="us"))
        if move_to_end_of_day:
            time = time.dt.offset_by("1d").dt.offset_by("-1s")
        return time
    elif isinstance(schema[column], pl.Datetime):
        return pl.col(column).cast(pl.Datetime(time_unit="us"))
    else:
        raise RuntimeError("Unknown how to handle date type? " + schema[column] + " " + column)


def write_event_data(
    path_to_MEDS_unsorted_dir: str,
    get_batch: Any,
    table_name: str,
    all_table_details: Iterable[Mapping[str, Any]],
    concept_id_map: Mapping[int, str],
    concept_name_map: Mapping[int, str],
) -> pl.LazyFrame:
    """Write event data from the given table to event files in MEDS Unsorted format"""
    for table_details in all_table_details:
        batch = get_batch()

        batch = batch.rename({c: c.lower() for c in batch.collect_schema().names()})
        schema = batch.collect_schema()
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Determine what to use for the `subject_id` column in MEDS Unsorted  #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        # Define the MEDS Unsorted `subject_id` (left) to be the `person_id` columns in OMOP (right)
        subject_id = pl.col("person_id").cast(pl.Int64)

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Determine what to use for the `time`                          #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        # Use special processing for the `PERSON` OMOP table
        if table_name == "person":
            # Take the `birth_datetime` if its available, otherwise
            # construct it from `year_of_birth`, `month_of_birth`, `day_of_birth`
            time = pl.coalesce(
                cast_to_datetime(schema, "birth_datetime"),
                pl.datetime(
                    pl.col("year_of_birth"),
                    pl.coalesce(pl.col("month_of_birth"), pl.lit(1)),
                    pl.coalesce(pl.col("day_of_birth"), pl.lit(1)),
                    time_unit="us",
                ),
            )
        else:
            # Use the OMOP table name + `_start_datetime` as the `time` column
            # if it's available otherwise `_start_date`, `_datetime`, `_date`
            # in that order of preference
            options = ["_start_datetime", "_start_date", "_datetime", "_date"]
            options = [
                cast_to_datetime(schema, table_name + option, move_to_end_of_day=True)
                for option in options
                if table_name + option in schema.names()
            ]
            assert len(options) > 0, f"Could not find the time column {schema.names()}"
            time = pl.coalesce(options)

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Determine what to use for the `code` column                   #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        if table_details.get("force_concept_code"):
            # Rather than getting the concept ID from the source table, we use the concept ID
            # passed in by the user eg `4083587` for `Birth`
            source_concept_id = pl.lit(0, dtype=pl.Int64)
            code = pl.lit(table_details.get("force_concept_code"), dtype=pl.Utf8)
        else:
            # Try using the source concept ID, but if it's not available then use the concept ID
            concept_id_field = table_details.get("concept_id_field", table_name + "_concept_id")
            concept_id = pl.col(concept_id_field).cast(pl.Int64)
            if concept_id_field.replace("_concept_id", "_source_concept_id") in schema.names():
                source_concept_id = pl.col(concept_id_field.replace("_concept_id", "_source_concept_id")).cast(pl.Int64)
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
            code = concept_id.replace_strict(concept_id_map, return_dtype=pl.Utf8(), default=None)

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Determine what to use for the `value`                       #
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
                .when(concept_id_value != 0)
                .then("OMOP_CONCEPT_ID/" + concept_id_value.cast(pl.Utf8()))
                .otherwise(pl.lit(None, dtype=pl.Utf8()))
            )

            value = pl.coalesce(value, backup_value)

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Determine the metadata columns                            #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        # Every key in the `metadata` dictionary will become a distinct
        # column in the MEDS file; for each event, the metadata column
        # and its corresponding value for a given subject will be stored
        # as event metadata in the MEDS representation
        metadata = {
            "table": pl.lit(table_name, dtype=str),
        }

        if "visit_occurrence_id" in schema.names():
            metadata["visit_id"] = pl.col("visit_occurrence_id")

        unit_columns = []
        if "unit_source_value" in schema.names():
            unit_columns.append(pl.col("unit_source_value"))
        if "unit_concept_id" in schema.names():
            unit_columns.append(
                pl.col("unit_concept_id").replace_strict(concept_id_map, return_dtype=pl.Utf8(), default=None))
        if unit_columns:
            metadata["unit"] = pl.coalesce(unit_columns)

        if "load_table_id" in schema.names():
            metadata["clarity_table"] = pl.col("load_table_id")

        if "note_id" in schema.names():
            metadata["note_id"] = pl.col("note_id")

        if (table_name + "_end_datetime") in schema.names():
            end = cast_to_datetime(schema, table_name + "_end_datetime", move_to_end_of_day=True)
            metadata["end"] = end

        batch = batch.filter(code.is_not_null())

        columns = {
            "subject_id": subject_id,
            "time": time,
            "code": code,
        }

        n, t = meds_etl.utils.convert_generic_value_to_specific(value)

        columns["numeric_value"] = n
        columns["text_value"] = t

        # Add metadata columns to the set of MEDS dataframe columns
        for k, v in metadata.items():
            columns[k] = v.alias(k)

        event_data = batch.select(**columns)
        # Write this part of the MEDS Unsorted file to disk
        fname = os.path.join(path_to_MEDS_unsorted_dir, f'{table_name.replace("/", "_")}_{uuid.uuid4()}.parquet')
        try:
            event_data.sink_parquet(fname, compression="zstd", compression_level=1, maintain_order=False)
        except pl.exceptions.InvalidOperationError as e:
            print(table_name)
            print(e)
            print(event_data.explain(streaming=True))
            raise e


def process_table_csv(args):
    (
        table_file,
        table_name,
        all_table_details,
        concept_id_map_data,
        concept_name_map_data,
        path_to_MEDS_unsorted_dir,
        path_to_decompressed_dir,
        verbose,
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
            batch_size=1_000_000,  # Read in 1M rows at a time
        )

        batch_index = 0

        while True:  # We read until there are no more batches
            batch_index += 1
            batch = table.next_batches(1)  # Returns a list of length 1 containing a table for the batch
            if batch is None:
                break

            batch = batch[0]  # (Because `table.next_batches` returns a list)

            batch = batch.lazy()

            write_event_data(
                path_to_MEDS_unsorted_dir,
                lambda: batch.lazy(),
                table_name,
                all_table_details,
                concept_id_map,
                concept_name_map,
            )


def process_table_parquet(args):
    (
        table_files,
        table_name,
        all_table_details,
        concept_id_map_data,
        concept_name_map_data,
        path_to_MEDS_unsorted_dir,
        verbose,
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
        print("Working on ", table_files, table_name, all_table_details)

    if not isinstance(all_table_details, list):
        all_table_details = [all_table_details]

    # Load the source table, decompress if needed
    write_event_data(
        path_to_MEDS_unsorted_dir,
        lambda: pl.scan_parquet(table_files),
        table_name,
        all_table_details,
        concept_id_map,
        concept_name_map,
    )


def extract_metadata(path_to_src_omop_dir: str, path_to_decompressed_dir: str, verbose: int = 0) -> Tuple:
    concept_id_map: Dict[int, str] = {}  # [key] concept_id -> [value] concept_code
    concept_name_map: Dict[int, str] = {}  # [key] concept_id -> [value] concept_name
    code_metadata: Dict[str, Any] = {}  # [key] concept_code -> [value] metadata

    # Read in the OMOP `CONCEPT` table from disk
    # (see https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html#concepts)
    # and use it to generate metadata file as well as populate maps
    # from (concept ID -> concept code) and (concept ID -> concept name)
    print("Generating metadata from OMOP `concept` table")
    for concept_file in tqdm(itertools.chain(*get_table_files(path_to_src_omop_dir, "concept")), 
                             total=len(get_table_files(path_to_src_omop_dir, "concept")[0]) + len(get_table_files(path_to_src_omop_dir, "concept")[1]), 
                             desc="Generating metadata from OMOP `concept` table"):
        # Note: Concept table is often split into gzipped shards by default
        if verbose:
            print(concept_file)
        with load_file(path_to_decompressed_dir, concept_file) as f:
            # Read the contents of the `concept` table shard
            # `load_file` will unzip the file into `path_to_decompressed_dir` if needed
            concept = read_polars_df(f.name)

            concept_id = pl.col("concept_id").cast(pl.Int64)
            code = pl.col("vocabulary_id") + "/" + pl.col("concept_code")

            # Convert the table into a dictionary
            result = concept.select(concept_id=concept_id, code=code, name=pl.col("concept_name"))

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
                    "code": custom_concepts["code"][i],
                    "description": custom_concepts["description"][i],
                    "parent_codes": [],
                }

    # Include map from custom concepts to normalized (ie standard ontology)
    # parent concepts, where possible, in the code_metadata dictionary
    for concept_relationship_file in tqdm(itertools.chain(*get_table_files(path_to_src_omop_dir, "concept_relationship")), 
                                                          total=len(get_table_files(path_to_src_omop_dir, "concept_relationship")[0]) + len(get_table_files(path_to_src_omop_dir, "concept_relationship")[1]), 
                                                          desc="Generating metadata from OMOP `concept_relationship` table"):
        with load_file(path_to_decompressed_dir, concept_relationship_file) as f:
            # This table has `concept_id_1`, `concept_id_2`, `relationship_id` columns
            concept_relationship = read_polars_df(f.name)

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
                custom_relationships["concept_id_1"], custom_relationships["concept_id_2"]
            ):
                if concept_id_1 in concept_id_map and concept_id_2 in concept_id_map:
                    code_metadata[concept_id_map[concept_id_1]]["parent_codes"].append(concept_id_map[concept_id_2])

    # Extract dataset metadata e.g., the CDM source name and its release date
    datasets: List[str] = []
    dataset_versions: List[str] = []
    for cdm_source_file in tqdm(itertools.chain(*get_table_files(path_to_src_omop_dir, "cdm_source")),
                                total=len(get_table_files(path_to_src_omop_dir, "cdm_source")[0]) + len(get_table_files(path_to_src_omop_dir, "cdm_source")[1]),
                                desc="Extracting dataset metadata"):
        with load_file(path_to_decompressed_dir, cdm_source_file) as f:
            cdm_source = read_polars_df(f.name)
            cdm_source = cdm_source.rename({c: c.lower() for c in cdm_source.columns})
            cdm_source = cdm_source.to_dict(as_series=False)

            datasets.extend(cdm_source["cdm_source_name"])
            dataset_versions.extend(cdm_source["cdm_release_date"])

    metadata = {
        "dataset_name": "|".join(datasets),  # eg 'Epic Clarity SHC|Epic Clarity LPCH'
        "dataset_version": "|".join(str(a) for a in dataset_versions),  # eg '2024-02-01|2024-02-01'
        "etl_name": "meds_etl.omop",
        "etl_version": meds_etl.__version__,
        "meds_version": meds.__version__,
    }
    # At this point metadata['code_metadata']['STANFORD_MEAS/AMOXICILLIN/CLAVULANATE']
    # should give a dictionary like
    # {'description': 'AMOXICILLIN/CLAVULANATE', 'parent_codes': ['LOINC/18862-3']}
    # where LOINC Code '18862-3' is a standard concept representing a lab test
    # measurement determining microorganism susceptibility to Amoxicillin+clavulanic acid

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata_schema)

    return metadata, code_metadata, concept_id_map, concept_name_map


def main():
    parser = argparse.ArgumentParser(prog="meds_etl_omop", description="Performs an ETL from OMOP v5 to MEDS")
    parser.add_argument(
        "path_to_src_omop_dir",
        type=str,
        help="Path to the OMOP source directory, e.g. "
        "`~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_2023_11_19` "
        " for STARR-OMOP full or "
        "`~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_1pcent_2024_02_09`",
    )
    parser.add_argument("path_to_dest_meds_dir", type=str, help="Path to where the output MEDS files will be stored")
    parser.add_argument(
        "--num_shards",
        type=int,
        default=100,
        help="Number of shards to use for converting MEDS from the unsorted format "
        "to MEDS (subjects are distributed approximately uniformly at "
        "random across shards and collation/joining of OMOP tables is "
        "performed on a shard-by-shard basis).",
    )
    parser.add_argument("--num_proc", type=int, default=1, help="Number of vCPUs to use for performing the MEDS ETL")
    parser.add_argument(
        "--backend",
        type=str,
        default="polars",
        help="The backend to use when converting from MEDS Unsorted to MEDS in the ETL. See the README for a discussion on possible backends.",
    )
    parser.add_argument("--verbose", type=int, default=0)
    parser.add_argument(
        "--continue_job",
        dest="continue_job",
        action="store_true",
        help="If set, the job continues from a previous run, starting after the "
        "conversion to MEDS Unsorted but before converting from MEDS Unsorted to MEDS.",
    )

    args = parser.parse_args()

    if not os.path.exists(args.path_to_src_omop_dir):
        raise ValueError(f'The source OMOP folder ("{args.path_to_src_omop_dir}") does not seem to exist?')

    # Create the directory where final MEDS files will go, if doesn't already exist
    os.makedirs(args.path_to_dest_meds_dir, exist_ok=True)

    # Within the target directory, create temporary subfolder for holding files
    # that need to be decompressed as part of the ETL (via eg `load_file`)
    path_to_decompressed_dir = os.path.join(args.path_to_dest_meds_dir, "decompressed")

    # Create temporary folder for storing MEDS Unsorted data within target directory
    path_to_temp_dir = os.path.join(args.path_to_dest_meds_dir, "temp")
    path_to_MEDS_unsorted_dir = os.path.join(path_to_temp_dir, "unsorted_data")

    if not args.continue_job or not (os.path.exists(path_to_MEDS_unsorted_dir) and os.path.exists(path_to_temp_dir)):
        if os.path.exists(path_to_temp_dir):
            if args.verbose:
                print(f"Deleting and recreating {path_to_temp_dir}")
            shutil.rmtree(path_to_temp_dir)
        os.mkdir(path_to_temp_dir)

        if os.path.exists(path_to_MEDS_unsorted_dir):
            if args.verbose:
                print(f"Deleting and recreating {path_to_MEDS_unsorted_dir}")
            shutil.rmtree(path_to_MEDS_unsorted_dir)
        os.mkdir(path_to_MEDS_unsorted_dir)

        if os.path.exists(path_to_decompressed_dir):
            if args.verbose:
                print(f"Deleting and recreating {path_to_decompressed_dir}")
            shutil.rmtree(path_to_decompressed_dir)
        os.mkdir(path_to_decompressed_dir)

        # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Generate metadata.json from OMOP concept table  #
        # # # # # # # # # # # # # # # # # # # # # # # # # #

        metadata, code_metadata, concept_id_map, concept_name_map = extract_metadata(
            path_to_src_omop_dir=args.path_to_src_omop_dir,
            path_to_decompressed_dir=path_to_decompressed_dir,
            verbose=args.verbose,
        )

        os.mkdir(os.path.join(path_to_temp_dir, "metadata"))

        # Save the extracted metadata file to disk...
        # We save one copy in the MEDS Unsorted data directory
        with open(os.path.join(path_to_temp_dir, "metadata", "dataset.json"), "w") as f:
            json.dump(metadata, f)

        table = pa.Table.from_pylist(code_metadata.values(), meds.code_metadata_schema())
        pq.write_table(table, os.path.join(path_to_temp_dir, "metadata", "codes.parquet"))
        # And we save another copy in the final/target MEDS directory
        shutil.copytree(
            os.path.join(path_to_temp_dir, "metadata"), os.path.join(args.path_to_dest_meds_dir, "metadata")
        )

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Convert all "measurements" to MEDS Unsorted, write to disk  #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        tables: Dict[str, Any] = {
            # Each key is a `table_name`
            "person": [  # `table_name`s map to `table_details`
                {
                    "force_concept_code": meds.birth_code,
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
            "visit": {"fallback_concept_id": DEFAULT_VISIT_CONCEPT_ID, "file_suffix": "occurrence"},
            "condition": {
                "file_suffix": "occurrence",
            },
            "death": {
                "force_concept_code": meds.death_code,
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
        # Each subprocess will read in a decompressed file and put all measurements for a given subject
        # into that subject's corresponding shard. This makes creating subject timelines downstream
        # (where timelines incorporate measurements from across different tables) much less RAM intensive.
        all_csv_tasks = []
        all_parquet_tasks = []
        for table_name, table_details in tables.items():
            csv_table_files, parquet_table_files = get_table_files(
                path_to_src_omop_dir=args.path_to_src_omop_dir,
                table_name=table_name,
                table_details=table_details[0] if isinstance(table_details, list) else table_details,
            )

            all_csv_tasks.extend(
                (
                    table_file,
                    table_name,
                    table_details,
                    concept_id_map_data,
                    concept_name_map_data,
                    path_to_MEDS_unsorted_dir,
                    path_to_decompressed_dir,
                    args.verbose,
                )
                for table_file in csv_table_files
            )

            all_parquet_tasks.extend(
                (
                    table_files,
                    table_name,
                    table_details,
                    concept_id_map_data,
                    concept_name_map_data,
                    path_to_MEDS_unsorted_dir,
                    args.verbose,
                )
                for table_files in split_list(parquet_table_files, args.num_shards)
            )

        rng = random.Random(RANDOM_SEED)
        rng.shuffle(all_csv_tasks)
        rng.shuffle(all_parquet_tasks)

        print("Decompressing OMOP tables, mapping to MEDS Unsorted format, writing to disk...")
        if args.num_proc > 1:
            os.environ["POLARS_MAX_THREADS"] = "1"
            with multiprocessing.get_context("spawn").Pool(args.num_proc) as pool:
                # Wrap all tasks with tqdm for a progress bar
                total_tasks = len(all_csv_tasks)
                with tqdm(total=total_tasks, desc="Mapping CSV OMOP tables -> MEDS format") as pbar:
                    for _ in pool.imap_unordered(process_table_csv, all_csv_tasks):
                        pbar.update()

                total_tasks = len(all_parquet_tasks)
                with tqdm(total=total_tasks, desc="Mapping Parquet OMOP tables -> MEDS format") as pbar:
                    for _ in pool.imap_unordered(process_table_parquet, all_parquet_tasks):
                        pbar.update()

        else:
            # Useful for debugging `process_table` without multiprocessing
            for task in tqdm(all_csv_tasks):
                process_table_csv(task)

            for task in tqdm(all_parquet_tasks):
                process_table_parquet(task)

        # TODO: Do we need to do this more often so as to reduce maximum disk storage?
        shutil.rmtree(path_to_decompressed_dir)

        print("Finished converting dataset to MEDS Unsorted.")

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Collate measurements into timelines for each subject, by shard
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    print("Converting from MEDS Unsorted to MEDS...")
    meds_etl.unsorted.sort(
        source_unsorted_path=path_to_temp_dir,
        target_meds_path=os.path.join(args.path_to_dest_meds_dir, "result"),
        num_shards=args.num_shards,
        num_proc=args.num_proc,
        backend=args.backend,
    )
    print("...finished converting MEDS Unsorted to MEDS")
    shutil.move(
        src=os.path.join(args.path_to_dest_meds_dir, "result", "data"),
        dst=os.path.join(args.path_to_dest_meds_dir, "data"),
    )
    shutil.rmtree(path=os.path.join(args.path_to_dest_meds_dir, "result"))

    print(f"Deleting temporary directory `{path_to_temp_dir}`")
    shutil.rmtree(path_to_temp_dir)


if __name__ == "__main__":
    main()

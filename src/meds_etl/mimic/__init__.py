import argparse
import json
import os
import shutil
import subprocess
from importlib.resources import files

import jsonschema
import meds
import polars as pl
import pyarrow.parquet as pq

import meds_etl

MIMIC_VERSION = "2.2"


def add_dot(code, position):
    return (
        pl.when(code.str.len_chars() > position)
        .then(code.str.slice(0, position) + "." + code.str.slice(position))
        .otherwise(code)
    )


def normalize_icd_diagnosis(icd_version, icd_code):
    icd9_code = pl.when(icd_code.str.starts_with("E")).then(add_dot(icd_code, 4)).otherwise(add_dot(icd_code, 3))

    icd10_code = add_dot(icd_code, 3)

    return pl.when(icd_version == "9").then("ICD9CM/" + icd9_code).otherwise("ICD10CM/" + icd10_code)


def normalize_icd_procedure(icd_version, icd_code):
    icd9_code = add_dot(icd_code, 2)
    icd10_code = icd_code

    return pl.when(icd_version == "9").then("ICD9Proc/" + icd9_code).otherwise("ICD10PCS/" + icd10_code)


def main():
    parser = argparse.ArgumentParser(prog="meds_etl_mimic", description="Performs an ETL from MIMIC_IV to MEDS")
    parser.add_argument("src_mimic", type=str)
    parser.add_argument("destination", type=str)
    parser.add_argument("--num_shards", type=int, default=100)
    args = parser.parse_args()

    if not os.path.exists(args.src_mimic):
        raise ValueError(f'The source MIMIC_IV folder ("{args.src_mimic}") does not seem to exist?')

    src_mimic_version = os.path.join(args.src_mimic, MIMIC_VERSION)

    if not os.path.exists(src_mimic_version):
        raise ValueError(f'The source MIMIC_IV folder does not contain a version 2.2 subfolder ("{src_mimic_version}")')

    os.makedirs(args.destination)

    all_tables = {
        # Explicitly ignore icustays as it is redunant with hosp/transfers
        "icu/icustays": None,
        # Ignore these tables as they don't generate events
        "icu/caregiver": None,
        "icu/d_items": None,
        "icu/datetimeevents": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid"),
            "time": pl.col("storetime"),
            "value": pl.col("value"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("caregiver_id"),
            },
        },
        "icu/inputevents": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid") + "/" + pl.col("ordercategorydescription"),
            "time": pl.col("storetime"),
            "value": pl.col("rate"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("caregiver_id"),
                "start": pl.col("starttime"),
                "end": pl.col("endtime"),
                "unit": pl.col("rateuom"),
            },
        },
        "icu/chartevents": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid"),
            "time": pl.coalesce(pl.col("storetime"), pl.col("charttime")),
            "value": pl.col("value"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("caregiver_id"),
                "unit": pl.col("valueuom"),
            },
        },
        "icu/procedureevents": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid"),
            "time": pl.col("storetime"),
            "value": pl.col("value"),
            "metadata": {
                "location": pl.col("location"),
                "location_category": pl.col("locationcategory"),
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("caregiver_id"),
                "unit": pl.col("valueuom"),
                "start": pl.col("starttime"),
                "end": pl.col("endtime"),
            },
        },
        "icu/ingredientevents": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid"),
            "time": pl.col("storetime"),
            "value": pl.col("rate"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("caregiver_id"),
                "unit": pl.col("rateuom"),
                "start": pl.col("starttime"),
                "end": pl.col("endtime"),
            },
        },
        "icu/outputevents": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid"),
            "time": pl.col("storetime"),
            "value": pl.col("value"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("caregiver_id"),
                "unit": pl.col("valueuom"),
            },
        },
        "hosp/admissions": [
            {
                "code": "MIMIC_IV_Admission/" + pl.col("admission_type"),
                "time": pl.col("admittime"),
                "metadata": {
                    "visit_id": pl.col("hadm_id"),
                    "caregiver_id": pl.col("admit_provider_id"),
                    "end": pl.col("dischtime"),
                },
            },
            {
                "code": "MIMIC_IV_Admission_Location/" + pl.col("admission_location"),
                "time": pl.col("admittime"),
                "metadata": {
                    "visit_id": pl.col("hadm_id"),
                },
            },
            {
                "code": "MIMIC_IV_Discharge_Location/" + pl.col("discharge_location"),
                "time": pl.col("dischtime"),
                "metadata": {
                    "visit_id": pl.col("hadm_id"),
                },
                "possibly_null_code": True,
            },
            {
                "code": "MIMIC_IV_Insurance/" + pl.col("insurance"),
                "time": pl.col("admittime"),
                "metadata": {
                    "visit_id": pl.col("hadm_id"),
                },
            },
            {
                "code": "MIMIC_IV_Language/" + pl.col("language"),
                "time": pl.col("admittime"),
                "metadata": {
                    "visit_id": pl.col("hadm_id"),
                },
            },
            {
                "code": "MIMIC_IV_Marital_Status/" + pl.col("marital_status"),
                "time": pl.col("admittime"),
                "metadata": {
                    "visit_id": pl.col("hadm_id"),
                },
                "possibly_null_code": True,
            },
            {
                "code": "MIMIC_IV_Race/" + pl.col("race"),
                "time": pl.col("admittime"),
                "metadata": {
                    "visit_id": pl.col("hadm_id"),
                },
            },
        ],
        # No events are generated from these tables
        "hosp/d_hcpcs": None,
        "hosp/d_icd_diagnoses": None,
        "hosp/d_icd_procedures": None,
        "hosp/d_labitems": None,
        "hosp/diagnoses_icd": {
            "code": normalize_icd_diagnosis(pl.col("icd_version"), pl.col("icd_code")),
            "time": pl.col("dischtime"),
            "requires_admission_join": True,
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "seq_num": pl.col("seq_num"),
            },
        },
        "hosp/drgcodes": {
            "code": pl.col("drg_type") + "/" + pl.col("drg_code"),
            "time": pl.col("dischtime"),
            "requires_admission_join": True,
            "metadata": {
                "visit_id": pl.col("hadm_id"),
            },
        },
        # It's unclear how to best process these tables.
        # TODO: Fix these
        "hosp/emar": None,
        "hosp/emar_detail": None,
        "hosp/hcpcsevents": {
            "code": "HCPCS/" + pl.col("hcpcs_cd"),
            "time": pl.col("chartdate"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "seq_num": pl.col("seq_num"),
            },
        },
        "hosp/labevents": {
            "code": "MIMIC_IV_LABITEM/" + pl.col("itemid"),
            "time": pl.coalesce(pl.col("storetime"), pl.col("charttime")),
            "value": pl.col("value"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("order_provider_id"),
                "unit": pl.col("valueuom"),
                "priority": pl.col("priority"),
                "comments": pl.col("comments"),
            },
        },
        # This transformation is ignoring a ton of data within the table.
        # TODO: Improve this
        "hosp/microbiologyevents": {
            "code": "MIMIC_IV_MicrobiologyTest/" + pl.col("test_name"),
            "time": pl.coalesce(pl.col("storetime"), pl.col("storedate"), pl.col("charttime"), pl.col("chartdate")),
            "value": pl.col("org_name"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "caregiver_id": pl.col("order_provider_id"),
                "specimen_name": "MIMIC_IV_MicrobiologySpecimin/" + pl.col("spec_type_desc"),
                "ab_name": "MIMIC_IV_MicrobiologyAntiBiotic/" + pl.col("ab_name"),
                "dilution_text": pl.col("dilution_text"),
                "interpretation": pl.col("interpretation"),
                "comments": pl.col("comments"),
            },
        },
        "hosp/omr": {
            "code": "MIMIC_IV_OMR/" + pl.col("result_name"),
            "time": pl.col("chartdate"),
            "value": pl.col("result_value"),
        },
        "hosp/patients": [
            {
                # Birth
                "code": pl.lit(meds.birth_code),
                "time": pl.datetime(pl.col("anchor_year").cast(int) - pl.col("anchor_age").cast(int), 1, 1).cast(str),
            },
            {
                "code": "MIMIC_IV_Gender/" + pl.col("gender"),
                "time": pl.datetime(pl.col("anchor_year").cast(int) - pl.col("anchor_age").cast(int), 1, 1).cast(str),
            },
            {
                # Death
                "code": pl.lit(meds.death_code),
                "time": pl.col("dod"),
                "possibly_null_time": True,
            },
        ],
        # It's unclear how to process this table.
        # TODO: Fix this
        "hosp/pharmacy": None,
        # The POE table contains the same information as other tables, so not needed
        "hosp/poe": None,
        "hosp/poe_detail": None,
        "hosp/prescriptions": {
            "code": pl.coalesce(
                "NDC/" + pl.when(pl.col("ndc") != "0").then(pl.col("ndc")), "MIMIC_IV_Drug/" + pl.col("drug")
            ),
            "time": pl.col("starttime"),
            "value": pl.col("dose_val_rx"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "route": pl.col("route"),
                "unit": pl.col("dose_unit_rx"),
            },
            "possibly_null_time": True,
        },
        "hosp/procedures_icd": {
            "code": normalize_icd_procedure(pl.col("icd_version"), pl.col("icd_code")),
            "time": pl.col("chartdate"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "seq_num": pl.col("seq_num"),
            },
        },
        # Doesn't generate events
        "hosp/provider": None,
        "hosp/services": {
            "code": "MIMIC_IV_Service/" + pl.col("curr_service"),
            "time": pl.col("transfertime"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
            },
        },
        "hosp/transfers": {
            "code": "MIMIC_IV_Transfer/" + pl.col("careunit"),
            "time": pl.col("intime"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
            },
            "possibly_null_code": True,
        },
    }

    events = []

    admission_table = pl.read_csv(
        os.path.join(src_mimic_version, "hosp", "admissions" + ".csv.gz"), infer_schema_length=0
    ).lazy()

    decompressed_dir = os.path.join(args.destination, "decompressed")
    os.mkdir(decompressed_dir)

    temp_dir = os.path.join(args.destination, "temp")
    os.mkdir(temp_dir)

    for shard_index in range(args.num_shards):
        os.mkdir(os.path.join(temp_dir, str(shard_index)))

    print("Processing tables into " + temp_dir)

    for table_name, mapping_codes in all_tables.items():
        print("Processing", table_name)
        if mapping_codes is None:
            continue

        uncompressed_path = os.path.join(decompressed_dir, table_name.replace("/", "_") + ".csv")
        compressed_path = os.path.join(src_mimic_version, table_name + ".csv.gz")

        with open(uncompressed_path, "w") as f:
            subprocess.run(["gunzip", "-c", compressed_path], stdout=f)

        if not isinstance(mapping_codes, list):
            mapping_codes = [mapping_codes]

        for map_index, mapping_code in enumerate(mapping_codes):
            reader = pl.read_csv_batched(
                uncompressed_path,
                infer_schema_length=0,
                batch_size=10_000_000,
            )

            patient_id = pl.col("subject_id").cast(pl.Int64)
            time = mapping_code["time"]
            time = pl.coalesce(
                time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
                time.str.to_datetime("%Y-%m-%d", strict=False).dt.offset_by("1d").dt.offset_by("-1s"),
            )
            code = mapping_code["code"]
            if "value" in mapping_code:
                value = mapping_code["value"]

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

            else:
                datetime_value = pl.lit(None, dtype=pl.Datetime)
                numeric_value = pl.lit(None, dtype=pl.Float32)
                text_value = pl.lit(None, dtype=str)

            if "metadata" not in mapping_code:
                mapping_code["metadata"] = {}

            mapping_code["metadata"]["table"] = pl.lit(table_name)

            def transform_metadata(d):
                return pl.struct([v.alias(k) for k, v in d.items()])

            metadata = transform_metadata(mapping_code["metadata"])

            batch_index = 0
            while True:
                batch_index += 1
                table_csv = reader.next_batches(1)
                if table_csv is None:
                    break

                table_csv = table_csv[0].lazy()

                if mapping_code.get("requires_admission_join", False):
                    table_csv = table_csv.join(admission_table, on="hadm_id")

                if mapping_code.get("possibly_null_code", False):
                    table_csv = table_csv.filter(code.is_not_null())

                if mapping_code.get("possibly_null_time", False):
                    table_csv = table_csv.filter(time.is_not_null())

                event_data = (
                    table_csv.select(
                        patient_id=patient_id,
                        time=time,
                        code=code,
                        text_value=text_value,
                        numeric_value=numeric_value,
                        datetime_value=datetime_value,
                        metadata=metadata,
                        shard=patient_id.hash(213345) % args.num_shards,
                    )
                    .collect()
                    .partition_by("shard", as_dict=True, maintain_order=False)
                )

                for shard_index, shard in event_data.items():
                    fname = os.path.join(
                        temp_dir, str(shard_index), f'{table_name.replace("/", "_")}_{map_index}_{batch_index}.parquet'
                    )
                    shard.write_parquet(fname, compression="uncompressed")

        os.remove(uncompressed_path)

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

    code_metadata = {}

    code_metadata_files = {
        "d_labitems_to_loinc": {
            "code": "MIMIC_IV_LABITEM/" + pl.col("itemid (omop_source_code)"),
            "descr": pl.col("label"),
            "parent": pl.col("omop_vocabulary_id") + "/" + pl.col("omop_concept_code"),
        },
        "inputevents_to_rxnorm": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid (omop_source_code)") + "/" + pl.col("ordercategorydescription"),
            "descr": pl.col("label"),
            "parent": pl.col("omop_vocabulary_id") + "/" + pl.col("omop_concept_code"),
        },
        "meas_chartevents_main": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid (omop_source_code)"),
            "descr": pl.col("label"),
            "parent": pl.col("omop_vocabulary_id") + "/" + pl.col("omop_concept_code"),
        },
        "outputevents_to_loinc": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid (omop_source_code)"),
            "descr": pl.col("label"),
            "parent": pl.col("omop_vocabulary_id") + "/" + pl.col("omop_concept_code"),
        },
        "proc_datetimeevents": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid (omop_source_code)"),
            "descr": pl.col("label"),
            "parent": pl.col("omop_vocabulary_id") + "/" + pl.col("omop_concept_code"),
        },
        "proc_itemid": {
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid (omop_source_code)"),
            "descr": pl.col("label"),
            "parent": pl.col("omop_vocabulary_id") + "/" + pl.col("omop_concept_code"),
        },
    }

    for fname, mapping_info in code_metadata_files.items():
        with files("meds_etl.mimic.concept_map").joinpath(fname + ".csv").open("rb") as f:
            table = pl.read_csv(f, infer_schema_length=0)
            code_and_value = table.select(
                code=mapping_info["code"], descr=mapping_info["descr"], parent=mapping_info["parent"]
            )
            for code, descr, value in code_and_value.iter_rows():
                if code in code_metadata:
                    assert (
                        value == code_metadata[code].get("parent_codes", [None])[0]
                    ), f"{code} {descr} {value} {code_metadata[code]}"
                result = {}

                if descr is not None:
                    result["description"] = descr

                if value is not None:
                    result["parent_codes"] = [value]

                code_metadata[code] = result

    metadata = {
        "dataset_name": "MIMIC-IV",
        "dataset_version": MIMIC_VERSION,
        "etl_name": "meds_etl.mimic",
        "etl_version": meds_etl.__version__,
        "code_metadata": code_metadata,
    }

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata)

    with open(os.path.join(args.destination, "metadata.json"), "w") as f:
        json.dump(metadata, f)

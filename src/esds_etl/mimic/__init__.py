import argparse
import os

import esds
import polars as pl
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(prog="esds_etl_mimic", description="Performs an ETL from MIMIC_IV to ESDS")
    parser.add_argument("src_mimic", type=str)
    parser.add_argument("destination", type=str)
    args = parser.parse_args()

    os.makedirs(args.destination, exist_ok=True)

    if not os.path.exists(args.src_mimic):
        raise ValueError(f'The source MIMIC_IV folder ("{args.src_mimic}") does not seem to exist?')

    src_mimic_version = os.path.join(args.src_mimic, "2.2")

    if not os.path.exists(src_mimic_version):
        raise ValueError(f'The source MIMIC_IV folder does not contain a version 2.2 subfolder ("{src_mimic_version}")')

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
            "code": "MIMIC_IV_ITEM/" + pl.col("itemid"),
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
            "code": "ICD" + pl.col("icd_version") + "CM/" + pl.col("icd_code"),
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
                "code": pl.lit("SNOMED/184099003"),
                "time": pl.datetime(pl.col("anchor_year").cast(int) - pl.col("anchor_age").cast(int), 1, 1).cast(str),
            },
            {
                "code": "MIMIC_IV_Gender/" + pl.col("gender"),
                "time": pl.datetime(pl.col("anchor_year").cast(int) - pl.col("anchor_age").cast(int), 1, 1).cast(str),
            },
            {
                # Death
                "code": pl.lit("SNOMED/419620001"),
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
            "code": pl.coalesce("NDC/" + pl.col("ndc"), "MIMIC_IV_Drug/" + pl.col("drug")),
            "time": pl.col("starttime"),
            "value": pl.col("dose_val_rx"),
            "metadata": {
                "visit_id": pl.col("hadm_id"),
                "route": pl.col("route"),
                "unit": pl.col("dose_unit_rx"),
            },
        },
        "hosp/procedures_icd": {
            "code": "ICD" + pl.col("icd_version") + "CM/" + pl.col("icd_code"),
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

    for table_name, mapping_codes in all_tables.items():
        if mapping_codes is None:
            continue

        if not isinstance(mapping_codes, list):
            mapping_codes = [mapping_codes]

        for mapping_code in mapping_codes:
            table_csv = pl.read_csv(
                os.path.join(src_mimic_version, table_name + ".csv.gz"), infer_schema_length=0
            ).lazy()

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
                text_value = datetime_value.is_null() & numeric_value.is_null() & (value != pl.lit(""))
            else:
                datetime_value = pl.lit(None)
                numeric_value = pl.lit(None)
                text_value = pl.lit(None)

            if "metadata" not in mapping_code:
                mapping_code["metadata"] = {}

            mapping_code["metadata"]["table"] = pl.lit(table_name)

            def transform_metadata(d):
                return pl.struct([v.alias(k) for k, v in d.items()])

            metadata = transform_metadata(mapping_code["metadata"])

            if mapping_code.get("requires_admission_join", False):
                table_csv = table_csv.join(admission_table, on="hadm_id")

            if mapping_code.get("possibly_null_code", False):
                table_csv = table_csv.filter(code != pl.lit(None))

            if mapping_code.get("possibly_null_time", False):
                table_csv = table_csv.filter(time != pl.lit(None))

            events.append(
                table_csv.select(
                    patient_id=patient_id,
                    time=time,
                    code=code,
                    text_value=text_value,
                    numeric_value=numeric_value,
                    datetime_value=datetime_value,
                    metadata=metadata,
                )
            )

    all_events = pl.concat(events, how="diagonal_relaxed").collect().lazy()

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

    print("All events", all_events.schema)

    grouped_by_time = all_events.groupby("patient_id", "time").agg(measurements=measurement)

    print("Grouped", grouped_by_time.schema)

    event = pl.struct(
        pl.col("time"),
        pl.col("measurements"),
    )

    grouped_by_patient = grouped_by_time.groupby("patient_id").agg(events=event.sort_by(pl.col("time")))

    grouped_by_patient.collect().write_parquet("result.parquet", compression=None)

    load_back = pq.read_table("result.parquet")
    event_schema = load_back.schema.field("events").type.value_type
    measurement_schema = event_schema.field("measurements").type.value_type
    metadata_schema = measurement_schema.field("metadata").type
    desired_schema = esds.patient_schema(metadata_schema)
    casted = load_back.cast(desired_schema)
    print(load_back.schema)
    print(desired_schema)
    pq.write_table(casted, "final_results.parquet")

    d_items = pl.read_csv(os.path.join(src_mimic_version, "icu", "d_items.csv.gz"))

    print(d_items)

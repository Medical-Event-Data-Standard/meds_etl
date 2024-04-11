from __future__ import annotations

import datetime
import json
import os
import pathlib
import random
from typing import List

import jsonschema
import meds
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import meds_etl.flat

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    import meds_etl_cpp
except ImportError:
    meds_etl_cpp = None


def get_random_patient(patient_id: int, include_metadata=True) -> meds.Patient:
    random.seed(patient_id)

    epoch = datetime.datetime(1990, 1, 1)
    birth = epoch + datetime.timedelta(days=random.randint(100, 1000))
    current_date = birth

    gender = "Gender/" + random.choice(["F", "M"])
    race = "Race/" + random.choice(["White", "Non-White"])

    patient: meds.Patient
    patient = {
        "patient_id": patient_id,
        "events": [
            {
                "time": birth,
                "measurements": [
                    {"code": meds.birth_code, "metadata": {}},
                    {"code": gender, "metadata": {}},
                    {"code": race, "metadata": {}},
                ],
            },
        ],
    }
    code_cats = ["ICD9CM", "RxNorm"]
    for _ in range(random.randint(1, 3 + (3 if gender == "Gender/F" else 0))):
        code_cat = random.choice(code_cats)
        if code_cat == "RxNorm":
            code = str(random.randint(0, 10000))
        else:
            code = str(random.randint(0, 10000))
            if len(code) > 3:
                code = code[:3] + "." + code[3:]
        current_date = current_date + datetime.timedelta(days=random.randint(1, 100))
        code = code_cat + "/" + code
        patient["events"].append(
            {"time": current_date, "measurements": [{"code": code, "metadata": {"ontology": code_cat}}]}
        )

    if not include_metadata:
        for e in patient["events"]:
            for m in e["measurements"]:
                if "metadata" in m:
                    del m["metadata"]

    return patient


def create_example_patients(include_metadata=True):
    patients = []
    for i in range(200):
        patients.append(get_random_patient(i, include_metadata=include_metadata))
    return patients


def create_dataset(tmp_path: pathlib.Path, include_metadata=True):
    os.makedirs(tmp_path / "data", exist_ok=True)

    if not include_metadata:
        metadata_schema = pa.null()
    else:
        metadata_schema = pa.struct(
            [
                ("ontology", pa.string()),
                ("dummy", pa.string()),
            ]
        )

    patients = create_example_patients(include_metadata=include_metadata)
    patient_schema = meds.patient_schema(metadata_schema)

    patient_table = pa.Table.from_pylist(patients, patient_schema)

    pq.write_table(patient_table, tmp_path / "data" / "patients.parquet")

    metadata = {
        "dataset_name": "synthetic datata",
        "dataset_version": "1",
        "etl_name": "synthetic data",
        "etl_version": "1",
        "code_metadata": {},
    }

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata)

    with open(tmp_path / "metadata.json", "w") as f:
        json.dump(metadata, f)

    return patients, patient_schema


def roundtrip_helper(tmp_path: pathlib.Path, patients: List[meds.Patient], format: meds_etl.flat.Format, num_proc: int):
    for backend in ["duckdb", "polars", "cpp"]:
        if backend == "duckdb" and duckdb is None:
            continue
        if backend == "cpp" and meds_etl_cpp is None or format != "parquet":
            continue
        print("Testing", format, backend)
        meds_dataset = tmp_path / "meds"
        meds_flat_dataset = tmp_path / f"meds_flat_{format}_{num_proc}_{backend}"
        meds_dataset2 = tmp_path / f"meds2_{format}_{num_proc}_{backend}"

        meds_etl.flat.convert_meds_to_flat(str(meds_dataset), str(meds_flat_dataset), num_proc=num_proc, format=format)

        meds_etl.flat.convert_flat_to_meds(
            str(meds_flat_dataset), str(meds_dataset2), num_proc=num_proc, num_shards=10, backend=backend
        )

        patient_table = pa.concat_tables(
            [pq.read_table(meds_dataset2 / "data" / i) for i in os.listdir(meds_dataset2 / "data")]
        )

        final_patients = patient_table.to_pylist()
        final_patients.sort(key=lambda a: a["patient_id"])

        for patient in final_patients:
            for event in patient["events"]:
                event["measurements"].sort(key=lambda m: m["code"])

        for patient in patients:
            for event in patient["events"]:
                event["measurements"].sort(key=lambda m: m["code"])

        print(patients[1])

        print(final_patients[1])

        for i in range(len(patients)):
            assert patients[i] == final_patients[i]

        assert patients == final_patients


def test_roundtrip_with_metadata(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset)
    patients = pq.read_table(meds_dataset / "data" / "patients.parquet").to_pylist()
    patients.sort(key=lambda a: a["patient_id"])

    roundtrip_helper(tmp_path, patients, "csv", 1)
    roundtrip_helper(tmp_path, patients, "compressed_csv", 1)
    roundtrip_helper(tmp_path, patients, "parquet", 1)

    roundtrip_helper(tmp_path, patients, "csv", 4)
    roundtrip_helper(tmp_path, patients, "compressed_csv", 4)
    roundtrip_helper(tmp_path, patients, "parquet", 4)


def test_roundtrip_no_metadata(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset, include_metadata=False)
    patients = pq.read_table(meds_dataset / "data" / "patients.parquet").to_pylist()
    patients.sort(key=lambda a: a["patient_id"])

    roundtrip_helper(tmp_path, patients, "csv", 1)
    roundtrip_helper(tmp_path, patients, "compressed_csv", 1)
    roundtrip_helper(tmp_path, patients, "parquet", 1)

    roundtrip_helper(tmp_path, patients, "csv", 4)
    roundtrip_helper(tmp_path, patients, "compressed_csv", 4)
    roundtrip_helper(tmp_path, patients, "parquet", 4)


def test_shuffle_polars(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset)

    patients = pq.read_table(meds_dataset / "data" / "patients.parquet").to_pylist()
    patients.sort(key=lambda a: a["patient_id"])
    for patient in patients:
        for event in patient["events"]:
            event["measurements"].sort(key=lambda m: m["code"])

    meds_flat_dataset = tmp_path / "meds_flat"
    meds_etl.flat.convert_meds_to_flat(str(meds_dataset), str(meds_flat_dataset), format="csv")

    with open(tmp_path / "meds_flat" / "flat_data" / "patients.csv") as f:
        lines = f.readlines()
        header = lines[0]
        rows = lines[1:]

    random.shuffle(rows)

    os.unlink(tmp_path / "meds_flat" / "flat_data" / "patients.csv")

    num_parts = 3
    rows_per_part = (len(rows) + num_parts - 1) // num_parts

    for a in range(num_parts):
        r = rows[a * rows_per_part : (a + 1) * rows_per_part]
        with open(tmp_path / "meds_flat" / "flat_data" / (str(a) + ".csv"), "w") as f:
            f.write(header)
            f.write("".join(r))

    meds_dataset2 = tmp_path / "meds2"

    meds_etl.flat.convert_flat_to_meds(str(meds_flat_dataset), str(meds_dataset2), num_shards=10, backend="polars")

    patient_table = pa.concat_tables(
        [pq.read_table(meds_dataset2 / "data" / i) for i in os.listdir(meds_dataset2 / "data")]
    )

    final_patients = patient_table.to_pylist()
    final_patients.sort(key=lambda a: a["patient_id"])
    for patient in final_patients:
        for event in patient["events"]:
            event["measurements"].sort(key=lambda m: m["code"])

    assert patients == final_patients


@pytest.mark.skipif(duckdb is None, reason="duckdb not installed")
def test_shuffle_duckdb(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset)

    patients = pq.read_table(meds_dataset / "data" / "patients.parquet").to_pylist()
    patients.sort(key=lambda a: a["patient_id"])
    for patient in patients:
        for event in patient["events"]:
            event["measurements"].sort(key=lambda m: m["code"])

    meds_flat_dataset = tmp_path / "meds_flat"
    meds_etl.flat.convert_meds_to_flat(str(meds_dataset), str(meds_flat_dataset), format="csv")

    with open(tmp_path / "meds_flat" / "flat_data" / "patients.csv") as f:
        lines = f.readlines()
        header = lines[0]
        rows = lines[1:]

    random.shuffle(rows)

    os.unlink(tmp_path / "meds_flat" / "flat_data" / "patients.csv")

    num_parts = 3
    rows_per_part = (len(rows) + num_parts - 1) // num_parts

    for a in range(num_parts):
        r = rows[a * rows_per_part : (a + 1) * rows_per_part]
        with open(tmp_path / "meds_flat" / "flat_data" / (str(a) + ".csv"), "w") as f:
            f.write(header)
            f.write("".join(r))

    meds_dataset2 = tmp_path / "meds2"

    meds_etl.flat.convert_flat_to_meds(
        str(meds_flat_dataset), str(meds_dataset2), backend="duckdb", num_shards=4, num_proc=2
    )

    patient_table = pa.concat_tables(
        [pq.read_table(meds_dataset2 / "data" / i) for i in os.listdir(meds_dataset2 / "data")]
    )

    final_patients = patient_table.to_pylist()
    final_patients.sort(key=lambda a: a["patient_id"])
    for patient in final_patients:
        for event in patient["events"]:
            event["measurements"].sort(key=lambda m: m["code"])

    assert patients == final_patients

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

import meds_etl.flat


def get_random_patient(patient_id: int) -> meds.Patient:
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

    return patient


def create_example_patients():
    patients = []
    for i in range(200):
        patients.append(get_random_patient(i))
    return patients


def create_dataset(tmp_path: pathlib.Path):
    os.makedirs(tmp_path / "data", exist_ok=True)

    metadata_schema = pa.struct(
        [
            ("ontology", pa.string()),
            ("dummy", pa.string()),
        ]
    )

    patients = create_example_patients()
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


def roundrip_helper(tmp_path: pathlib.Path, patients: List[meds.Patient], format: meds_etl.flat.Format, num_proc: int):
    print("Testing", format)
    meds_dataset = tmp_path / "meds"
    meds_flat_dataset = tmp_path / ("meds_flat_" + format + "_" + str(num_proc))
    meds_dataset2 = tmp_path / ("meds2_" + format + "_" + str(num_proc))

    meds_etl.flat.convert_meds_to_flat(str(meds_dataset), str(meds_flat_dataset), num_proc=num_proc, format=format)

    meds_etl.flat.convert_flat_to_meds(str(meds_flat_dataset), str(meds_dataset2), num_proc=num_proc, num_shards=10)

    patient_table = pa.concat_tables(
        [pq.read_table(meds_dataset2 / "data" / i) for i in os.listdir(meds_dataset2 / "data")]
    )

    final_patients = patient_table.to_pylist()
    final_patients.sort(key=lambda a: a["patient_id"])

    assert patients == final_patients


def test_roundtrip(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset)
    patients = pq.read_table(meds_dataset / "data" / "patients.parquet").to_pylist()
    patients.sort(key=lambda a: a["patient_id"])

    roundrip_helper(tmp_path, patients, "csv", 1)
    roundrip_helper(tmp_path, patients, "compressed_csv", 1)
    roundrip_helper(tmp_path, patients, "parquet", 1)

    roundrip_helper(tmp_path, patients, "csv", 4)
    roundrip_helper(tmp_path, patients, "compressed_csv", 4)
    roundrip_helper(tmp_path, patients, "parquet", 4)


def test_shuffle(tmp_path: pathlib.Path):
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
            f.write(header + "\n")
            f.write("\n".join(r))

    meds_dataset2 = tmp_path / "meds2"

    meds_etl.flat.convert_flat_to_meds(str(meds_flat_dataset), str(meds_dataset2), num_shards=10)

    patient_table = pa.concat_tables(
        [pq.read_table(meds_dataset2 / "data" / i) for i in os.listdir(meds_dataset2 / "data")]
    )

    final_patients = patient_table.to_pylist()
    final_patients.sort(key=lambda a: a["patient_id"])
    for patient in final_patients:
        for event in patient["events"]:
            event["measurements"].sort(key=lambda m: m["code"])

    assert patients == final_patients

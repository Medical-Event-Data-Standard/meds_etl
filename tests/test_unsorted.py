from __future__ import annotations

import datetime
import glob
import json
import os
import pathlib
import random
import shutil
from typing import List, Set

import jsonschema
import meds
import pyarrow as pa
import pyarrow.parquet as pq

import meds_etl.unsorted

try:
    import meds_etl_cpp
except ImportError:
    meds_etl_cpp = None


def get_random_subject(subject_id: int, include_properties=True) -> List[dict]:
    random.seed(subject_id)

    epoch = datetime.datetime(1990, 1, 1)
    birth = epoch + datetime.timedelta(days=random.randint(100, 1000))
    current_date = birth

    gender = "Gender/" + random.choice(["F", "M"])
    race = "Race/" + random.choice(["White", "Non-White"])

    subject = [
        {"subject_id": subject_id, "time": birth, "code": meds.birth_code},
        {"subject_id": subject_id, "time": birth, "code": gender},
        {"subject_id": subject_id, "time": birth, "code": race},
    ]

    code_cats = ["ICD9CM", "RxNorm"]
    for i in range(random.randint(1, 3 + (3 if gender == "Gender/F" else 0))):
        code_cat = random.choice(code_cats)
        if code_cat == "RxNorm":
            code = str(random.randint(0, 10000))
        else:
            code = str(random.randint(0, 10000))
            if len(code) > 3:
                code = code[:3] + "." + code[3:]
        if subject_id == 0 and i == 0:
            code_cat = "Random"
        current_date = current_date + datetime.timedelta(days=random.randint(1, 100))
        code = code_cat + "/" + code
        subject.append(
            {"subject_id": subject_id, "time": current_date, "code": code, "number": 100, "ontology": code_cat}
        )

    if not include_properties:
        for e in subject:
            if "ontology" in e:
                del e["ontology"]
            if "number" in e:
                del e["number"]

    subject.sort(key=lambda a: (a["time"], a["code"]))
    return subject


def create_example_subjects(include_properties=True):
    subjects = []
    for i in range(200):
        subjects.extend(get_random_subject(i, include_properties=include_properties))
    return subjects


def create_dataset(tmp_path: pathlib.Path, include_properties=True):
    os.makedirs(tmp_path / "data", exist_ok=True)

    if not include_properties:
        properties_schema = []
    else:
        properties_schema = [
            ("dummy", pa.large_string()),
            ("number", pa.int64()),
            ("ontology", pa.large_string()),
        ]

    subjects = create_example_subjects(include_properties=include_properties)
    subject_schema = meds.data_schema(properties_schema)

    subject_table = pa.Table.from_pylist(subjects, subject_schema)

    pq.write_table(subject_table, tmp_path / "data" / "subjects.parquet")

    metadata = {
        "dataset_name": "synthetic datata",
        "dataset_version": "1",
        "etl_name": "synthetic data",
        "etl_version": "1",
    }

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata_schema)

    (tmp_path / "metadata").mkdir()

    with open(tmp_path / "metadata" / "dataset.json", "w") as f:
        json.dump(metadata, f)

    return subjects, subject_schema


def roundtrip_helper(tmp_path: pathlib.Path, subjects: List[meds.subject], num_proc: int):
    for backend in ["polars", "cpp"]:
        if (backend == "cpp") and (meds_etl_cpp is None):
            continue
        print("Testing", backend, num_proc)
        meds_dataset = tmp_path / "meds"
        meds_unsorted_dataset = tmp_path / f"meds_unsorted_{num_proc}_{backend}"
        meds_dataset2 = tmp_path / f"meds2_{num_proc}_{backend}"

        meds_unsorted_dataset.mkdir()

        shutil.copytree(meds_dataset / "data", meds_unsorted_dataset / "unsorted_data")
        shutil.copytree(meds_dataset / "metadata", meds_unsorted_dataset / "metadata")

        meds_etl.unsorted.sort(
            str(meds_unsorted_dataset), str(meds_dataset2), num_proc=num_proc, num_shards=num_proc, backend=backend
        )

        print(meds_dataset2)

        subject_table = pa.concat_tables(
            [pq.read_table(meds_dataset2 / "data" / i) for i in os.listdir(meds_dataset2 / "data")]
        )
        final_subjects = subject_table.to_pylist()
        final_subjects.sort(key=lambda a: (a["subject_id"], a["time"], a["code"]))

        assert final_subjects == subjects


def test_roundtrip_with_properties(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset)
    subjects = pq.read_table(meds_dataset / "data" / "subjects.parquet").to_pylist()
    subjects.sort(key=lambda a: a["subject_id"])

    roundtrip_helper(tmp_path, subjects, 1)
    roundtrip_helper(tmp_path, subjects, 4)


def test_roundtrip_no_properties(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset, include_properties=False)
    subjects = pq.read_table(meds_dataset / "data" / "subjects.parquet").to_pylist()
    subjects.sort(key=lambda a: a["subject_id"])

    roundtrip_helper(tmp_path, subjects, 1)
    roundtrip_helper(tmp_path, subjects, 4)


def test_shuffle_polars(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset)

    subjects = pq.read_table(meds_dataset / "data" / "subjects.parquet")

    meds_flat_dataset = tmp_path / "meds_unsorted"
    meds_flat_dataset.mkdir()
    shutil.copytree(meds_dataset / "metadata", meds_flat_dataset / "metadata")
    (meds_flat_dataset / "unsorted_data").mkdir()

    indices = list(range(len(subjects)))
    random.shuffle(indices)

    num_parts = 3
    rows_per_part = (len(indices) + num_parts - 1) // num_parts

    for a in range(num_parts):
        i = indices[a * rows_per_part : (a + 1) * rows_per_part]
        shuffled_subjects = subjects.take(i)
        pq.write_table(shuffled_subjects, meds_flat_dataset / "unsorted_data" / (str(a) + ".parquet"))

    meds_dataset2 = tmp_path / "meds2"

    meds_etl.unsorted.sort(str(meds_flat_dataset), str(meds_dataset2), num_shards=10, backend="polars")

    seen_subject_ids: Set[int] = set()

    for result in glob.glob(str(meds_dataset2 / "data" / "*")):
        print(result)
        data = pq.read_table(result)

        subject_ids = set(data["subject_id"])

        assert len(seen_subject_ids & subject_ids) == 0

        seen_subject_ids |= subject_ids

        mask = pa.compute.is_in(subjects["subject_id"], pa.array(subject_ids))

        comparison = subjects.filter(mask)

        assert comparison.shape == data.shape
        assert comparison.schema == data.schema

        assert comparison == data


def test_shuffle_cpp(tmp_path: pathlib.Path):
    meds_dataset = tmp_path / "meds"
    create_dataset(meds_dataset)

    subjects = pq.read_table(meds_dataset / "data" / "subjects.parquet")

    meds_flat_dataset = tmp_path / "meds_unsorted"
    meds_flat_dataset.mkdir()
    shutil.copytree(meds_dataset / "metadata", meds_flat_dataset / "metadata")
    (meds_flat_dataset / "unsorted_data").mkdir()

    indices = list(range(len(subjects)))
    random.shuffle(indices)

    num_parts = 3
    rows_per_part = (len(indices) + num_parts - 1) // num_parts

    for a in range(num_parts):
        i = indices[a * rows_per_part : (a + 1) * rows_per_part]
        shuffled_subjects = subjects.take(i)
        pq.write_table(shuffled_subjects, meds_flat_dataset / "unsorted_data" / (str(a) + ".parquet"))

    meds_dataset2 = tmp_path / "meds2"

    meds_etl.unsorted.sort(str(meds_flat_dataset), str(meds_dataset2), num_shards=10, backend="cpp")

    seen_subject_ids: Set[int] = set()

    for result in glob.glob(str(meds_dataset2 / "data" / "*")):
        print(result)
        data = pq.read_table(result).sort_by(
            [
                ("subject_id", "ascending"),
                ("time", "ascending"),
                ("code", "ascending"),
            ]
        )

        subject_ids = set(data["subject_id"])

        assert len(seen_subject_ids & subject_ids) == 0

        seen_subject_ids |= subject_ids

        mask = pa.compute.is_in(subjects["subject_id"], pa.array(subject_ids))

        comparison = subjects.filter(mask)

        print(data.schema)
        print(comparison.schema)

        print(comparison)
        print(data)

        print("----")

        assert comparison.shape == data.shape
        assert comparison.schema == data.schema

        assert data == comparison

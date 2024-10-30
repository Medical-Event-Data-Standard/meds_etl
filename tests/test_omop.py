import datetime
import tempfile
from pathlib import Path

import polars as pl

from meds_etl.omop import DEFAULT_VISIT_CONCEPT_ID, write_event_data


def test_discharged_to_concept_id_correct():
    """
    Test the tests.
    """
    # Define the schema and a sample record for the OMOP visit table
    visit_table_details = [
        {"fallback_concept_id": DEFAULT_VISIT_CONCEPT_ID, "file_suffix": "occurrence"},
        {
            "concept_id_field": "discharged_to_concept_id",
            "time_field_options": ["visit_end_datetime", "visit_end_date"],
            "file_suffix": "occurrence",
        },
    ]
    visit_occurrence = pl.DataFrame(
        {
            "visit_occurrence_id": [1, 2, 3],
            "person_id": [12345, 12345, 12345],
            "visit_concept_id": [9201, 9202, 9202],  # Example: 9201 for inpatient visit
            "visit_start_date": [datetime.date(2024, 10, 25), datetime.date(2024, 10, 30), datetime.date(2024, 11, 1)],
            "visit_end_date": [datetime.date(2024, 10, 28), datetime.date(2024, 10, 30), datetime.date(2024, 11, 1)],
            "visit_type_concept_id": [44818517, 44818517, 44818517],  # Example: 44818517 for primary care visit
            "provider_id": [56789, 56789, 56789],
            "care_site_id": [101, None, None],
            "visit_source_value": ["Visit/IP", "Visit/OP", "Visit/OP"],
            "visit_source_concept_id": [9201, 9202, 9202],  # Use 0 if no mapping exists
            "admitting_source_concept_id": [38004294, 0, 0],  # Example: 38004294 for Emergency Room
            "discharged_to_concept_id": [38004453, 0, None],  # Example: 38004453 for Home
            "preceding_visit_occurrence_id": [None, None, None],
        }
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        write_event_data(
            path_to_MEDS_unsorted_dir=tmpdir,
            get_batch=lambda: visit_occurrence.lazy(),
            table_name="visit",
            all_table_details=visit_table_details,
            concept_id_map={
                9201: "Visit/IP",
                9202: "Visit/OP",
                38004453: "SNOMED/38004453",
                319835: "Hypertension",
                45763524: "Diabetes",
            },
            concept_name_map={
                9201: "Inpatient Visit",
                9202: "Outpatient Visit",
                38004453: "Home",
                319835: "Hypertension",
                45763524: "Diabetes",
            },
        )
        expected_meds = pl.read_parquet(list(Path(tmpdir).glob("*.parquet")))
        assert len(expected_meds) == 4
        actual_meds_dicts = expected_meds.sort("time").select("code", "time").to_dicts()
        expected_meds_dicts = [
            {"code": "Visit/IP", "time": datetime.datetime(2024, 10, 25, 23, 59, 59)},
            {"code": "SNOMED/38004453", "time": datetime.datetime(2024, 10, 28, 23, 59, 59)},
            {"code": "Visit/OP", "time": datetime.datetime(2024, 10, 30, 23, 59, 59)},
            {"code": "Visit/OP", "time": datetime.datetime(2024, 11, 1, 23, 59, 59)},
        ]
        assert actual_meds_dicts == expected_meds_dicts

import os
import shutil
import subprocess

import pytest
from datasets import Dataset


def test_hello_world():
    """
    Test the tests.
    """
    assert "Hello World" == "Hello World"


def data_exists(data_folder):
    """
    data_folder
    """
    path = os.path.join("tests", "data", data_folder)
    return os.path.exists(path) and os.listdir(path)


@pytest.mark.skipif(not data_exists("mimic-iv-demo"), reason="Data not available, skipping tests")
class TestMimicETL:
    """
    Test the MIMIC ETL.
    """

    @classmethod
    def setup_class(cls):
        """
        Setup method that runs before any tests in the class.
        Used here to run the ETL process.
        """
        cls.source_path = os.path.join("tests", "data", "mimic-iv-demo")
        cls.destination_path = os.path.join(cls.source_path, "build")

        # Remove the build directory if it exists
        if os.path.exists(cls.destination_path):
            shutil.rmtree(cls.destination_path)

        # Run the ETL
        subprocess.run(["meds_etl_mimic", cls.source_path, cls.destination_path], check=True)

        # Initialize the dataset variable. Set it in the test_load_dataset method
        cls.dataset = None

    @classmethod
    def teardown_class(cls):
        """
        Teardown method to run after all tests in the class.
        This method deletes the ETL output.
        """
        if os.path.exists(cls.destination_path):
            shutil.rmtree(cls.destination_path)

    def test_destination_contains_files(self):
        """
        Check if the destination folder contains files after running the ETL.
        """
        files = os.listdir(self.destination_path)
        assert len(files) > 0, "Destination directory is empty. ETL did not produce any output."

    def test_load_dataset(self):
        """
        Check that the ETL output can be loaded as a 🤗 dataset
        """
        path = os.path.join(self.destination_path, "data")
        self.__class__.dataset = Dataset.from_parquet(os.path.join(path, "*"))
        assert self.dataset is not None, "Failed to load the dataset."

    def test_number_of_patients(self):
        """
        The demo contains 100 patients.
        """
        assert len(self.dataset) == 100

    def test_expected_features_exist(self):
        """
        The dataset should contain two columns: "patient_id" and "events".
        """
        expected_columns = ["patient_id", "events"]
        assert all(column in self.dataset.features for column in expected_columns)

    def test_expected_data_types(self):
        """
        Check that patient_id is an integer and events is a list.
        """
        assert self.dataset.features["patient_id"].dtype == "int64"
        assert isinstance(self.dataset[0]["events"], list)

import pytest
from typing import Dict
import os
import sys

sys.path.append(os.path.abspath("../.."))
from qprofiler import DataProfiler
from qprofiler import QTest

DATA_PATH = "datasets/loan-perf.csv"
TEST_DATA_PATH = "datasets/loan-perf-test.csv"


@pytest.fixture
def profiler() -> DataProfiler:
    profiler = DataProfiler()
    ref = profiler.scan_csv_file(DATA_PATH, unique_identifier="customerid")
    profiler.create_profile(ref, "reference", True)
    return profiler


@pytest.fixture
def checks(profiler: DataProfiler) -> QTest:
    return QTest(profile_path=profiler.profiler_config.joinpath("reference.yml"))


@pytest.fixture
def test_profile(checks: QTest) -> Dict:
    return checks.scan_csv_file(TEST_DATA_PATH, unique_identifier="customerid")


def test_profile_path_attribute(checks: QTest, profiler: DataProfiler) -> None:
    assert checks.profile_path == profiler.profiler_config.joinpath("reference.yml")


def test_exception_when_wrong_profile_path(profiler: DataProfiler) -> None:
    with pytest.raises(FileNotFoundError):
        QTest(profile_path=profiler.profiler_config.joinpath("refernce.yml"))


def test_number_of_columns_true(checks: QTest) -> None:
    # same training data
    ref_profile = checks.profile
    assert checks.check_number_of_columns(test_profile=ref_profile) == True


def test_number_of_columns_wrong(checks: QTest, test_profile: Dict) -> None:
    assert checks.check_number_of_columns(test_profile=test_profile) == False


def test_min_number_of_records(checks: QTest, test_profile: Dict) -> None:
    assert (
        checks.check_min_number_of_records(test_profile=test_profile, min_threshold=100)
        == True
    )


def test_max_number_of_records(checks: QTest, test_profile: Dict) -> None:
    assert (
        checks.check_max_number_of_records(
            test_profile=test_profile, max_threshold=1000
        )
        == False
    )


def test_if_numeric_below_thresh(checks: QTest, test_profile: Dict) -> None:
    assert (
        checks.check_numeric_below_thresh(
            test_profile=test_profile, min_thresh=15, col="termdays"
        )
        == True
    )

def test_if_no_constant_columns_in_ref(checks: QTest, test_profile: Dict) -> None:
    assert checks.check_if_no_constant_columns(checks.profile) == True

def test_if_constant_columns_in_test(checks: QTest, test_profile: Dict) -> None:
    assert checks.check_if_no_constant_columns(test_profile=test_profile) == True

def test_if_matched_constant_columns(checks: QTest, test_profile: Dict) -> None:
    assert checks.check_if_matched_const_columns(test_profile=test_profile) == True

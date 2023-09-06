from pathlib import Path
from .utils import Message
from .scan import ScanData
from typing import Dict, Optional, List, Union
import yaml

message = Message()


class QTest(ScanData):
    """
    all quality checks test cases that is needed to check quality of production(test)
    datasets, and how it is similar to development(train) datasets.

    inherit all methods from ScanData to scan and produce profile like saved profiles in
    profiler.

    Parameters
    ----------
    profile_path: path of the .yml saved profile.

    Attributes
    ----------
    profile: dictionary of the saved .yml profile.

    profile_path: path of the .yml saved profile.
    """

    def __init__(self, profile_path: Path) -> None:
        super().__init__()
        try:
            with open(profile_path, "r") as conf:
                self.profile = yaml.safe_load(conf)
                self.profile_path: Path = profile_path
        except FileNotFoundError:
            raise FileNotFoundError("no profile in this path")

    def check_number_of_columns(self, test_profile: Dict) -> bool:
        """
        check the number of columns in test dataset, if it matches
        the number of columns in reference dataset that have saved
        profile, then it return True else False.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if self.profile["number-of-columns"] == test_profile["number-of-columns"]:
            return True
        else:
            return False

    def check_min_number_of_records(
        self, test_profile: Dict, min_threshold: Optional[int] = None
    ) -> bool:
        """
        check the number of records in test datasets, there are two
        options either to add minimum threshold of acceptable number
        of records or to let the minimum number of records greater than or
        equal to number of records in reference dataset.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        min_thresh: the acceptable minimum number of records

        Returns
        -------
        boolen flag (True/False).
        """
        if min_threshold:
            if self.profile["number-of-records"] >= min_threshold:
                return True
            else:
                return False
        else:
            if self.profile["number-of-records"] >= test_profile["number-of-records"]:
                return True
            else:
                return False

    def check_max_number_of_records(
        self, test_profile: Dict, max_threshold: Optional[int] = None
    ) -> bool:
        """
        check the number of records in test datasets, there are two
        options either to add maximum threshold of acceptable number
        of records or to let the maximum number of records less than or
        equal to number of records in reference dataset.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        max_thresh: the acceptable maximum number of records

        Returns
        -------
        boolen flag (True/False).
        """
        if max_threshold:
            if self.profile["number-of-records"] <= max_threshold:
                return True
            else:
                return False
        else:
            if self.profile["number-of-records"] <= test_profile["number-of-records"]:
                return True
            else:
                return False

    def check_columns(self, test_profile: Dict) -> bool:
        """
        check the column names that exist in test dataset,
        and compare it to reference dataset, it both are identical
        then returns True else False.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if len(self.profile["columns"]) == len(test_profile["columns"]):
            counter = 0
            for idx in range(len(self.profile["columns"])):
                if self.profile["columns"][idx] == test_profile["columns"][idx]:
                    counter += 1
                    continue
                else:
                    break
            return True if counter == len(self.profile["columns"]) else False
        return False

    def check_schema(self, test_profile: Dict) -> bool:
        """
        check if test dataset schema is identical to
        reference dataset schema.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if len(self.profile["schema"]) == len(test_profile["schema"]):
            counter = 0
            for idx in range(len(self.profile["schema"])):
                if (
                    self.profile["schema"][self.profile["columns"][idx]]
                    == test_profile["schema"][test_profile["columns"][idx]]
                ):
                    counter += 1
                    continue
                else:
                    break
            return True if counter == len(self.profile["schema"]) else False
        return False

    def check_uid(self, test_profile: Dict) -> bool:
        """
        check if reference and test datasets have the same id.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if self.profile["unique-identifier"] == test_profile["unique-identifier"]:
            return True
        return False

    def check_numeric_columns(self, test_profile: Dict) -> bool:
        """
        check if reference and test datasets have the same numerical columns.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if len(self.profile["numeric-columns"]) == len(test_profile["numeric-columns"]):
            counter = 0
            for idx in range(len(self.profile["numeric-columns"])):
                if (
                    self.profile["numeric-columns"][idx]
                    == test_profile["numeric-columns"][idx]
                ):
                    counter += 1
                    continue
                else:
                    break
            return True if counter == len(self.profile["numeric-columns"]) else False
        return False

    def check_categorical_columns(self, test_profile: Dict) -> bool:
        """
        check if reference and test datasets have the same categorical columns.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if len(self.profile["categorical-columns"]) == len(
            test_profile["categorical-columns"]
        ):
            counter = 0
            for idx in range(len(self.profile["categorical-columns"])):
                if (
                    self.profile["categorical-columns"][idx]
                    == test_profile["categorical-columns"][idx]
                ):
                    counter += 1
                    continue
                else:
                    break
            return (
                True if counter == len(self.profile["categorical-columns"]) else False
            )
        return False

    def check_numeric_below_thresh(
        self,
        test_profile: Dict,
        min_thresh: Optional[str] = None,
        col: Optional[Union[List[str], str]] = None,
    ) -> bool:
        """
        check if test numerical columns aren't below minimum threshold of
        reference numerical columns or defined threshold.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        min_threshold: minimum threshold that all numeric column values
        must be above or equal to it.

        col: list of columns or column name to check, if not specified,
        will check all numeric columns.

        Returns
        -------
        boolen flag (True/False).
        """
        if col:
            if isinstance(col, str):
                if col in self.profile["numeric-columns-range"].keys():
                    if min_thresh:
                        if test_profile["numeric-columns-range"][col][0] >= min_thresh:
                            return True
                        return False
                    else:
                        if (
                            test_profile["numeric-columns-range"][col][0]
                            >= self.profile["numeric-columns-range"][col][0]
                        ):
                            return True
                        return False
                return False
            elif isinstance(col, List[str]):
                if min_thresh:
                    counter = 0
                    for val in col:
                        if col in self.profile["numeric-columns-range"].keys():
                            if (
                                test_profile["numeric-columns-range"][col][0]
                                >= min_thresh
                            ):
                                counter += 1
                                continue
                            else:
                                break
                        else:
                            break
                    return True if counter == len(col) else False
                else:
                    counter = 0
                    for val in col:
                        if col in self.profile["numeric-columns-range"].keys():
                            if (
                                test_profile["numeric-columns-range"][col][0]
                                >= self.profile["numeric-columns-range"][col][0]
                            ):
                                counter += 1
                                continue
                            else:
                                break
                        else:
                            break
                    return True if counter == len(col) else False
        else:
            if min_thresh:
                counter = 0
                for val in self.profile["numeric-columns-range"].keys():
                    if test_profile["numeric-columns-range"][val][0] >= min_thresh:
                        counter += 1
                        continue
                    else:
                        break
                return (
                    True
                    if counter == len(self.profile["numeric-columns-range"].keys())
                    else False
                )
            else:
                counter = 0
                for val in self.profile["numeric-columns-range"].keys():
                    if (
                        test_profile["numeric-columns-range"][val][0]
                        >= self.profile["numeric-columns-range"][val][0]
                    ):
                        counter += 1
                        continue
                    else:
                        break
                return (
                    True
                    if counter == len(self.profile["numeric-columns-range"].keys())
                    else False
                )

    def check_numeric_above_thresh(
        self,
        test_profile: Dict,
        max_thresh: Optional[str] = None,
        col: Optional[Union[List[str], str]] = None,
    ) -> bool:
        """
        check if test numerical columns aren't above maximum threshold of
        reference numerical columns or defined threshold.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        max_threshold: maximum threshold that all numeric column values
        must be below or equal to it.

        col: list of columns or column name to check, if not specified,
        will check all numeric columns.

        Returns
        -------
        boolen flag (True/False).
        """
        if col:
            if isinstance(col, str):
                if col in self.profile["numeric-columns-range"].keys():
                    if max_thresh:
                        if test_profile["numeric-columns-range"][col][1] <= max_thresh:
                            return True
                        return False
                    else:
                        if (
                            test_profile["numeric-columns-range"][col][1]
                            <= self.profile["numeric-columns-range"][col][1]
                        ):
                            return True
                        return False
                return False
            elif isinstance(col, List[str]):
                if max_thresh:
                    counter = 0
                    for val in col:
                        if col in self.profile["numeric-columns-range"].keys():
                            if (
                                test_profile["numeric-columns-range"][col][1]
                                <= max_thresh
                            ):
                                counter += 1
                                continue
                            else:
                                break
                        else:
                            break
                    return True if counter == len(col) else False
                else:
                    counter = 0
                    for val in col:
                        if col in self.profile["numeric-columns-range"].keys():
                            if (
                                test_profile["numeric-columns-range"][col][1]
                                <= self.profile["numeric-columns-range"][col][1]
                            ):
                                counter += 1
                                continue
                            else:
                                break
                        else:
                            break
                    return True if counter == len(col) else False
        else:
            if max_thresh:
                counter = 0
                for val in self.profile["numeric-columns-range"].keys():
                    if test_profile["numeric-columns-range"][val][1] <= max_thresh:
                        counter += 1
                        continue
                    else:
                        break
                return (
                    True
                    if counter == len(self.profile["numeric-columns-range"].keys())
                    else False
                )
            else:
                counter = 0
                for val in self.profile["numeric-columns-range"].keys():
                    if (
                        test_profile["numeric-columns-range"][val][1]
                        <= self.profile["numeric-columns-range"][val][1]
                    ):
                        counter += 1
                        continue
                    else:
                        break
                return (
                    True
                    if counter == len(self.profile["numeric-columns-range"].keys())
                    else False
                )

    def check_high_cardinality(self, test_profile: Dict, max_thresh: int = 10) -> bool:
        """
        check that categorical columns distinct values aren't above
        a maximum threshold to avoid high cardinality problems in
        production.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        max_threshold: maximum threshold that all categorical column values
        must be below or equal to it.

        Returns
        -------
        boolen flag (True/False).
        """
        counter = 0
        for col in test_profile["unique-categorical-values"].keys():
            if test_profile["unique-categorical-values"][col] <= max_thresh:
                counter += 1
            else:
                break
        return (
            True if counter == len(self.profile["unique-categorical-values"]) else False
        )

    def check_unique_categories(self, test_profile: Dict) -> bool:
        """
        check that number of distinct values in test categorical columns
        is equal to number of distinct values in reference ccategorical
        columns.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        counter = 0
        for col in self.profile["unique-categorical-values"].keys():
            if (
                test_profile["unique-categorical-values"][col]
                == self.profile["unique-categorical-values"][col]
            ):
                counter += 1
            else:
                break
        return (
            True if counter == len(self.profile["unique-categorical-values"]) else False
        )

    def check_missing_values(
        self, test_profile: Dict, max_thresh: Optional[int] = None
    ) -> bool:
        """
        check that missing values in columns aren't exceeded acceptable
        maximum threshold or number of missing values in reference columns.

        Parameters
        ----------
        test_profile: Dcitionary of the test dataset.

        max_thresh: maximum number of records that are accepted to be missing.

        Returns
        -------
        boolen flag (True/False).
        """
        counter = 0
        for col in self.profile["missing-values"].keys():
            if max_thresh:
                if test_profile["missing-values"][col] <= max_thresh:
                    counter += 1
                else:
                    break
            else:
                if (
                    test_profile["missing-values"][col]
                    <= self.profile["missing-values"][col]
                ):
                    counter += 1
                else:
                    break
        return True if counter == len(self.profile["missing-values"]) else False

    def check_row_duplicates(self, test_profile: Dict) -> bool:
        """
        check if there are full-row duplicates in test datasets.

        Parameters
        ----------
        test_profile: Dictionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if self.profile["duplicate_records"] == 0:
            if test_profile["duplicate_records"] == 0:
                return True
            return False
        return True
    
    def check_if_no_constant_columns(self, test_profile: Dict) -> bool:
        """
        check if there are columns that have only one value(constant column)
        in test dataset.

        Parameters
        ----------
        test_profile: Dictionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if len(test_profile["is_constatnt"]) == 0:
            return True
        else:
            return False
        
    def check_if_matched_const_columns(self, test_profile: Dict) -> bool:
        """
        check if the constant columns in reference profile is
        identical to test profile.

        Parameters
        ----------
        test_profile: Dictionary of the test dataset.

        Returns
        -------
        boolen flag (True/False).
        """
        if len(self.profile["is_constatnt"]) == 0 and \
              len(test_profile["is_constatnt"]) == 0:
            return True
        else:
            if len(test_profile["is_constatnt"]) != len(self.profile["is_constatnt"]):
                return False
            else:
                if set(test_profile["is_constatnt"]) \
                    .issubset(self.profile["is_constatnt"]):
                    return True
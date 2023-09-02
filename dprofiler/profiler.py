import polars as pl
import polars.selectors as cs
from pathlib import Path
from typing import Optional, List, Dict
import os
import ruamel.yaml
from .utils import Message

message = Message()

class DataProfiler:
    """
    Create a dataset profile as .yml file that can be used
    in many different senarios like validate, and check quality
    of similar datasets in production or test/validation datasets,
    also create data cleaning and automatic transformation pipeline
    to be used in certain conditions based on dataset.

    creating an instance of DataProfiler will automatically create a directory on 
    your current working directory(default), or any other path on your
    system that will hold .yml files of datasets.

    all profiles that will be created using this instance will be in 
    this directory.

    Parameters
    ----------
    path : path of the directory that holds all data-profiles .yml files.

    Attributes
    ----------
    cwd : current working directory.

    profiler_path : path of the directory that holds all data-profiles .yml files
    if passed as a parameter to DataProfiler, else it will be in the current working directory.

    profiler_config : path of the directory in your system.
    """
    def __new__(self, path:Optional[str] = None):
        message.printit("new data-profile created successfully.",
                        "info")
        return super().__new__(self)

    def __init__(self, path:Optional[str] = None) -> None:
        self.cwd = Path(os.getcwd())
        def _profiler_path() -> Path:
            if path:
                profiler_path = Path(path)
            else:
                profiler_path = self.cwd
            return profiler_path
        self.profiler_path = _profiler_path()
        def _create_config_dir() -> Path:
            try:
                profiler_dir = self.profiler_path.joinpath(".dprofiler")
                if not profiler_dir.exists():
                    profiler_dir.mkdir()
                else:
                    message.printit("profiler already exists",
                                    "warn")
                return profiler_dir
            except FileNotFoundError:
                raise FileNotFoundError(f"The system cannot find the path specified:{self.profiler_path}")
        self.profiler_config = _create_config_dir()
    
    def __str__(self) -> str:
        return f"Profile of:{self.profiler_path}"
    
    @staticmethod
    def calc_profile(file_path:str,
                     df:pl.DataFrame,
                     unique_id:str) -> Dict:
        """
        calculate the profile of the tabular dataset using polars,
        profile measurements:
        - file path on your system
        - number of columns of dataset.
        - number of records.
        - columns in dataset.
        - schema of the dataset (column:data-type).
        - unique identifier of the dataset.
        - all string-type columns.
        - all numeric-type columns.
        - range of numeric columns (min, max).
        - number of unique values in string-type columns.
        - number of missing records in columns.
        - number of duplicate records.

        Parameters
        ----------
        file_path : path of tabular dataset.
        df : polars dataframe.
        unique_id : unique identifier name in dataset.

        Returns
        -------
        dictionary that holds all data of profile.
        """
        cols:List[str] = df.columns
        schema:Dict = {col:f"{dtype}" for col, dtype in df.schema.items()}
        n_cols:int = df.width
        n_rows:int = df.height
        cat_cols:List[str] = df.select(cs.string()).columns
        num_cols:List[str] = df.select(cs.numeric()).columns
        unique_val_df = df.select(pl.col(pl.Utf8).n_unique())
        n_unique = {col:unique_val_df.select(col).item()\
                   for col in unique_val_df.columns}
        null_cols = {col:df.select(col).null_count().item()\
                    for col in cols}
        duplicate_records = df.is_duplicated().sum()
        num_cols_range = {col:[df.select(col).min().item(),
                               df.select(col).max().item()]\
                          for col in num_cols}
        return {
            "file": file_path,
            "number-of-columns":n_cols,
            "number-of-records":n_rows,
            "columns":cols,
            "schema":schema,
            "unique-identifier": unique_id,
            "categorical-columns": cat_cols,
            "numeric-columns": num_cols,
            "numeric-columns-range": num_cols_range,
            "unique-categorical-values": n_unique,
            "missing-values": null_cols,
            "duplicate_records": duplicate_records
        }
    
    def scan_csv_file(self, 
                      file_path:str,
                      unique_identifier:Optional[str] = None,
                      sep:str = ",") -> Dict:
        """
        scan csv file and returns dictionary that holds all
        information of profile .yml file.

        Parameters
        ----------
        file_path : path of tabular dataset.
        unique_identifier : unique identifier name in dataset.
        sep: separator of file, default is ','

        Returns
        -------
        dictionary that holds all data of profile.
        """
        df = pl.read_csv(file_path,
                         ignore_errors = True,
                         separator = sep)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def scan_parquet_file(self,
                          file_path:str,
                          unique_identifier:Optional[str] = None) -> Dict:
        """
        scan parquet file and returns dictionary that holds all
        information of profile .yml file.

        Parameters
        ----------
        file_path : path of tabular dataset.
        unique_identifier : unique identifier name in dataset.

        Returns
        -------
        dictionary that holds all data of profile.
        """
        df = pl.read_parquet(file_path)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def scan_json_file(self,
                       file_path:str,
                       unique_identifier:Optional[str] = None) -> Dict:
        """
        scan json file and returns dictionary that holds all
        information of profile .yml file.

        Parameters
        ----------
        file_path : path of tabular dataset.
        unique_identifier : unique identifier name in dataset.

        Returns
        -------
        dictionary that holds all data of profile.
        """
        df = pl.read_json(file_path)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def scan_excel_file(self,
                        file_path:str,
                        sheet_name:str,
                        unique_identifier:Optional[str] = None) -> Dict:
        """
        scan csv file and returns dictionary that holds all
        information of profile .yml file.

        Parameters
        ----------
        file_path : path of tabular dataset.
        sheet_name : sheet name of the dataset in Excel file.
        unique_identifier : unique identifier name in dataset.

        Returns
        -------
        dictionary that holds all data of profile.
        """
        df = pl.read_excel(file_path, sheet_name = sheet_name)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def create_profile(self,
                       data_profile:Dict,
                       file_name:str,
                       override:Optional[str] = None) -> None:
        """
        create .yml file of the dataset, that will contain all information
        of the dataset.

        Parameters
        ----------
        data_profile : dictionary that holds all information of dataset.
        file_name : file name of the .yml file to avoid duplication issues.
        override : this is the option to override the information in 
        .yml file if exist and rewrite the profile again.
        """
        if not (file_name.endswith(".yml") or file_name.endswith(".yaml")):
            file_name = file_name+".yml"
        if self.profiler_config.joinpath(file_name).exists():
            if override:
                with open(self.profiler_config.joinpath(file_name),
                          "w") as conf:
                    yaml = ruamel.yaml.YAML()
                    yaml.indent(sequence = 4, offset = 2)
                    yaml.dump(data_profile, conf)
            else:
                message.printit("profile already exists",
                                "warn")
        else:
            with open(self.profiler_config.joinpath(file_name),
                      "w") as conf:
                yaml = ruamel.yaml.YAML()
                yaml.indent(sequence = 4, offset = 2)
                yaml.dump(data_profile, conf)

    def del_profile(self, 
                    file_name:str) -> None:
        """
        delete .yml profile.

        Parameters
        ----------
        file_name : file name that will be deleted.
        """
        try:
            if not (file_name.endswith(".yml") or file_name.endswith(".yaml")):
                file_name = file_name+".yml"
            self.profiler_config.joinpath(file_name).unlink()
        except FileNotFoundError:
            raise FileNotFoundError(f"No Data Profile Exists in profiler with name: {file_name}")
    
    
    
import polars as pl
import polars.selectors as cs
from pathlib import Path
from typing import Optional, List, Dict
import os
import ruamel.yaml
from .utils import Message

message = Message()

class DataProfiler:
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
        df = pl.read_csv(file_path,
                         ignore_errors = True,
                         separator = sep)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def scan_parquet_file(self,
                          file_path:str,
                          unique_identifier:Optional[str] = None) -> Dict:
        df = pl.read_parquet(file_path)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def scan_json_file(self,
                       file_path:str,
                       unique_identifier:Optional[str] = None) -> Dict:
        df = pl.read_json(file_path)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def scan_excel_file(self,
                        file_path:str,
                        sheet_name:str,
                        unique_identifier:Optional[str] = None) -> Dict:
        df = pl.read_excel(file_path, sheet_name = sheet_name)
        return DataProfiler.calc_profile(file_path=file_path,
                                         df = df,
                                         unique_id=unique_identifier)
    
    def create_profile(self,
                       data_profile:Dict,
                       file_name:str,
                       override:Optional[str] = None) -> None:
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
        try:
            if not (file_name.endswith(".yml") or file_name.endswith(".yaml")):
                file_name = file_name+".yml"
            self.profiler_config.joinpath(file_name).unlink()
        except FileNotFoundError:
            raise FileNotFoundError(f"No Data Profile Exists in profiler with name: {file_name}")
    
    
    
import polars as pl
from pathlib import Path
from .utils import Message
from .scan import ScanData
from typing import Dict
import yaml

message = Message()

class QTest(ScanData):
    def __init__(self,
                 profile_path:Path) -> None:
        super().__init__()
        try:
            with open(profile_path, "r") as conf:
                self.profile = yaml.safe_load(conf)
                self.profile_path = profile_path
        except FileNotFoundError:
            raise FileNotFoundError("no profile in this path")
        
    def check_number_of_columns(self,
                                test_profile:Dict) -> bool:
        if self.profile["number-of-columns"] == test_profile["number-of-columns"]:
            return True
        else:
            return False

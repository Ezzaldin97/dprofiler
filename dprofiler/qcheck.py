from pathlib import Path
from .utils import Message
from .scan import ScanData
from typing import Dict, Optional, List, Union
import yaml

message = Message()

class QTest(ScanData):
    def __init__(self,
                 profile_path:Path) -> None:
        super().__init__()
        try:
            with open(profile_path, "r") as conf:
                self.profile = yaml.safe_load(conf)
                self.profile_path:Path = profile_path
        except FileNotFoundError:
            raise FileNotFoundError("no profile in this path")
        
    def check_number_of_columns(self,
                                test_profile:Dict) -> bool:
        if self.profile["number-of-columns"] == test_profile["number-of-columns"]:
            return True
        else:
            return False
    
    def check_min_number_of_records(self,
                                    test_profile:Dict,
                                    min_threshold:Optional[int] = None) -> bool:
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
            
    def check_max_number_of_records(self,
                                    test_profile:Dict,
                                    max_threshold:Optional[int] = None) -> bool:
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
            
    def check_columns(self,
                      test_profile:Dict) -> bool:
        if len(self.profile["columns"]) == len(test_profile["columns"]):
            counter = 0
            for idx in range(len(self.profile["columns"])):
                if self.profile["columns"][idx] == test_profile["columns"][idx]:
                    counter+=1
                    continue
                else:
                    break
            return True if counter == len(self.profile["columns"]) else False
        return False
    
    def check_schema(self,
                     test_profile:Dict) -> bool:
        if len(self.profile["schema"]) == len(test_profile["schema"]):
            counter = 0
            for idx in range(len(self.profile["schema"])):
                if self.profile["schema"][self.profile["columns"][idx]]\
                   == test_profile["schema"][test_profile["columns"][idx]]:
                    counter+=1
                    continue
                else:
                    break
            return True if counter == len(self.profile["schema"]) else False
        return False

    def check_uid(self,
                  test_profile:Dict) -> bool:
        if self.profile["unique-identifier"]\
              == test_profile["unique-identifier"]:
            return True
        return False
    
    def check_numeric_columns(self,
                              test_profile:Dict) -> bool:
        if len(self.profile["numeric-columns"])\
            == len(test_profile["numeric-columns"]):
            counter = 0
            for idx in range(len(self.profile["numeric-columns"])):
                if self.profile["numeric-columns"][idx]\
                    == test_profile["numeric-columns"][idx]:
                    counter+=1
                    continue
                else:
                    break
            return True if counter == len(self.profile["numeric-columns"]) else False
        return False
    
    def check_categorical_columns(self,
                                  test_profile:Dict) -> bool:
        if len(self.profile["categorical-columns"])\
            == len(test_profile["categorical-columns"]):
            counter = 0
            for idx in range(len(self.profile["categorical-columns"])):
                if self.profile["categorical-columns"][idx]\
                    == test_profile["categorical-columns"][idx]:
                    counter+=1
                    continue
                else:
                    break
            return True if counter == len(self.profile["categorical-columns"]) else False
        return False
    
    def check_numeric_below_thresh(self,
                                   test_profile:Dict,
                                   min_thresh:Optional[str] = None,
                                   col:Optional[Union[List[str],
                                                      str]] = None
                                   ) -> bool:
        if col:
            if isinstance(col, str):
                if col in self.profile["numeric-columns-range"].keys():
                    if min_thresh:
                        if test_profile["numeric-columns-range"][col][0] \
                            < min_thresh:
                            return True
                        return False
                    else:
                        if test_profile["numeric-columns-range"][col][0] \
                            < self.profile["numeric-columns-range"][col][0]:
                            return True
                        return False
                return False
            elif isinstance(col, List[str]):
                if min_thresh:
                    counter = 0
                    for val in col:
                        if col in self.profile["numeric-columns-range"].keys():
                            if test_profile["numeric-columns-range"][col][0] \
                                < min_thresh:
                                counter+=1
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
                            if test_profile["numeric-columns-range"][col][0] \
                                < self.profile["numeric-columns-range"][col][0]:
                                counter+=1
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
                    if test_profile["numeric-columns-range"][val][0] \
                        < min_thresh:
                        counter+=1
                        continue
                    else:
                        break
                return True if counter == len(self.profile["numeric-columns-range"].keys()) else False
            else:
                counter = 0
                for val in self.profile["numeric-columns-range"].keys():
                    if test_profile["numeric-columns-range"][val][0] \
                        < self.profile["numeric-columns-range"][val][0]:
                        counter+=1
                        continue
                    else:
                        break
                return True if counter == len(self.profile["numeric-columns-range"].keys()) else False

    def check_numeric_above_thresh(self,
                                   test_profile:Dict,
                                   max_thresh:Optional[str] = None,
                                   col:Optional[Union[List[str],
                                                      str]] = None
                                   ) -> bool:
        if col:
            if isinstance(col, str):
                if col in self.profile["numeric-columns-range"].keys():
                    if max_thresh:
                        if test_profile["numeric-columns-range"][col][1] \
                            > max_thresh:
                            return True
                        return False
                    else:
                        if test_profile["numeric-columns-range"][col][1] \
                            > self.profile["numeric-columns-range"][col][1]:
                            return True
                        return False
                return False
            elif isinstance(col, List[str]):
                if max_thresh:
                    counter = 0
                    for val in col:
                        if col in self.profile["numeric-columns-range"].keys():
                            if test_profile["numeric-columns-range"][col][1] \
                                > max_thresh:
                                counter+=1
                                continue
                            else:
                                break
                        else:
                            break
                    return True if counter == len(col)\
                      else False
                else:
                    counter = 0
                    for val in col:
                        if col in self.profile["numeric-columns-range"].keys():
                            if test_profile["numeric-columns-range"][col][1] \
                                > self.profile["numeric-columns-range"][col][1]:
                                counter+=1
                                continue
                            else:
                                break
                        else:
                            break
                    return True if counter == len(col)\
                      else False
        else:
            if max_thresh:
                counter = 0
                for val in self.profile["numeric-columns-range"].keys():
                    if test_profile["numeric-columns-range"][val][1] \
                        > max_thresh:
                        counter+=1
                        continue
                    else:
                        break
                return True if counter == len(self.profile["numeric-columns-range"].keys()) else False
            else:
                counter = 0
                for val in self.profile["numeric-columns-range"].keys():
                    if test_profile["numeric-columns-range"][val][1] \
                        > self.profile["numeric-columns-range"][val][1]:
                        counter+=1
                        continue
                    else:
                        break
                return True if counter == len(self.profile["numeric-columns-range"].keys()) else False

    def check_high_cardinality(self,
                               test_profile:Dict,
                               max_thresh:int = 10
                               ) -> bool:
        counter = 0
        for col in test_profile["unique-categorical-values"].keys():
            if test_profile["unique-categorical-values"][col] <= max_thresh:
                counter+=1
            else:
                break
        print(counter)
        return True if counter == len(self.profile["unique-categorical-values"]) else False
    
    def check_unique_categories(self,
                                test_profile:Dict
                                ) -> bool:
        pass


    

            


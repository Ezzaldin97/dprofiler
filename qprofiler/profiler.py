from pathlib import Path
from typing import Optional, Dict
import os
import ruamel.yaml
from datetime import datetime
from shutil import rmtree
from .utils import Message
from .scan import ScanData
from .utils import DirTree
from .metadata import FileSystem
from .metadata import MetadataDB

message = Message()


class DataProfiler(ScanData):
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
    if passed as a parameter to DataProfiler, else it will be in
    the current working directory.

    profiler_config : path of the directory in your system.
    """

    def __init__(self, path: Optional[str] = None) -> None:
        super().__init__()
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
                    message.printit("new data-profile created successfully.", "info")
                else:
                    message.printit("profiler already exists", "warn")
                return profiler_dir
            except FileNotFoundError:
                raise FileNotFoundError(f"{self.profiler_path} doesn't exist")

        self.profiler_config = _create_config_dir()
        self._fs = FileSystem(self.profiler_config)
        self._metadata = MetadataDB(self.profiler_config)

    def __str__(self) -> str:
        """ provide .dprofiler path """
        return f"Profile of:{self.profiler_path}"

    def create_profile(
        self, data_profile: Dict, file_name: str, override: Optional[bool] = None
    ) -> None:
        """
        create .yml file of the dataset, that will contain all information
        of the dataset.

        Parameters
        ----------
        data_profile : dictionary that holds all information of dataset.
        file_name : file name of the .yml file.
        override : this is the option to override the information in
        .yml file if exist and rewrite the profile again.
        """
        project = file_name
        if not (file_name.endswith(".yml") or file_name.endswith(".yaml")):
            file_name = file_name + ".yml"
        if self._fs.in_use.joinpath(project).joinpath(file_name).exists():
            if override:
                archieve_ts = self._fs.archive_file(
                    project=project, file_name=file_name
                )
                with open(
                    self._fs.in_use.joinpath(project).joinpath(file_name), "w"
                ) as conf:
                    yaml = ruamel.yaml.YAML()
                    yaml.indent(sequence=4, offset=2)
                    yaml.dump(data_profile, conf)
                self._metadata.update_project(
                    project=project,
                    created_at=datetime.now(),
                    archieved_at=datetime.strptime(archieve_ts, "%Y-%m-%d %H:%M:%S.%f"),
                )

            else:
                message.printit("profile already exists", "warn")
        else:
            project_path = self._fs.construct_new(project=project)
            with open(project_path.joinpath(file_name), "w") as conf:
                yaml = ruamel.yaml.YAML()
                yaml.indent(sequence=4, offset=2)
                yaml.dump(data_profile, conf)
            self._metadata.add_project(project=project, created_at=datetime.now())

    def update_profile(self, data_profile: Dict, file_name: str) -> None:
        """
        update data quality profile.

        archieve quality profile file, and add the new data quality
        profile (.yml) to in-use directory

        update the metadata of the project as follows:
        - update the archieve datetime.
        - insert a new record with the new creation datetime.

        Parameters
        ----------
        data_profile : dictionary that holds all information of dataset.

        file_name : file name of the .yml file,
        currently the project name is same as file name.
        """
        project = file_name
        if not (file_name.endswith(".yml") or file_name.endswith(".yaml")):
            file_name = file_name + ".yml"
        archieve_ts = self._fs.archive_file(project=project, file_name=file_name)
        with open(self._fs.in_use.joinpath(project).joinpath(file_name), "w") as conf:
            yaml = ruamel.yaml.YAML()
            yaml.indent(sequence=4, offset=2)
            yaml.dump(data_profile, conf)
        self._metadata.update_project(
            project=project,
            created_at=datetime.now(),
            archieved_at=datetime.strptime(archieve_ts, "%Y-%m-%d %H:%M:%S.%f"),
        )

    def del_profile(self, file_name: str) -> None:
        """
        delete .yml profile.

        Parameters
        ----------
        file_name : file name that will be deleted.
        """
        self._fs.del_project(project=file_name)
        self._metadata.del_project(project=file_name)

    def format_profiler(self) -> None:
        """ Remove .dprofiler from you system. """
        rmtree(self.profiler_config)
        message.printit("profiler removed successfully from your file system.")

    def profiler_tree(self) -> str:
        """
        Generate the File Structure of profiler as tree

        Returns
        -------
        str of tree file structure of profiler
        """
        profiler_tree = DirTree(self.profiler_config)
        return profiler_tree.generate()

    def profiler_metadata(self) -> None:
        """
        get metadata of the stored quality profiles
        """
        self._metadata.get_metadata()

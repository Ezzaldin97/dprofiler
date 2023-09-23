import pathlib
from shutil import rmtree
from datetime import datetime
from typing import Callable, Any


class FileSystem:
    """
    create and organize the file structure of .dprofiler
    directory

    .dprofiler must contain archive, and in_use directories
    archive directory contain a set of directories of specific project,
    each directory contain a history profiles of this project.

    creating an instance of FileSystem will create the file structure of
    .dprofiler.

    calling any method will automatically check the file structure,
    to make sure that everything is up to date as expected, and to
    avoid any actions.

    Parameters
    ----------
    root: pathlib.Path,
    path of the .dprofiler directory

    """

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        if not root.joinpath("archive").exists():
            root.joinpath("archive").mkdir()
        self.archive = root.joinpath("archive")
        if not root.joinpath("in_use").exists():
            root.joinpath("in_use").mkdir()
        self.in_use = root.joinpath("in_use")
        self.projects = [project.name for project in root.joinpath("in_use").iterdir()]

    def status(func: Callable[..., Any]):
        """check file structure of .dprofile file system"""

        def wrapper(self, *args, **kwargs):
            if self.archive.exists() and self.in_use.exists():
                archive_projects = [project.name for project in self.archive.iterdir()]
                in_use_projects = [project.name for project in self.in_use.iterdir()]
                if (
                    archive_projects == self.projects
                    and in_use_projects == self.projects
                ):
                    return func(self, *args, **kwargs)
                else:
                    raise Exception("archive and in_use are different.")
            else:
                raise Exception("file structure is not correct.")

        return wrapper

    @status
    def construct_new(self, project: str) -> pathlib.Path:
        """
        create new directory of project in .dprofiler
        file system

        Parameters
        ----------
        project: str,
        project name
        """
        try:
            self.in_use.joinpath(project).mkdir()
            self.archive.joinpath(project).mkdir()
            self.projects.append(project)
            return self.in_use.joinpath(project)
        except FileExistsError:
            raise FileExistsError(f"{project} already exists")

    @status
    def archive_file(self, project: str, file_name: str) -> str:
        """
        removes the lastest version file of project
        from
        .dprofiler/in_use/{project}
        to
        .dprofiler/in_use/{archive}

        Parameters
        ----------
        project: str,
        project name

        file_name: str,
        file name of the file.

        Returns
        -------
        timestamp of archiving action.
        """
        ts = datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")
        ts_other = datetime.today().strftime("%Y-%m-%d-%H-%M-%S.%f")
        try:
            self.in_use.joinpath(project).joinpath(file_name).rename(
                self.archive.joinpath(project).joinpath(f"{project}-{ts_other}.yml")
            )
            return ts
        except FileNotFoundError:
            raise FileNotFoundError(
                f"{file_name} doesn't exist in {self.in_use.name} directory"
            )

    @status
    def del_project(self, project: str) -> None:
        """
        remove the directory of project from .dprofiler
        file system

        Parameters
        ----------
        project: str,
        project name
        """
        if project in self.projects:
            rmtree(self.in_use.joinpath(project))
            rmtree(self.archive.joinpath(project))
            self.projects.remove(project)
        else:
            raise FileNotFoundError(f"{project} not exist, double-check project name")

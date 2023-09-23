import polars as pl
import sqlite3 as sql
import pathlib
from datetime import datetime


class MetadataDB:
    """
    stores, manage, update, delete and get metadata
    of saved quality profiles.

    when start saving new profile, it added to the metadata
    database, and stores the creation datetime, and also
    prepare metadata of archieving.

    updating the quality profile will archieve the old profile,
    and add a new one to the in-use profiles.

    when deleting the profiles, this will remove all the history of
    the project including all the metadata.

    Parameters
    ----------
    root: pathlib.Path object
    path of the metadata database directory.
    """

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.db_path = root.joinpath("metadata.db")
        try:
            conn = sql.connect(root.joinpath("metadata.db"))
            cur = conn.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS in_use(
              profile_name VARCHAR(100) PRIMARY KEY,
              created_at TIMESTAMP          
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS archieved(
              archieve_id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TIMESTAMP,
              archieved_at TIMESTAMP DEFAULT NULL,
              profile_name VARCHAR(100),
              FOREIGN KEY (profile_name) REFERENCES in_use (profile_name)          
            );
            """)
            conn.commit()
            conn.close()
        except Exception as exp:
            raise Exception(f"error due to {exp}")

    def add_project(self, project: str, created_at: datetime) -> None:
        """
        add new quality profile to metadata.

        Parameters
        ----------
        project: str
        profile quality file name

        created_at: datetime object
        timestamp of the creation date of the new profile.
        """
        conn = sql.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM in_use WHERE 'profile_name' = '{project}' ;")
        if len(cur.fetchall()) == 0:
            if isinstance(created_at, datetime):
                cur.execute("INSERT INTO in_use VALUES(?, ?)", (project, created_at))
                cur.execute(
                    "INSERT INTO archieved ('created_at', 'profile_name') VALUES(?, ?)",
                    (created_at, project),
                )
                conn.commit()
                conn.close()
            else:
                conn.close()
                raise TypeError(f"{created_at} is not a datetime type.")
        else:
            conn.close()
            raise Exception(f"{project} is already in-use.")

    def update_project(
        self, project: str, created_at: datetime, archieved_at: datetime
    ) -> None:
        """
        when updating the quality profile of dataset,
        old quality profile moved to archieved files
        and the new one created.
        it updates the information of the archieving and creation
        information of the updated profile.

        Parameters
        ----------
        project: str
        profile quality file name

        created_at: datetime object
        timestamp of the creation date of the updated profile.

        archieved_at: datetime object
        timestamp of archieving date of old profile.
        """
        conn = sql.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM in_use WHERE profile_name = '{project}' ;")
        if len(cur.fetchall()) > 0:
            if isinstance(created_at, datetime) and isinstance(archieved_at, datetime):
                created_at, archieved_at = created_at.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ), archieved_at.strftime("%Y-%m-%d %H:%M:%S")
                cur.execute(
                    f"UPDATE archieved SET archieved_at = '{archieved_at}' WHERE"
                    f" archieved_at IS NULL AND profile_name = '{project}';"
                )
                cur.execute(
                    "INSERT INTO archieved ('created_at', 'profile_name') VALUES(?, ?)",
                    (created_at, project),
                )
                cur.execute(f"DELETE FROM in_use WHERE profile_name = '{project}'")
                cur.execute("INSERT INTO in_use VALUES(?, ?)", (project, created_at))
                conn.commit()
                conn.close()
            else:
                conn.close()
                raise TypeError("passed parameters aren't a datetime type.")
        else:
            conn.close()
            raise Exception(f"{project} isn't in in-use projects.")

    def del_project(self, project: str) -> None:
        conn = sql.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(f"DELETE FROM in_use WHERE profile_name = '{project}';")
        cur.execute(f"DELETE FROM archieved WHERE profile_name = '{project}';")
        conn.commit()
        conn.close()

    def get_metadata(self) -> None:
        """
        show an aggregated view of metadata for
        all stored projects.
        metadata of the project is:
        - profile name.
        - last creation date.
        - last archieving date.
        - number of archieved files.
        """
        conn = sql.connect(self.db_path)
        q = """
        SELECT profile_name,
               MAX(created_at) AS last_creation_date,
               MAX(archieved_at) AS last_archieving_date,
               COUNT(*) AS no_of_archieves
        FROM archieved
        GROUP BY 1;
        """
        df = pl.read_database(query=q, connection=conn)
        print(df)
        conn.close()

import os
import sqlite3
from threading import Lock
from typing import Union


class SqliteDriver():
    """
    To handle concurrency while using sqlite3
    """

    # Named execution mode
    FETCH_ALL = 0
    FETCH_ONE = 1
    COMMIT = 2

    def __init__(self, db_path: str):
        self._lock = Lock()
        if os.path.exists(db_path):
            self.db_exists = True
        else:
            self.db_exists = False

        self._conn = sqlite3.connect(db_path, check_same_thread=False)        # User must handle concurrency on his own
        self._cursor = self._conn.cursor()
        self._cursor.execute('PRAGMA foreign_keys = ON')

    def execute(self, command_str: str, type: Union[str, None] = None):
        """
        This will be executed by Models

        `type` includes:
         - None: for create/drop table, delete row
         - 'fetchall': for select several rows
         - 'fetchone': for select one row
         - 'commit': for update/insert row

        """
        # print(f'check lock {self._lock.locked()}')
        self._lock.acquire()
        print(command_str)
        # res = self._cursor.execute(command_str)
        # print('a')

        output = None
        try:
            if type is None:
                self._cursor.execute(command_str)
            elif type == self.FETCH_ALL:
                res = self._cursor.execute(command_str)
                output = res.fetchall()
            elif type == self.FETCH_ONE:
                res = self._cursor.execute(command_str)
                output = res.fetchone()
            elif type == self.COMMIT:
                self._cursor.execute(command_str)
                self._conn.commit()
        except Exception as e:
            self._lock.release()
            print(e)
            raise e

        self._lock.release()
        # print('Lock must be released')

        return output

    def get_last_insert_rowid(self):
        return self._cursor.lastrowid

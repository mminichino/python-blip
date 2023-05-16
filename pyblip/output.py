##

import sqlite3
import os
import json
from .exceptions import OutputError


class LocalDB(object):

    def __init__(self, directory: str = None):
        if not directory:
            directory = os.environ.get('HOME') if os.environ.get('HOME') else "/var/tmp"
        self.directory = directory
        self.db_file = None
        self.con = None
        self.cur = None

        if not os.access(self.directory, os.W_OK):
            raise OutputError(f"Directory {self.directory} is not writable")

    def database(self, name: str):
        self.db_file = f"{self.directory}/{name}.db"
        self.con = sqlite3.connect(self.db_file)
        self.cur = self.con.cursor()

        self.cur.execute('''
           CREATE TABLE IF NOT EXISTS documents(
               doc_id TEXT PRIMARY KEY ON CONFLICT REPLACE, 
               document TEXT 
           )''')
        self.con.commit()

        return self

    def write(self, doc_id: str, document: str):
        self.cur.execute("INSERT OR REPLACE INTO documents VALUES (?, ?)", (doc_id, document))
        self.con.commit()


class LocalFile(object):

    def __init__(self, directory: str = None):
        if not directory:
            directory = os.environ.get('HOME') if os.environ.get('HOME') else "/var/tmp"
        self.directory = directory
        self.jsonl_file = None

        if not os.access(self.directory, os.W_OK):
            raise OutputError(f"Directory {self.directory} is not writable")

    def database(self, name: str):
        self.jsonl_file = f"{self.directory}/{name}.jsonl"

        try:
            open(self.jsonl_file, 'w').close()
        except Exception as err:
            raise OutputError(f"can not open file {self.jsonl_file}: {err}")

        return self

    def write(self, doc_id: str, document: str):
        try:
            with open(self.jsonl_file, 'a') as jsonl_file:
                line = {doc_id: document}
                jsonl_file.write(json.dumps(line) + '\n')
        except Exception as err:
            raise OutputError(f"can not write to file: {err}")


class ScreenOutput(object):

    def __init__(self):
        self._database = None

    def database(self, name: str):
        self._database = name
        return self

    @staticmethod
    def write(doc_id: str, document: str):
        line = {doc_id: json.loads(document)}
        print(json.dumps(line) + '\n')

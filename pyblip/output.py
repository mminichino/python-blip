##

import sqlite3
import os
import json
import re
from typing import Union
import mimetypes
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
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS attachments(
                doc_id TEXT PRIMARY KEY ON CONFLICT REPLACE,
                content_type TEXT,
                data BLOB
            )''')
        self.con.commit()

        return self

    def write(self, doc_id: str, document: Union[dict, str]):
        if type(document) == dict:
            document = json.dumps(document)
        self.cur.execute("INSERT OR REPLACE INTO documents VALUES (?, ?)", (doc_id, document))
        self.con.commit()

    def write_attachment(self, doc_id: str, c_type: str, data: bytes):
        self.cur.execute("INSERT OR REPLACE INTO attachments VALUES (?, ?, ?)", (doc_id, c_type, data))
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

    def write(self, doc_id: str, document: Union[dict, str]):
        try:
            with open(self.jsonl_file, 'a') as jsonl_file:
                line = {doc_id: document}
                jsonl_file.write(json.dumps(line) + '\n')
        except Exception as err:
            raise OutputError(f"can not write to file: {err}")

    def write_attachment(self, doc_id: str, c_type: str, data: bytes):
        extension = mimetypes.guess_all_extensions(c_type)[0]
        file_prefix = re.sub(r'[#%&{}<>*?$!:@+|=\\/\'\s`\"]', '_', doc_id).strip().lower()
        filename = f"{self.directory}/{file_prefix}{extension}"
        try:
            with open(filename, 'wb') as data_file:
                data_file.write(data)
                data_file.close()
        except Exception as err:
            raise OutputError(f"can not write to file: {err}")


class ScreenOutput(object):

    def __init__(self):
        self._database = None

    def database(self, name: str):
        self._database = name
        return self

    @staticmethod
    def write(doc_id: str, document: Union[dict, str]):
        line = {doc_id: document}
        print(json.dumps(line))

    @staticmethod
    def write_attachment(doc_id: str, c_type: str, data: bytes):
        print(f"Attachment from document {doc_id} of type {c_type} length {len(data)}")

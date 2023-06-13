##

import sqlite3
import os
import json
import re
from typing import Union
import mimetypes
import logging
from .exceptions import OutputError

logger = logging.getLogger('pythonblip.output')
logger.addHandler(logging.NullHandler())


class LocalDB(object):

    def __init__(self, directory: str = None):
        if not directory:
            directory = os.environ.get('HOME') if os.environ.get('HOME') else "/var/tmp"
        self.directory = directory
        self.db_files = {}
        self._database = None
        self.db_file = None
        self.con = None
        self.cur = None

        if not os.access(self.directory, os.W_OK):
            raise OutputError(f"Directory {self.directory} is not writable")

    def database(self, database: str, collections: list[str]):
        self._database = database
        for collection in collections:
            name = collection if collection != "_default" else database
            self.db_files[name] = {}
            self.db_files[name]["db_file"] = f"{self.directory}/{name}.db"
            self.db_files[name]["con"] = sqlite3.connect(self.db_files[name]["db_file"])
            self.db_files[name]["cur"] = self.db_files[name]["con"].cursor()

            self.db_files[name]["cur"].execute('''
               CREATE TABLE IF NOT EXISTS documents(
                   doc_id TEXT PRIMARY KEY ON CONFLICT REPLACE,
                   document TEXT 
               )''')
            self.db_files[name]["cur"].execute('''
                CREATE TABLE IF NOT EXISTS attachments(
                    doc_id TEXT PRIMARY KEY ON CONFLICT REPLACE,
                    content_type TEXT,
                    data BLOB
                )''')
            self.db_files[name]["con"].commit()

        return self

    def write(self, doc_id: str, document: Union[dict, str], collection: str = None):
        name = collection if collection and collection != "_default" else self._database
        if type(document) == dict:
            document = json.dumps(document)
        self.db_files[name]["cur"].execute("INSERT OR REPLACE INTO documents VALUES (?, ?)", (doc_id, document))
        self.db_files[name]["con"].commit()

    def write_attachment(self, doc_id: str, c_type: str, data: bytes, collection: str = None):
        name = collection if collection and collection != "_default" else self._database
        self.db_files[name]["cur"].execute("INSERT OR REPLACE INTO attachments VALUES (?, ?, ?)", (doc_id, c_type, data))
        self.db_files[name]["con"].commit()


class LocalFile(object):

    def __init__(self, directory: str = None):
        if not directory:
            directory = os.environ.get('HOME') if os.environ.get('HOME') else "/var/tmp"
        self.directory = directory
        self.jsonl_file = {}
        self._database = None

        if not os.access(self.directory, os.W_OK):
            raise OutputError(f"Directory {self.directory} is not writable")

    def database(self, database: str, collections: list[str]):
        self._database = database
        for collection in collections:
            name = collection if collection != "_default" else database
            self.jsonl_file[name] = f"{self.directory}/{name}.jsonl"

            try:
                open(self.jsonl_file[name], 'w').close()
            except Exception as err:
                raise OutputError(f"can not open file {self.jsonl_file[name]}: {err}")

        return self

    def write(self, doc_id: str, document: Union[dict, str], collection: str = None):
        name = collection if collection and collection != "_default" else self._database
        try:
            with open(self.jsonl_file[name], 'a') as jsonl_file:
                line = {doc_id: document}
                jsonl_file.write(json.dumps(line) + '\n')
        except Exception as err:
            raise OutputError(f"can not write to file: {err}")

    def write_attachment(self, doc_id: str, c_type: str, data: bytes, collection: str = None):
        name = collection if collection and collection != "_default" else self._database
        extension = mimetypes.guess_all_extensions(c_type)[0]
        file_prefix = re.sub(r'[#%&{}<>*?$!:@+|=\\/\'\s`\"]', '_', doc_id).strip().lower()
        filename = f"{self.directory}/{name}_{file_prefix}{extension}"
        try:
            with open(filename, 'wb') as data_file:
                data_file.write(data)
                data_file.close()
        except Exception as err:
            raise OutputError(f"can not write to file: {err}")


class ScreenOutput(object):

    def __init__(self):
        self._database = None
        self.collections = []

    def database(self, database: str, collections: list[str]):
        self._database = database
        for collection in collections:
            name = collection if collection != "_default" else database
            self.collections.append(name)
        return self

    @staticmethod
    def write(doc_id: str, document: Union[dict, str], collection: str = None):
        logger.debug(f"Screen Output {doc_id} from {collection}")
        line = {doc_id: document}
        print(json.dumps(line))

    @staticmethod
    def write_attachment(doc_id: str, c_type: str, data: bytes, collection: str = None):
        logger.debug(f"Screen Output: Attachment {doc_id} from {collection}")
        print(f"Attachment from document {doc_id} of type {c_type} length {len(data)}")

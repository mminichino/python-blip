##

import os
import attr
import sqlite3
import time
import logging
from attr.validators import instance_of
from enum import Enum
from typing import Union
from .headers import SessionAuth, BasicAuth
from .exceptions import ReplicationError, BLIPError, ClientError
from .protocol import BLIPProtocol

logger = logging.getLogger('pyblip.replicator')
logger.addHandler(logging.NullHandler())


class ReplicatorType(Enum):
    PULL = 1
    PUSH = 2
    PUSH_AND_PULL = 3


class LocalDB(object):

    def __init__(self, directory: str, database: str = "default"):
        self.directory = directory
        self.db_file = f"{self.directory}/{database}.db"

        if not os.access(self.directory, os.W_OK):
            raise ReplicationError(f"Directory {self.directory} is not writable")

        self.con = sqlite3.connect(self.db_file)
        self.cur = self.con.cursor()

        self.cur.execute('''
           CREATE TABLE IF NOT EXISTS documents(
               doc_id TEXT PRIMARY KEY ON CONFLICT REPLACE, 
               document TEXT 
           )''')
        self.con.commit()

    def write(self, doc_id: str, document: str):
        self.cur.execute("INSERT OR REPLACE INTO documents VALUES (?, ?)", (doc_id, document))
        self.con.commit()


@attr.s
class ReplicatorConfiguration(object):
    database = attr.ib(validator=instance_of(str))
    target = attr.ib(validator=instance_of(str))
    type = attr.ib(validator=instance_of(ReplicatorType))
    authenticator = attr.ib(validator=instance_of((SessionAuth, BasicAuth)))
    datastore = attr.ib(validator=instance_of(LocalDB))
    continuous = attr.ib(validator=instance_of(bool))

    @classmethod
    def create(cls, database: str,
               target: str,
               r_type: ReplicatorType,
               authenticator: Union[SessionAuth, BasicAuth],
               directory: str = "/var/tmp",
               continuous: bool = False):
        return cls(
            database,
            target,
            r_type,
            authenticator,
            LocalDB(directory, database),
            continuous
        )


class Replicator(object):

    def __init__(self, config: ReplicatorConfiguration):
        self.config = config
        self.get_checkpoint_props = {
            "Profile": "getCheckpoint",
            "client": "testClient"
        }
        self.set_checkpoint_props = {
            "Profile": "setCheckpoint",
            "client": "testClient",
            "rev": ""
        }
        self.set_checkpoint_body = {
            "time": int(time.time()),
            "remote": None
        }
        self.sub_changes_props = {
            "Profile": "subChanges",
            "versioning": "rev-trees",
            "activeOnly": True
        }
        self.max_history_props = {
            "maxHistory": 20,
            "blobs": True,
            "deltas": True
        }
        self.history_body = []
        self.blip = BLIPProtocol(self.config.target, self.config.authenticator.header())

    def start(self):
        try:
            self.blip.send_message(0, self.get_checkpoint_props)
            message = self.blip.receive_message()
        except BLIPError as err:
            if err.error_code:
                if err.error_code == 404:
                    logger.info("Previous checkpoint not found")
                else:
                    raise ReplicationError(f"Replication protocol error: {err}")
        except ClientError as err:
            if err.error_code == 401:
                raise ReplicationError("Unauthorized: invalid credentials provided.")
            else:
                raise ReplicationError(f"Websocket error: {err}")
        except Exception as err:
            logger.error(f"Replication error: {err}")
            raise ReplicationError(f"General error: {err}")

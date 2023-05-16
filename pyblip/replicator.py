##

import os
import attr
import sqlite3
import time
import logging
from hashlib import sha1
import base64
import uuid
import json
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
            f"{target}/{database}/_blipsync",
            r_type,
            authenticator,
            LocalDB(directory, database),
            continuous
        )


class Replicator(object):

    def __init__(self, config: ReplicatorConfiguration):
        self.config = config
        self.uuid = uuid.getnode()
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

    def get_uuid_hash(self):
        id_hash = sha1()

        id_hash.update(self.uuid)
        id_hash.update(self.config.database)
        id_hash.update(self.config.target)
        id_hash.update(self.config.type.name)

        r_uuid = id_hash.hexdigest()
        checkpoint = base64.b64encode(bytes.fromhex(r_uuid)).decode()
        return f"cp-{checkpoint}"

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
            raise ReplicationError(f"General error: {err}")

    def replicate(self):
        history_body = []
        try:
            sub_changes_message = self.blip.send_message(0, self.sub_changes_props)
            reply_message = self.blip.receive_message()
            doc_list = self.blip.receive_message()
            doc_count = json.loads(doc_list.body_as_string())
            for i in range(len(doc_count)):
                history_body.append([])
            max_history_msg = self.blip.send_message(1, self.max_history_props, reply=doc_list.number, body_json=history_body)
            changes_reply_msg = self.blip.receive_message()
            received_doc_count = 0
            for _ in range(len(doc_count)):
                reply_message = self.blip.receive_message()
                received_doc_count += 1
            logging.debug(f"Replicated {received_doc_count} documents")
        except BLIPError as err:
            raise ReplicationError(f"Replication protocol error: {err}")
        except ClientError as err:
            if err.error_code == 401:
                raise ReplicationError("Unauthorized: invalid credentials provided.")
            else:
                raise ReplicationError(f"Websocket error: {err}")
        except Exception as err:
            raise ReplicationError(f"General error: {err}")

    def stop(self):
        self.blip.stop()

##

import attr
import time
import logging
from hashlib import sha1, sha256
import base64
import uuid
import json
from attr.validators import instance_of
from enum import Enum
from typing import Union
from .headers import SessionAuth, BasicAuth
from .exceptions import ReplicationError, BLIPError, ClientError
from .protocol import BLIPProtocol
from .output import LocalDB, LocalFile, ScreenOutput

logger = logging.getLogger('pythonblip.replicator')
logger.addHandler(logging.NullHandler())


class ReplicatorType(Enum):
    PULL = 1
    PUSH = 2
    PUSH_AND_PULL = 3


@attr.s
class ReplicatorConfiguration(object):
    database = attr.ib(validator=instance_of(str))
    target = attr.ib(validator=instance_of(str))
    type = attr.ib(validator=instance_of(ReplicatorType))
    authenticator = attr.ib(validator=instance_of((SessionAuth, BasicAuth)))
    tls = attr.ib(validator=instance_of(bool))
    port = attr.ib(validator=instance_of(str))
    scope = attr.ib(validator=instance_of(str))
    collections = attr.ib(validator=instance_of(list))
    datastore = attr.ib(validator=instance_of((LocalDB, LocalFile, ScreenOutput)))
    continuous = attr.ib(validator=instance_of(bool))
    checkpoint = attr.ib(validator=instance_of(bool))

    @classmethod
    def create(cls, database: str,
               hostname: str,
               r_type: ReplicatorType,
               authenticator: Union[SessionAuth, BasicAuth],
               tls: bool = False,
               port: str = "4984",
               scope: str = "_default",
               collections: list[str] = None,
               output: Union[LocalDB, LocalFile, ScreenOutput] = None,
               continuous: bool = False,
               checkpoint: bool = True):
        if not collections:
            collections = ["_default"]
        if tls:
            prefix = "wss"
        else:
            prefix = "ws"
        return cls(
            database,
            f"{prefix}://{hostname}:{port}/{database}/_blipsync",
            r_type,
            authenticator,
            tls,
            port,
            scope,
            collections,
            output.database(database, collections),
            continuous,
            checkpoint
        )


class Replicator(object):

    def __init__(self, config: ReplicatorConfiguration):
        self.config = config
        self.uuid = str(uuid.getnode())
        self.client = self.get_id_hash()
        self.get_checkpoint_props = {
            "Profile": "getCheckpoint",
        }
        self.get_checkpoint_collections_props = {
            "Profile": "getCollections"
        }
        self.checkpoint_collections_body = {
            "checkpoint_ids": [],
            "collections": []
        }
        self.set_checkpoint_props = {
            "Profile": "setCheckpoint",
            "client": self.client,
            "rev": ""
        }
        self.set_checkpoint_body = {
            "time": int(time.time()),
            "remote": 0
        }
        self.set_checkpoint_body_list = []
        self.sub_changes_props = {
            "Profile": "subChanges",
            "versioning": "rev-trees",
            "activeOnly": True
        }
        self.sub_changes_collection_props = {
            "Profile": "subChanges",
            "collection": 0,
            "versioning": "rev-trees",
            "activeOnly": True
        }
        self.max_history_props = {
            "maxHistory": 20,
            "blobs": True,
            "deltas": True
        }
        self.get_attachment_props = {
            "Profile": "getAttachment",
            "digest": "",
            "docID": ""
        }
        self.history_body = []
        self.attachments = []
        self.collections = self.config.collections
        self.collection_list = []
        self.hash_list = []
        self.collection_rev_list = []
        if self.collections[0] != "_default":
            for collection in self.collections:
                _target = f"{self.config.scope}.{collection}"
                _hash = self.get_id_hash(self.config.scope, collection)
                self.collection_list.append(_target)
                self.hash_list.append(_hash)
        self.blip = BLIPProtocol(self.config.target, self.config.authenticator.header(), self.config.tls)
        logger.info(f"Replicator active for client {self.client}")

    def get_id_hash(self, scope: str = None, collection: str = None) -> str:
        if not collection and not scope:
            id_hash = sha1()
        else:
            id_hash = sha256()

        id_hash.update(self.uuid.encode('utf-8'))
        id_hash.update(self.config.database.encode('utf-8'))
        id_hash.update(self.config.target.encode('utf-8'))
        id_hash.update(self.config.type.name.encode('utf-8'))
        if scope:
            id_hash.update(scope.encode('utf-8'))
        if collection:
            id_hash.update(collection.encode('utf-8'))

        r_uuid = id_hash.hexdigest()
        checkpoint = base64.b64encode(bytes.fromhex(r_uuid)).decode()
        return f"cp-{checkpoint}"

    def start(self):
        message_body = None
        if len(self.collection_list) > 0:
            self.get_checkpoint_props.update({"Profile": "getCollections"})
            self.checkpoint_collections_body.update({"checkpoint_ids": self.hash_list})
            self.checkpoint_collections_body.update({"collections": self.collection_list})
            message_body = self.checkpoint_collections_body
        else:
            self.get_checkpoint_props.update({"client": self.client})
        try:
            self.blip.send_message(0, self.get_checkpoint_props, body_json=message_body)
            checkpoint_message = self.blip.receive_message()
            checkpoint = json.loads(checkpoint_message.body.decode('utf-8'))
            if type(checkpoint) == dict:
                self.set_checkpoint_body.update({"time": checkpoint['time']})
                self.set_checkpoint_body.update({"remote": checkpoint['remote']})
                self.set_checkpoint_props.update({"rev": checkpoint_message.properties.get("rev", "")})
            else:
                self.collection_rev_list = checkpoint
                logger.debug(self.collection_rev_list)
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

    def get_collections(self):
        try:
            self.checkpoint_collections_body.update({"checkpoint_ids": self.hash_list})
            self.checkpoint_collections_body.update({"collections": self.collection_list})
            self.blip.send_message(0, self.get_checkpoint_collections_props, body_json=self.checkpoint_collections_body)
            checkpoint_message = self.blip.receive_message()
            checkpoint = json.loads(checkpoint_message.body.decode('utf-8'))
            self.set_checkpoint_body_list = checkpoint
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
        for n, collection in enumerate(self.collections):
            history_body = []
            sequences = []
            attachments = []
            try:
                logger.info(f"Replicating collection {collection}")
                if collection != "_default":
                    self.sub_changes_props["collection"] = n
                sub_changes_message = self.blip.send_message(0, self.sub_changes_props)
                reply_message = self.blip.receive_message()
                doc_list = self.blip.receive_message()
                doc_count = json.loads(doc_list.body_as_string())
                if not doc_count:
                    continue
                for i in range(len(doc_count)):
                    history_body.append([])
                max_history_msg = self.blip.send_message(1, self.max_history_props, reply=doc_list.number, body_json=history_body)
                changes_reply_msg = self.blip.receive_message()
                received_doc_count = 0
                for _ in range(len(doc_count)):
                    reply_message = self.blip.receive_message()
                    sequences.append(reply_message.properties['sequence'])
                    doc_id = reply_message.properties['id']
                    document = reply_message.body.decode('utf-8')
                    try:
                        document = json.loads(document)
                        if document.get("_attachments"):
                            attachment = {"docID": doc_id}
                            for item in document.get("_attachments"):
                                attachment.update(document.get("_attachments", {}).get(item))
                            attachments.append(attachment)
                    except json.decoder.JSONDecodeError:
                        pass
                    self.config.datastore.write(doc_id, document, collection=collection)
                    received_doc_count += 1
                logger.info(f"Replicated {received_doc_count} documents")
                logger.debug(f"Max sequence {max(sequences)}")
                if self.config.checkpoint:
                    logger.info(f"Setting checkpoint for sequence {max(sequences)}")
                    self.set_checkpoint_body.update({"remote": max(sequences)})
                    if collection != "_default":
                        self.set_checkpoint_props["collection"] = n
                        self.set_checkpoint_props["client"] = self.checkpoint_collections_body["checkpoint_ids"][n]
                        self.set_checkpoint_props["rev"] = self.collection_rev_list[n].get("_rev", "")
                    set_checkpoint = self.blip.send_message(0, self.set_checkpoint_props, body_json=self.set_checkpoint_body)
                    reply_message = self.blip.receive_message()
                    self.set_checkpoint_props.update({"rev": reply_message.properties.get("rev", "")})
                for attachment in attachments:
                    self.get_attachment(attachment, n, collection)
            except BLIPError as err:
                self.stop()
                raise ReplicationError(f"Replication protocol error: {err}")
            except ClientError as err:
                if err.error_code == 401:
                    raise ReplicationError("Unauthorized: invalid credentials provided.")
                else:
                    self.stop()
                    raise ReplicationError(f"Websocket error: {err}")
            except Exception as err:
                self.stop()
                raise ReplicationError(f"General error: {err}")

    def get_attachment(self, attachment: dict, number: int, collection: str):
        data = bytearray()
        logger.info(f"Getting attachment for {attachment['docID']} length {attachment['length']} collection {collection} #{number}")
        self.get_attachment_props["digest"] = attachment["digest"]
        self.get_attachment_props["docID"] = attachment["docID"]
        if collection != "_default":
            self.get_attachment_props["collection"] = number
        try:
            self.blip.send_message(0, self.get_attachment_props)
            reply_message = self.blip.receive_message()
            data = reply_message.body_as_bytes()
            self.config.datastore.write_attachment(attachment['docID'], attachment['content_type'], data, collection=collection)
        except Exception as err:
            import traceback
            tb = traceback.format_exc()
            print(tb)
            self.stop()
            raise ReplicationError(f"Get attachment error: {err}")
        logger.debug(f"Received {len(data)} bytes")

    def stop(self):
        self.blip.stop()

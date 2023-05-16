#!/usr/bin/env python3

import os
import sys
import argparse
import time
import logging
import warnings
import json
from random import randbytes
import base64
from hashlib import sha1

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from pyblip.headers import SessionAuth
from pyblip.protocol import BLIPProtocol
from pyblip.replicator import Replicator, ReplicatorConfiguration, ReplicatorType
from pyblip.exceptions import BLIPError, NotAuthorized, HTTPNotImplemented, InternalServerError, ClientError
from pyblip.output import LocalDB, LocalFile, ScreenOutput

warnings.filterwarnings("ignore")
logger = logging.getLogger()


class Params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true', help="Use SSL")
        parser.add_argument('--host', action='store', help="Hostname or IP address", default="127.0.0.1")
        parser.add_argument('--user', action='store', help="User Name", default="Administrator")
        parser.add_argument('--password', action='store', help="User Password", default="password")
        parser.add_argument('--bucket', action='store', help="Test Bucket", default="testrun")
        parser.add_argument('--start', action='store_true', help="Start Container")
        parser.add_argument('--stop', action='store_true', help="Stop Container")
        parser.add_argument("--external", action="store_true")
        parser.add_argument('--database', action='store', help="Test Database", default="testrun")
        parser.add_argument('--session', action='store', help="Session ID")
        parser.add_argument("--screen", action="store_true")
        parser.add_argument("--file", action="store_true")
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


def manual_2():
    connect_string = f"ws://{options.host}:4984"
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    if options.screen:
        output = ScreenOutput()
    elif options.file:
        output = LocalFile(os.environ['HOME'])
    else:
        output = LocalDB(os.environ['HOME'])

    replicator = Replicator(ReplicatorConfiguration.create(
        options.database,
        connect_string,
        ReplicatorType.PULL,
        SessionAuth(options.session),
        output
    ))

    try:
        replicator.start()
        replicator.replicate()
        replicator.stop()
    except Exception as err:
        print(f"Error: {err}")


def manual_1():
    uri = f"ws://{options.host}:4984/{options.database}/_blipsync"
    header = SessionAuth(options.session).header()
    get_checkpoint_props = {
        "Profile": "getCheckpoint",
        "client": "testClient"
    }

    set_checkpoint_props = {
        "Profile": "setCheckpoint",
        "client": "testClient",
        "rev": ""
    }

    set_checkpoint_body = {
        "time": int(time.time()),
        "remote": None
    }

    # json.dumps(set_checkpoint_body, separators=(',', ':'))

    sub_changes = {
        "Profile": "subChanges",
        "versioning": "rev-trees",
        "activeOnly": True
    }

    max_history = {
        "maxHistory": 20,
        "blobs": True,
        "deltas": True
    }

    # history_body = [[],[],[],[],[],[],[],[],[],[]]
    history_body = []

    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    try:
        blip = BLIPProtocol(uri, header)
    except NotAuthorized:
        print("Invalid credentials")
        sys.exit(1)
    except (HTTPNotImplemented, InternalServerError):
        raise

    uuid = sha1(randbytes(20)).hexdigest()
    checkpoint = base64.b64encode(bytes.fromhex(uuid)).decode()
    # frame = blip.get_checkpoint(f"cp-{checkpoint}")
    try:
        blip.send_message(0, get_checkpoint_props)
        message = blip.receive_message()
    except BLIPError as err:
        if err.error_code:
            if err.error_code == 404:
                print("Not found.")
    except ClientError as err:
        if err.error_code == 401:
            print("Unauthorized: invalid credentials provided.")
            sys.exit(0)
        else:
            raise
    except Exception as err:
        logger.error(f"Error: {err}")

    try:
        sub_changes_message = blip.send_message(0, sub_changes)
        reply_message = blip.receive_message()
        doc_list = blip.receive_message()
        doc_count = json.loads(doc_list.body_as_string())
        for i in range(len(doc_count)):
            history_body.append([])
        max_history_msg = blip.send_message(1, max_history, reply=doc_list.number, body_json=history_body)
        changes_reply_msg = blip.receive_message()
        received_doc_count = 0
        while True:
            try:
                reply_message = blip.receive_message()
                received_doc_count += 1
            except ClientError as err:
                if err.error_code == 408:
                    break
                else:
                    raise
        print(len(doc_count))
        print(received_doc_count)
    except BLIPError as err:
        if err.error_code:
            if err.error_code == 404:
                print("Not found.")
    except ClientError as err:
        if err.error_code == 401:
            print("Unauthorized: invalid credentials provided.")
            sys.exit(0)
        else:
            raise
    except Exception as err:
        logger.error(f"Error: {err}")

    time.sleep(5)
    blip.stop()


p = Params()
options = p.parameters

# manual_1()
manual_2()

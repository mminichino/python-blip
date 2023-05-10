#!/usr/bin/env python3

import os
import sys
import argparse
import time
import logging
import warnings
from random import randbytes
import base64
from hashlib import sha1

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from pyblip.headers import SessionAuth
from pyblip.protocol import BLIPProtocol

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
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


def manual_1():
    uri = f"ws://{options.host}:4984/{options.database}/_blipsync"
    header = SessionAuth(options.session).header()
    properties = {
        "Profile": "setCheckpoint",
        "client": "testClient"
    }

    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    blip = BLIPProtocol(uri, header)

    uuid = sha1(randbytes(20)).hexdigest()
    checkpoint = base64.b64encode(bytes.fromhex(uuid)).decode()
    # frame = blip.get_checkpoint(f"cp-{checkpoint}")
    try:
        blip.send_message(0, properties)
        message = blip.receive_message()
    except Exception as err:
        logger.error(f"Error: {err}")
    time.sleep(5)
    blip.stop()


p = Params()
options = p.parameters

manual_1()

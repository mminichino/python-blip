#!/usr/bin/env python3

import os
import sys
import argparse
from random import randbytes
import base64

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from pyblip.client import BlipClient
from pyblip.headers import BasicAuth, SessionAuth
from pyblip.protocol import BLIPProtocol


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

    client = BlipClient(uri, header)
    blip = BLIPProtocol()

    checkpoint = base64.b64encode(randbytes(20)).decode('utf-8')
    frame = blip.get_checkpoint(f"cp-{checkpoint}")
    client.send_message(frame)
    result = client.get_message()
    print(result)


p = Params()
options = p.parameters

manual_1()

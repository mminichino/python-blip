#!/usr/bin/env python3

import os
import sys
import argparse
import logging
import warnings

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from pythonblip.headers import SessionAuth
from pythonblip.replicator import Replicator, ReplicatorConfiguration, ReplicatorType
from pythonblip.output import LocalDB, LocalFile, ScreenOutput
from conftest import pytest_sessionstart, pytest_sessionfinish

warnings.filterwarnings("ignore")
logger = logging.getLogger()


class Params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true', help="Use SSL")
        parser.add_argument('--host', action='store', help="Hostname or IP address", default="127.0.0.1")
        parser.add_argument('--port', action='store', help="Port number", default="4984")
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
        parser.add_argument("--dir", action="store", help="Output Directory")
        parser.add_argument("--scope", action="store", help="Scope")
        parser.add_argument("--collections", action="store", help="Collections")
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


def container_start():
    pytest_sessionstart(None)


def container_stop():
    pytest_sessionfinish(None, 0)


def manual_1():
    directory = options.dir if options.dir else os.environ['HOME']
    scope = options.scope if options.scope else "_default"
    collections = options.collections.split(',') if options.collections else ["_default"]
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    if options.screen:
        output = ScreenOutput()
    elif options.file:
        output = LocalFile(directory)
    else:
        output = LocalDB(directory)

    replicator = Replicator(ReplicatorConfiguration.create(
        options.database,
        options.host,
        ReplicatorType.PULL,
        SessionAuth(options.session),
        options.ssl,
        options.port,
        scope,
        collections,
        output
    ))

    try:
        replicator.start()
        replicator.replicate()
        replicator.stop()
    except Exception as err:
        print(f"Error: {err}")


p = Params()
options = p.parameters

try:
    debug_level = int(os.environ['PY_BLIP_DEBUG_LEVEL'])
except (ValueError, KeyError):
    debug_level = 3

if debug_level == 0:
    logger.setLevel(logging.DEBUG)
elif debug_level == 1:
    logger.setLevel(logging.ERROR)
elif debug_level == 2:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.CRITICAL)

logging.basicConfig()

if options.start:
    container_start()
    sys.exit(0)

if options.stop:
    container_stop()
    sys.exit(0)

manual_1()

#!/usr/bin/env python3

import os
import subprocess
import sys
import pytest
from common import start_container, stop_container

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pkg_dir = parent + '/pythonblip'
sys.path.append(parent)
sys.path.append(pkg_dir)
sys.path.append(current)


def cli_run(cmd: str, *args: str):
    command_output = ""
    run_cmd = [
        cmd,
        *args
    ]

    p = subprocess.Popen(run_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    while True:
        line = p.stdout.readline()
        if not line:
            break
        line_string = line.decode("utf-8")
        command_output += line_string

    p.communicate()

    return p.returncode, command_output


def line_count(filename: str) -> int:
    with open(filename, 'r') as file:
        lines = len(file.readlines())
        return lines


@pytest.fixture(autouse=True, scope='module')
def test_start_stop():
    start_container(ssl=True)
    yield
    stop_container()


def test_ssl_1(hostname):
    global parent
    os.environ['PYTHONPATH'] = parent
    cmd = parent + '/bin/blipctl'
    out_dir = current + '/output'
    adjuster_file = out_dir + '/adjuster.jsonl'
    claims_file = out_dir + '/claims.jsonl'
    company_file = out_dir + '/company.jsonl'
    customer_file = out_dir + '/customer.jsonl'
    picture_file = out_dir + '/picture.jsonl'
    args = ['-n', hostname, '-d', 'insurance', '-t', pytest.insurance_session_id, '-s', 'data', '-c', 'adjuster,claims,company,customer,picture', '-f', '-D', out_dir, '--ssl']

    result, output = cli_run(cmd, *args)
    print(output)
    assert result == 0
    assert os.path.isfile(adjuster_file) is True
    assert os.path.isfile(claims_file) is True
    assert os.path.isfile(company_file) is True
    assert os.path.isfile(customer_file) is True
    assert os.path.isfile(picture_file) is True
    assert line_count(adjuster_file) == 10
    assert line_count(claims_file) == 10
    assert line_count(company_file) == 0
    assert line_count(customer_file) == 10
    assert line_count(picture_file) == 0


def test_ssl_2(hostname):
    global parent
    os.environ['PYTHONPATH'] = parent
    cmd = parent + '/bin/blipctl'
    out_dir = current + '/output'
    adjuster_file = out_dir + '/adjuster.jsonl'
    args = ['-n', hostname, '-d', 'adjuster', '-t', pytest.adjuster_session_id, '-f', '-D', out_dir, '--ssl']

    result, output = cli_run(cmd, *args)
    print(output)
    assert result == 0
    assert os.path.isfile(adjuster_file) is True
    assert line_count(adjuster_file) == 30

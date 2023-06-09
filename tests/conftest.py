import json

import pytest
import docker
from docker.models.containers import Container
import tarfile
import io
import os
import warnings

warnings.filterwarnings("ignore")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)


def copy_to_container(container_id: Container, src: str, dst: str):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w|') as tar, open(src, 'rb') as file:
        info = tar.gettarinfo(fileobj=file)
        info.name = os.path.basename(src)
        tar.addfile(info, file)

    container_id.put_archive(dst, stream.getvalue())


def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="localhost")
    parser.addoption("--bucket", action="store", default="test")
    parser.addoption("--external", action="store_true")


@pytest.fixture
def hostname(request):
    return request.config.getoption("--host")


@pytest.fixture
def bucket(request):
    return request.config.getoption("--bucket")


def pytest_configure(config):
    pytest.adjuster_session_id = None
    pytest.insurance_session_id = None


def pytest_sessionstart(session):
    if session:
        external = session.config.getoption('--external')
        if external:
            return
    print("Starting test container")
    client = docker.from_env()
    container_id = client.containers.run('mminichino/cbdev:latest',
                                         detach=True,
                                         name="pytest",
                                         ports={
                                                8091: 8091,
                                                18091: 18091,
                                                8092: 8092,
                                                18092: 18092,
                                                8093: 8093,
                                                18093: 18093,
                                                8094: 8094,
                                                18094: 18094,
                                                8095: 8095,
                                                18095: 18095,
                                                8096: 8096,
                                                18096: 18096,
                                                8097: 8097,
                                                18097: 18097,
                                                11207: 11207,
                                                11210: 11210,
                                                9102: 9102,
                                                4984: 4984,
                                                4985: 4985,
                                         },
                                         )

    print("Container started")
    print("Waiting for container startup")

    while True:
        exit_code, output = container_id.exec_run(['/bin/bash',
                                                   '-c',
                                                   'test -f /demo/couchbase/.ready'])
        if exit_code == 0:
            break

    print("Waiting for Couchbase Server to be ready")
    exit_code, output = container_id.exec_run(['/demo/couchbase/cbperf/bin/cb_perf',
                                               'list',
                                               '--host', '127.0.0.1',
                                               '--wait'])
    for line in output.split(b'\n'):
        print(line.decode("utf-8"))
    assert exit_code == 0

    copy_to_container(container_id, parent + "/tests/insurance.js", "/etc/sync_gateway")
    copy_to_container(container_id, parent + "/tests/adjuster_demo.js", "/etc/sync_gateway")

    print("Creating test buckets and loading data")

    cmd_list = [
        "/demo/couchbase/cbperf/bin/cb_perf load --host 127.0.0.1 --schema insurance_sample --replica 0 --safe --quota 128",
        "/demo/couchbase/cbperf/bin/cb_perf load --host 127.0.0.1 --count 30 --schema adjuster_demo --replica 0 --safe --quota 128",
        "/demo/couchbase/sgwcli/sgwcli database create -h 127.0.0.1 -b insurance_sample -k insurance_sample.data -n insurance",
        "/demo/couchbase/sgwcli/sgwcli database create -h 127.0.0.1 -b adjuster_demo -n adjuster",
        "/demo/couchbase/sgwcli/sgwcli database wait -h 127.0.0.1 -n insurance",
        "/demo/couchbase/sgwcli/sgwcli database wait -h 127.0.0.1 -n adjuster",
        "/demo/couchbase/sgwcli/sgwcli user map -h 127.0.0.1 -d 127.0.0.1 -f region -k insurance_sample -n insurance",
        "/demo/couchbase/sgwcli/sgwcli user map -h 127.0.0.1 -d 127.0.0.1 -f region -k adjuster_demo -n adjuster",
        "/demo/couchbase/sgwcli/sgwcli database sync -h 127.0.0.1 -n insurance -f /etc/sync_gateway/insurance.js",
        "/demo/couchbase/sgwcli/sgwcli database sync -h 127.0.0.1 -n adjuster -f /etc/sync_gateway/adjuster_demo.js"
    ]

    for cmd in cmd_list:
        exit_code, output = container_id.exec_run(cmd.split())
        for line in output.split(b'\n'):
            print(line.decode("utf-8"))
        assert exit_code == 0

    exit_code, output = container_id.exec_run(['/demo/couchbase/sgwcli/sgwcli',
                                               'auth',
                                               'session',
                                               '-h', '127.0.0.1',
                                               '-n', 'adjuster',
                                               '-U', 'region@central'])
    json_data = json.loads(bytes(output).decode('utf-8'))
    assert exit_code == 0
    print(json_data['session_id'])

    exit_code, output = container_id.exec_run(['/demo/couchbase/sgwcli/sgwcli',
                                               'auth',
                                               'session',
                                               '-h', '127.0.0.1',
                                               '-n', 'insurance',
                                               '-U', 'region@central'])
    json_data = json.loads(bytes(output).decode('utf-8'))
    assert exit_code == 0
    print(json_data['session_id'])

    print("Ready.")


def pytest_sessionfinish(session, exitstatus):
    if session:
        external = session.config.getoption('--external')
        if external:
            return
    print("")
    print("Stopping container")
    client = docker.from_env()
    container_id = client.containers.get('pytest')
    container_id.stop()
    print("Removing test container")
    container_id.remove()
    print("Done.")


def pytest_unconfigure(config):
    pass

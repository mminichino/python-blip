##

import pytest
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


def pytest_configure():
    pytest.adjuster_session_id = None
    pytest.insurance_session_id = None


def pytest_sessionstart():
    pass


def pytest_sessionfinish():
    pass


def pytest_unconfigure():
    pass

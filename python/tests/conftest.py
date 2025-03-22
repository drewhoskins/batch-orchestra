import asyncio
import multiprocessing
import sys
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

from batch_orchestra.batch_worker import BatchWorkerClient

#
# This file was copied from https://github.com/temporalio/samples-python/blob/main/tests/conftest.py
# Check there for updates.
#

# Due to https://github.com/python/cpython/issues/77906, multiprocessing on
# macOS starting with Python 3.8 has changed from "fork" to "spawn". For
# pre-3.8, we are changing it for them.
if sys.version_info < (3, 8) and sys.platform.startswith("darwin"):
    multiprocessing.set_start_method("spawn", True)


def pytest_addoption(parser):
    parser.addoption(
        "--workflow-environment",
        default="localhost:7233",
        help="Where to execute the workflows in tests: 'local', 'time-skipping', or target a local server (e.g. 'localhost:7233')",
    )


@pytest.fixture(scope="session")
def event_loop():
    # See https://github.com/pytest-dev/pytest-asyncio/issues/68
    # See https://github.com/pytest-dev/pytest-asyncio/issues/257
    # Also need ProactorEventLoop on older versions of Python with Windows so
    # that asyncio subprocess works properly
    if sys.version_info < (3, 8) and sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def env(request) -> AsyncGenerator[WorkflowEnvironment, None]:
    env_type = request.config.getoption("--workflow-environment")
    if env_type == "local":
        # TODO
        raise NotImplementedError("Something's not working yet in this mode.")
    #        env = await WorkflowEnvironment.start_local()
    elif env_type == "time-skipping":
        raise NotImplementedError("Time-skipping mode is untested.")
    #        env = await WorkflowEnvironment.start_time_skipping()
    else:
        try:
            client = await Client.connect(env_type)
            client = BatchWorkerClient.register(client)
        except RuntimeError as e:
            message = f"Could not connect to temporal-server at {env_type}.  Check the README.md Python Quick Start if you need guidance."
            raise RuntimeError(message) from e
        env = WorkflowEnvironment.from_client(client)
    yield env
    await env.shutdown()


@pytest_asyncio.fixture
async def client(env: WorkflowEnvironment) -> Client:
    return env.client

# Python

## Quick Start

First, set up [Temporal prerequisites](../README.md#quick-start)

You'll also need python (I've tested with 3.10) and [poetry](https://python-poetry.org/).

If you don't already have poetry:

    python -m pip install poetry
    
or

    pipx install poetry

Clone temporal-batch and pull the latest source.

Then, run the following from the python directory:

    poetry install

That loads all required dependencies. 

Make sure temporal-server is running on localhost:7233 (the default)
Then use pytest to run the tests:

    poetry run pytest tests/

You may also run the sample.  See its [README](./sample/README.md)

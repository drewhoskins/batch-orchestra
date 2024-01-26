# Python Development Usage

Prerequisites:

* Python >= 3.8
* [Poetry](https://python-poetry.org)
* [Local Temporal server running](https://docs.temporal.io/application-development/foundations#run-a-development-cluster)

If you don't already have poetry:

    python -m pip install poetry
or
    pipx install poetry

Then, with the temporal-batch repository cloned, run the following from the python directory:

    poetry install

That loads all required dependencies. Then use pytest to run a test:

    poetry run pytest tests/process_page_test.py

Some tests require a local temporal server running.

    TODO

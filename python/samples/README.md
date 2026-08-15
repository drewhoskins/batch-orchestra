# Usage

To run a python sample, having already gone through the [quick start guide](../README.md)

## Instructions if you use poetry

Start some sample workers to process in parallel with:

    poetry run python samples/run_workers.py

Start a workflow to run on those workers, for example with

    poetry run python samples/perform_sql_batch_migration.py

An example of visualising the DB migration project using a Streamlit app, created for the Temporal Replay Hackathon 2025 can be run using:

    poetry install --with samples
    poetry run streamlit run samples/progress_viewer.py

## Instructions if you use pip

Create an env as in [the Python README](../README.md) and activate it in two terminals.

In one terminal, start some sample workers to process in parallel with:

    python samples/run_workers.py

In the other, a workflow to run on those workers, for example with

    python samples/perform_sql_batch_migration.py

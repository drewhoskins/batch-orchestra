# Usage

To run a python sample, having already gone through the [quick start guide](../README.md)

[Set up a temporal server (at localhost:7233)](https://docs.temporal.io/application-development/foundations#run-a-development-cluster) .

Start some sample workers to process in parallel with:

    poetry run python samples/run_workers.py

Start a workflow to run on those workers, for example with

    poetry run python samples/perform_sql_batch_migration.py

# Usage

To run a python sample, having already gone through the [quick start guide](../README.md)

1. * [Set up a temporal server (at localhost:7233)](https://docs.temporal.io/application-development/foundations#run-a-development-cluster) .

2. Start some sample workers to process in parallel with:

    poetry run python sample/run_workers.py

3. Start a workflow to run on those workers, for example with

    poetry run python sample/perform_sql_batch_migration.py

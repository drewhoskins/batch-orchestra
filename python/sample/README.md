# Usage

To run a sample,

1. * [Set up a temporal server (at localhost:7233)](https://docs.temporal.io/application-development/foundations#run-a-development-cluster) .

2. Start a sample worker with:

    poetry run python sample/run_worker.py

3. Start a workflow to run on that worker, for example with

    poetry run python sample/start_batch_executor.py

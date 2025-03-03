# Python Quick Start

First, set up [Temporal prerequisites](../README.md#quick-start)

You'll also need python (I've tested with 3.10) and [poetry](https://python-poetry.org/).

If you don't already have poetry:

    python -m pip install poetry
    
or

    pipx install poetry

Clone batch-orchestra and pull the latest source.

Then, run the following from the python directory:

    poetry install

That loads all required dependencies. 

Make sure temporal-server is running on localhost:7233 (the default)

[Set up a temporal server (at localhost:7233)](https://docs.temporal.io/application-development/foundations#run-a-development-cluster) .

Then use pytest to run the tests:

    poetry run pytest tests/

You may also run the samples, which are currently the best place to get an idea of how to implement.  See the [samples README](./samples/README.md)

# Page processors

The main thing you implement is a page processor annotated with `@page_processor`.
See `InflateProductPrices` in the sample for an example of a page processor.

# Setting up your Temporal Worker

Your Worker that processes batches will be a standard Temporal Worker with one tweak.  

You need to do three things

* Register the workflow `BatchOrchestrator`
* Register the activity, `process_page`, a generic framework function that will call functions you've annotated with @page_processor.
* Create a BatchWorkerClient, using your Worker's temporal client, like so:

    temporal_client = BatchWorkerClient.register(temporal_client)


See [here](./samples/run_workers.py) for an example.

# Custom trackers

If you want, you can track your batch's progress without doing any tracking on your client.
The batch orchestrator will periodically call your tracker so it can report what's been going on so far.
Create a function annotated with `@batch_tracker`.  In that function, you can do whatever you want, such as writing the progress to a database.

    @batch_tracker
    async def my_tracker(context: BatchTrackerContext):
        record_my_progress(context.progress)

# Contributor's guide

## VSCode developers

To set up development in VSCode, you'll want to make poetry's python your interpreter so it can find all the modules.
To put the interpreter path on your clipboard, run

    poetry env info --path | pbcopy

Then, in VSCode run Cmd/Ctrl-Shift-P, choose `Python: Select Interpreter`, `Select at Workspace Level`, and `Enter interpreter path...`.
Copy your payload in.

## Reviewer's guide

Some things to keep an eye on/ that I want to tackle before the initial release 

* Should pass cursors as raw rather than str?
* Harmonize with python-sdk and python best practices.  (What did I miss?)


# Roadmap
* Error handling
  * Call a handler when there are initial failures, to be used for notifications and such.
  * Allow users to designate individual records as failures and proceed with the rest of their page.

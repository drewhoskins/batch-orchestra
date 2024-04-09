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
* Huge batches that span multiple workflow histories.  Likely will use child workflows or continue-as-new.
  * Log batch_id
* A signal that allows changing max_parallelism.  This will allow pausing and controlled rampup.

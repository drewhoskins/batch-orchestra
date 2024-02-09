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

## Reviewer's guide

Some things to keep an eye on/ that I want to tackle before the initial release 

* Naming of the library.  Is temporal-batch acceptable and advisable?
* Should pass cursors as raw rather than str?
* Is BatchOrchestratorEncodingPayloadConverter using modern best practices, and will it interfere with non-batch workflows running on the same workflow?
* Test on other machines
* Polish and document.
* Harmonize with python-sdk and python best practices.  (What did I miss?)
* Allow scripts and tests to be run from more directories (it's finicky right now)

## Roadmap
* Error handling
** Allow users to author an activity to intercept failures, perhaps to send an notification. 
** Allow users to designate individual records as failures and proceed with the rest of their page.
* Huge batches that span multiple workflow histories.  Likely will use child workflows.
* Log batch_id
* A signal that allows changing max_parallelism.  This will allow pausing and controlled rampup.

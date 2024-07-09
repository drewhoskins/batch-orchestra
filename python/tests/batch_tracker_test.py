from __future__ import annotations
import sys

try:
    from dataclasses import asdict
    from unittest.mock import patch
    from datetime import datetime, timedelta

    import pytest

    from temporalio.client import WorkflowHandle, Client
    from temporalio.testing import ActivityEnvironment

    from batch_orchestrator_io import BatchOrchestratorProgress
    from batch_tracker import batch_tracker, track_batch_progress, BatchTrackerContext, BatchTrackerKeepPolling
    from batch_worker import BatchWorkerClient

except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print("But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry.")
    print(f"Original error: {e}")
    sys.exit(1)

async def init_client():
    client = await Client.connect("localhost:7233")
    client = BatchWorkerClient.register(client)
    return client


def notify_tired_engineer(batch_id: str, num_stuck_pages: int):
    pass

@batch_tracker
async def polls_for_stuck_pages(context: BatchTrackerContext):
    if context.progress.num_stuck_pages > 0:
        notify_tired_engineer(context.batch_id, context.progress.num_stuck_pages)


# Trackers run periodically and allow the user to take action on the batch's progress, such as polling
# for stuck pages and notifying an engineer, or checking for elapsed time.
@pytest.mark.asyncio
async def test_stuck_page_tracker():
    with patch('batch_tracker_test.notify_tired_engineer') as notify_mock, patch.object(WorkflowHandle, 'query') as query_mock:
        env = ActivityEnvironment()
        current_progress = BatchOrchestratorProgress(
            num_completed_pages=0, 
            num_stuck_pages=1, 
            num_processing_pages=1, 
            max_parallelism_achieved=1, 
            num_failed_pages=0, 
            is_finished=False,
            _start_timestamp=datetime.now().timestamp())
        query_mock.return_value = asdict(current_progress)
        await init_client()
        try:
            await env.run(track_batch_progress, polls_for_stuck_pages.__name__, 'my_batch_id', None)
        except BatchTrackerKeepPolling:
            notify_mock.assert_called_once_with('my_batch_id', 1)
        else:
            assert False, "Expected BatchTrackerKeepPolling to be raised"


def notify_that_batch_is_past_slo(batch_id: str):
    pass

@batch_tracker
async def tracks_batch_taking_too_long(context: BatchTrackerContext):
    now = datetime.now()
    if now - context.progress.start_time() > timedelta(hours=1):
        notify_that_batch_is_past_slo(context.batch_id)

@pytest.mark.asyncio
async def test_elapsed_time_tracker():
    with patch('batch_tracker_test.notify_that_batch_is_past_slo') as notify_mock, patch.object(WorkflowHandle, 'query') as query_mock:
        env = ActivityEnvironment()

        # first, we'll test that the tracker doesn't raise an exception if the batch is still within the SLO
        then = datetime.now()
        current_status = BatchOrchestratorProgress(
            num_completed_pages=5, 
            num_stuck_pages=0,
            num_processing_pages=1,
            num_failed_pages=0, 
            max_parallelism_achieved=3, 
            is_finished=False,
            _start_timestamp=then.timestamp())
        query_mock.return_value = asdict(current_status)
        await init_client()
        try:
            await env.run(track_batch_progress, tracks_batch_taking_too_long.__name__, 'my_batch_id', None)
        except BatchTrackerKeepPolling:
            notify_mock.assert_not_called()
        else:
            assert False, "Expected BatchTrackerKeepPolling to be raised"

        # But now pretend the batch has taken two hours.  The tracker should notify someone.
        then = datetime.now() - timedelta(hours=2)
        current_status = BatchOrchestratorProgress(
            num_completed_pages=5, 
            max_parallelism_achieved=3, 
            num_processing_pages=1,
            num_failed_pages=0,
            num_stuck_pages=0,
            is_finished=False,
            _start_timestamp=then.timestamp())
        query_mock.return_value = asdict(current_status)
        await init_client()
        try:
            await env.run(track_batch_progress, tracks_batch_taking_too_long.__name__, 'my_batch_id', None)
        except BatchTrackerKeepPolling:
            notify_mock.assert_called_once_with('my_batch_id')
        else:
            assert False, "Expected BatchTrackerKeepPolling to be raised"

@pytest.mark.asyncio
async def test_uninitialized_client():    
    env = ActivityEnvironment()
    BatchWorkerClient.get_instance()._clear_temporal_client()
    try:
        await env.run(track_batch_progress, polls_for_stuck_pages.__name__, 'my_batch_id', None)
    except ValueError as e:
        assert str(e) == "Missing a temporal client for use by your @page_processor or @batch_tracker. " + \
            "Make sure to call BatchWorkerClient.register(client)."


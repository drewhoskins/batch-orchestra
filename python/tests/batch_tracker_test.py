from __future__ import annotations
import sys

try:
    from dataclasses import dataclass, asdict
    from typing import Any, Sequence
    from unittest.mock import patch
    from datetime import datetime, timedelta


    import pytest

    from temporalio.client import WorkflowHandle, Client
    from temporalio.testing import ActivityEnvironment
    from temporalio.activity import info
    import temporalio.common
    import temporalio.exceptions

    from batch_orchestrator_io import BatchOrchestratorProgress
    from batch_tracker import batch_tracker, track_batch_progress, BatchTrackerContext, BatchTrackerKeepPolling
    from batch_processor import temporal_client_factory

except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print(f"Original error: {e}")
    sys.exit(1)


@temporal_client_factory
async def make_temporal_client():
    return await Client.connect("localhost:7233")

def notify_tired_engineer(batch_id: str, num_stuck_pages: int):
    pass

@batch_tracker
async def polls_for_stuck_pages(context: BatchTrackerContext):
    if context.progress.num_stuck_pages > 0:
        notify_tired_engineer(context.batch_id, context.progress.num_stuck_pages)

@pytest.mark.asyncio
async def test_invalid_temporal_client_factory():
    env = ActivityEnvironment()
    try:
        await env.run(track_batch_progress, 'not a callable', polls_for_stuck_pages.__name__, 'my_batch_id', None)
    except ValueError as e:
        assert str(e) == f"You passed temporal_client_factory_name 'not a callable' into the BatchOrchestrator, but it was not registered on " + \
            f"your worker. Please annotate it with @temporal_client_factory and make sure its module is imported. " + \
            f"Available callables: ['make_temporal_client']"
    else:
        assert False, "Should have thrown an error."


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
        got_polling_exception = False
        try:
            await env.run(track_batch_progress, make_temporal_client.__name__, polls_for_stuck_pages.__name__, 'my_batch_id', None)
        except BatchTrackerKeepPolling as e:
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
        try:
            await env.run(track_batch_progress, make_temporal_client.__name__, tracks_batch_taking_too_long.__name__, 'my_batch_id', None)
        except BatchTrackerKeepPolling as e:
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
        try:
            await env.run(track_batch_progress, make_temporal_client.__name__, tracks_batch_taking_too_long.__name__, 'my_batch_id', None)
        except BatchTrackerKeepPolling as e:
            notify_mock.assert_called_once_with('my_batch_id')
        else:
            assert False, "Expected BatchTrackerKeepPolling to be raised"

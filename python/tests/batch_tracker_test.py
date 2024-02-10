from __future__ import annotations
import sys

try:
    from dataclasses import dataclass
    from typing import Any, Sequence
    from unittest.mock import patch
    from datetime import datetime, timedelta


    import pytest

    from temporalio.client import WorkflowHandle
    from temporalio.testing import ActivityEnvironment
    from temporalio.activity import info
    import temporalio.common
    import temporalio.exceptions

    from batch_orchestrator_data import BatchOrchestratorResults
    from batch_tracker import batch_tracker, track_batch_progress, BatchTrackerContext, BatchTrackerKeepPolling
except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run python ./tests/batch_orchestrator_test.py`.")
    print(f"Original error: {e}")
    sys.exit(1)

def notify_tired_engineer(batch_id: str, num_stuck_pages: int):
    pass

@batch_tracker
async def polls_for_stuck_pages(context: BatchTrackerContext):
    if context.current_status.num_failed_pages > 0:
        notify_tired_engineer(context.batch_id, context.current_status.num_failed_pages)

# Trackers run periodically and allow the user to take action on the batch's progress, such as polling
# for stuck pages and notifying an engineer, or checking for elapsed time.
@pytest.mark.asyncio
async def test_stuck_page_tracker():
    with patch('batch_tracker_test.notify_tired_engineer') as notify_mock:
        env = ActivityEnvironment()
        current_status = BatchOrchestratorResults(num_pages_processed=0, max_parallelism_achieved=1, num_failed_pages=1, _start_timestamp=datetime.now().timestamp())
        got_polling_exception = False
        try:
            await env.run(track_batch_progress, polls_for_stuck_pages.__name__, 'my_batch_id', None, current_status)
        except BatchTrackerKeepPolling as e:
            notify_mock.assert_called_once_with('my_batch_id', 1)
        else:
            assert False, "Expected BatchTrackerKeepPolling to be raised"

def notify_that_batch_is_past_slo(batch_id: str):
    pass

@batch_tracker
async def tracks_batch_taking_too_long(context: BatchTrackerContext):
    now = datetime.now()
    if now - context.current_status.start_time() > timedelta(hours=1):
        notify_that_batch_is_past_slo(context.batch_id)

@pytest.mark.asyncio
async def test_elapsed_time_tracker():
    with patch('batch_tracker_test.notify_that_batch_is_past_slo') as notify_mock:
        env = ActivityEnvironment()

        # first, we'll test that the tracker doesn't raise an exception if the batch is still within the SLO
        then = datetime.now()
        current_status = BatchOrchestratorResults(
            num_pages_processed=5, 
            max_parallelism_achieved=3, 
            num_failed_pages=0, 
            _start_timestamp=then.timestamp())
        try:
            await env.run(track_batch_progress, tracks_batch_taking_too_long.__name__, 'my_batch_id', None, current_status)
        except BatchTrackerKeepPolling as e:
            notify_mock.assert_not_called()
        else:
            assert False, "Expected BatchTrackerKeepPolling to be raised"

        # But now pretend the batch has taken two hours.  The tracker should notify someone.
        then = datetime.now() - timedelta(hours=2)
        current_status = BatchOrchestratorResults(
            num_pages_processed=5, 
            max_parallelism_achieved=3, 
            num_failed_pages=0,
            _start_timestamp=then.timestamp())
        try:
            await env.run(track_batch_progress, tracks_batch_taking_too_long.__name__, 'my_batch_id', None, current_status)
        except BatchTrackerKeepPolling as e:
            notify_mock.assert_called_once_with('my_batch_id')
        else:
            assert False, "Expected BatchTrackerKeepPolling to be raised"

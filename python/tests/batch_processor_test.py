from __future__ import annotations
import sys
from unittest.mock import AsyncMock
try:
    from dataclasses import dataclass
    from typing import Any, Sequence
    from unittest.mock import patch

    import pytest

    from temporalio.client import WorkflowHandle
    from temporalio.testing import ActivityEnvironment
    from temporalio.activity import info
    import temporalio.common
    import temporalio.exceptions

    import batch_orchestrator
    from batch_processor import BatchProcessorContext, BatchPage, page_processor, process_page
except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print(f"Original error: {e}")
    sys.exit(1)

@page_processor
async def returns_cursor(context: BatchProcessorContext):
    assert context.args_str == "some_args"
    return context.page.cursor_str


@pytest.mark.asyncio
async def test_page_processor():
    env = ActivityEnvironment()
    result = await env.run(process_page, returns_cursor.__name__, None, BatchPage("some_cursor", 10), 0, "some_args", False)
    assert result == "some_cursor"


@page_processor
async def starts_new_page(context: BatchProcessorContext):
    page = context.page
    next_page = BatchPage(page.cursor_str + "_the_second", page.size)
    await context.enqueue_next_page(next_page)

async def on_signal(
        parent_workflow,
        signal: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any]):
    assert signal == "_signal_add_page"
    page = args[0]
    page_num = args[1]
    assert page.cursor_str == "some_cursor_the_second"
    assert page_num == 1 # Signals for the next page after page 0

@pytest.mark.asyncio
async def test_signal():
    with patch.object(WorkflowHandle, 'signal', new=on_signal) as signal_mock:
        env = ActivityEnvironment()
        result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, None, BatchPage("some_cursor", 10), 0, "some_args", False)

expected_heartbeat_details = "signaled_next_page"

def on_heartbeat(details):
    assert details == expected_heartbeat_details

@pytest.mark.asyncio
async def test_heartbeat():
    with patch.object(WorkflowHandle, 'signal', new=on_signal) as signal_mock:
        env = ActivityEnvironment()
        env.on_heartbeat = on_heartbeat

        result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, None, BatchPage("some_cursor", 10), 0, "some_args", False)

@pytest.mark.asyncio
async def test_idempotency():
    # Simulate the first time the page is processed and we should signal the workflow
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock:
            env = ActivityEnvironment()
            result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, None, BatchPage("some_cursor", 10), 0, "some_args", False)
        
            signal_mock.assert_awaited_once()

    # And the second time, we shouldn't.
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock, patch('temporalio.activity.info') as mock_activity_info:
            instance = mock_activity_info.return_value
            instance.heartbeat_details = [expected_heartbeat_details]
            env = ActivityEnvironment()
            result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, None, BatchPage("some_cursor", 10), 0, "some_args", False)
        
            signal_mock.assert_not_awaited()

@pytest.mark.asyncio
async def test_extended_retry_does_not_resignal():
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock:
        env = ActivityEnvironment()
        # Pass in that we already signaled, so when the activity enqueues the new page, we ignore it.
        result = await env.run(process_page, starts_new_page.__name__, None, BatchPage("some_cursor", 10), 0, "some_args", True)
        signal_mock.assert_not_awaited()

@page_processor
async def attempts_to_signal_twice(context: BatchProcessorContext):
    current_page = context.page
    await context.enqueue_next_page(BatchPage("second_cursor", current_page.size))
    await context.enqueue_next_page(BatchPage("third_cursor", current_page.size))

@page_processor
async def checks_batch_id(context: BatchProcessorContext):
    assert context.batch_id == "my_batch_id"

@pytest.mark.asyncio
async def test_batch_id():
    with patch.object(WorkflowHandle, 'signal') as signal_mock:
        env = ActivityEnvironment()
        result = await env.run(process_page, checks_batch_id.__name__, "my_batch_id", BatchPage("first_cursor", 10), 0, "some_args", False)    

@pytest.mark.asyncio
async def test_cannot_enqueue_two_pages():
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock:
        env = ActivityEnvironment()
        try:
            # Pass in that we already signaled, so when the activity enqueues the new page, we ignore it.
            result = await env.run(process_page, attempts_to_signal_twice.__name__, None, BatchPage("first_cursor", 10), 0, "some_args", False)
        except AssertionError as e:
            assert str(e) == ("You cannot call enqueue_next_page twice in the same page_processor.  Each processed page " +
              "is responsible for enqueuing the following page.")
        else: 
            assert False, "Should have asserted preventing the user from calling enqueue_next_page twice."

if __name__ == "__main__":
    pytest.main(sys.argv)

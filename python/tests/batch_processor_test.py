from __future__ import annotations
import sys

try:
    from dataclasses import dataclass
    from typing import Any, Sequence, Optional
    from unittest.mock import patch, AsyncMock

    import pytest

    from temporalio.client import WorkflowHandle, Client
    from temporalio.testing import ActivityEnvironment
    from temporalio.activity import info
    import temporalio.common
    import temporalio.exceptions

    from batch_worker import BatchWorkerClient
    from batch_processor import BatchProcessorContext, BatchPage, page_processor, process_page
except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print("But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry.")
    print(f"Original error: {e}")
    sys.exit(1)

async def run_page_processor(
        page_processor_name: str, *,
        env=ActivityEnvironment(),
        batch_id: Optional[str]=None, 
        did_signal_next_page: bool = False) -> Any:
    client = await Client.connect("localhost:7233")
    BatchWorkerClient.augment(client)

    return await env.run(
        process_page, 
        page_processor_name, 
        batch_id, 
        BatchPage("some_cursor", 10), 
        0, 
        "some_args", 
        did_signal_next_page)

@page_processor
async def returns_cursor(context: BatchProcessorContext):
    assert context.args_str == "some_args"
    return context.page.cursor_str


@pytest.mark.asyncio
async def test_page_processor():
    result = await run_page_processor(returns_cursor.__name__)
    assert result == "some_cursor"

@pytest.mark.asyncio
async def test_invalid_page_processor_name():
    env = ActivityEnvironment()
    try:
        await run_page_processor("not a callable")
    except ValueError as e:
        assert str(e).startswith(f"You passed page_processor_name 'not a callable' into the BatchOrchestrator, but it was not registered on " + \
            f"your worker. Please annotate it with @page_processor and make sure its module is imported. " + \
            f"Available callables: [")
        assert "'returns_cursor'" in str(e)
    else:
        assert False, "Should have thrown an error."

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
        await run_page_processor(starts_new_page.__name__)

expected_heartbeat_details = "signaled_next_page"

def on_heartbeat(details):
    assert details == expected_heartbeat_details

@pytest.mark.asyncio
async def test_heartbeat():
    with patch.object(WorkflowHandle, 'signal', new=on_signal) as signal_mock:
        env = ActivityEnvironment()
        env.on_heartbeat = on_heartbeat
        await run_page_processor(starts_new_page.__name__, env=env)

@pytest.mark.asyncio
async def test_idempotency():
    # Simulate the first time the page is processed and we should signal the workflow
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock:
        await run_page_processor(starts_new_page.__name__)
        signal_mock.assert_awaited_once()

    # And the second time, we shouldn't.
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock, patch('temporalio.activity.info') as mock_activity_info:
        instance = mock_activity_info.return_value
        instance.heartbeat_details = [expected_heartbeat_details]

        await run_page_processor(starts_new_page.__name__)
        signal_mock.assert_not_awaited()

@pytest.mark.asyncio
async def test_extended_retry_does_not_resignal():
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock:
        await run_page_processor(starts_new_page.__name__, did_signal_next_page=True)
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
        await run_page_processor(checks_batch_id.__name__, batch_id="my_batch_id")

@pytest.mark.asyncio
async def test_cannot_enqueue_two_pages():
    with patch.object(WorkflowHandle, 'signal', new_callable=AsyncMock) as signal_mock:
        env = ActivityEnvironment()
        try:
            await run_page_processor(attempts_to_signal_twice.__name__)
        except AssertionError as e:
            assert str(e) == ("You cannot call enqueue_next_page twice in the same page_processor.  Each processed page " +
              "is responsible for enqueuing the following page.")
        else: 
            assert False, "Should have asserted preventing the user from calling enqueue_next_page twice."

@pytest.mark.asyncio
async def test_uninitialized_client():
    env = ActivityEnvironment()
    BatchWorkerClient.get_instance()._clear_temporal_client()
    try:
        return await env.run(
            process_page, 
            starts_new_page.__name__, 
            None, 
            BatchPage("some_cursor", 10), 
            0, 
            "some_args", 
            False)
    except ValueError as e:
        assert str(e) == "Missing a temporal client for use by your @page_processor or @batch_tracker. " + \
            "Make sure to call BatchWorkerClient.augment(client) and pass the resulting client into your Worker."
    else:
        assert False, "Should have thrown an error."

if __name__ == "__main__":
    pytest.main(sys.argv)

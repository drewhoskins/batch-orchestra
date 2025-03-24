from __future__ import annotations

import sys
from typing import Type

try:
    from typing import Any, Optional, Sequence
    from unittest.mock import AsyncMock, patch

    import pytest
    import temporalio.common
    import temporalio.exceptions

    from batch_orchestra.batch_worker import BatchWorkerClient
    from batch_orchestra.batch_processor import BatchProcessorContext, BatchPage, page_processor, process_page, PageProcessor
except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print(
        "But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry."
    )
    print(f"Original error: {e}")
    sys.exit(1)


async def run_page_processor(
    page_processor_name: Type,
    *,
    env=None,
    batch_id: Optional[str] = None,
    did_signal_next_page: bool = False,
) -> Any:
    if env is None:
        # if we do this in the argument list, then ActivityEnvironment is instantiated once and reused
        # for the entire lifetime of the program. We want to create a new one for each call
        env = ActivityEnvironment()
    try:
        client = await Client.connect("localhost:7233")
    except RuntimeError as e:
        message = (
            "You should be running a Temporal Server locally.  See the Quick Start in the README.md for instructions."
        )
        raise ValueError(message) from e
    BatchWorkerClient.register(client)

    return await env.run(
        process_page,
        page_processor_name if isinstance(page_processor_name, str) else page_processor_name.__name__,
        batch_id,
        BatchPage("some_cursor", 10),
        0,
        "some_args",
        did_signal_next_page,
    )


@page_processor
class ReturnsCursor(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        assert context.args_str == "some_args"
        return context.page.cursor_str

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@pytest.mark.asyncio
async def test_page_processor():
    result = await run_page_processor(ReturnsCursor)
    assert result == "some_cursor"


@pytest.mark.asyncio
async def test_invalid_page_processor_name():
    try:
        await run_page_processor("not a callable")
    except ValueError as e:
        assert str(e).startswith(
            "You passed page_processor_name 'not a callable' into the BatchOrchestrator, but it was not registered on "
            + "your worker. Please annotate a class inheriting from batch_processor.PageProcessor with @page_processor and make sure its module is imported. "
            + "Available classes: ["
        )
        assert "'ReturnsCursor'" in str(e)
    else:
        raise AssertionError("Should have thrown an error.")


@page_processor
class StartsNewPage(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        page = context.page
        next_page = BatchPage(page.cursor_str + "_the_second", page.size)
        await context.enqueue_next_page(next_page)

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


async def on_signal(parent_workflow, signal: str, arg: Any = temporalio.common._arg_unset, *, args: Sequence[Any]):
    assert signal == "_signal_add_page"
    page = args[0]
    page_num = args[1]
    assert page.cursor_str == "some_cursor_the_second"
    assert page_num == 1  # Signals for the next page after page 0


@pytest.mark.asyncio
async def test_signal():
    with patch.object(WorkflowHandle, "signal", new=on_signal):
        await run_page_processor(StartsNewPage)


expected_heartbeat_details = "signaled_next_page"


def on_heartbeat(details):
    assert details == expected_heartbeat_details


@pytest.mark.asyncio
async def test_heartbeat():
    with patch.object(WorkflowHandle, "signal", new=on_signal):
        env = ActivityEnvironment()
        env.on_heartbeat = on_heartbeat
        await run_page_processor(StartsNewPage, env=env)


@pytest.mark.asyncio
async def test_idempotency():
    # Simulate the first time the page is processed and we should signal the workflow
    with patch.object(WorkflowHandle, "signal", new_callable=AsyncMock) as signal_mock:
        await run_page_processor(StartsNewPage)
        signal_mock.assert_awaited_once()

    # And the second time, we shouldn't.
    with (
        patch.object(WorkflowHandle, "signal", new_callable=AsyncMock) as signal_mock,
        patch("temporalio.activity.info") as mock_activity_info,
    ):
        instance = mock_activity_info.return_value
        instance.heartbeat_details = [expected_heartbeat_details]

        await run_page_processor(StartsNewPage)
        signal_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_extended_retry_does_not_resignal():
    with patch.object(WorkflowHandle, "signal", new_callable=AsyncMock) as signal_mock:
        await run_page_processor(StartsNewPage, did_signal_next_page=True)
        signal_mock.assert_not_awaited()


@page_processor
class AttemptsToSignalTwice(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        current_page = context.page
        await context.enqueue_next_page(BatchPage("second_cursor", current_page.size))
        await context.enqueue_next_page(BatchPage("third_cursor", current_page.size))

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@page_processor
class ChecksBatchID(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        assert context.batch_id == "my_batch_id"

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@pytest.mark.asyncio
async def test_batch_id():
    with patch.object(WorkflowHandle, "signal"):
        await run_page_processor(ChecksBatchID, batch_id="my_batch_id")


@pytest.mark.asyncio
async def test_cannot_enqueue_two_pages():
    with patch.object(WorkflowHandle, "signal", new_callable=AsyncMock):
        try:
            await run_page_processor(AttemptsToSignalTwice)
        except AssertionError as e:
            assert str(e) == (
                "You cannot call enqueue_next_page twice in the same page_processor.  Each processed page "
                + "is responsible for enqueuing the following page."
            )
        else:
            raise AssertionError("Should have asserted preventing the user from calling enqueue_next_page twice.")


@pytest.mark.asyncio
async def test_uninitialized_client():
    env = ActivityEnvironment()
    BatchWorkerClient.get_instance()._clear_temporal_client()
    try:
        return await env.run(
            process_page, StartsNewPage.__name__, None, BatchPage("some_cursor", 10), 0, "some_args", False
        )
    except ValueError as e:
        assert (
            str(e)
            == "Missing a temporal client for use by your @page_processor or @batch_tracker. "
            + "Make sure to call BatchWorkerClient.register(client)."
        )
    else:
        raise AssertionError("Should have thrown an error.")


@page_processor
class TooManyRetries(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        raise ValueError("I'm a bad page processor.")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_MOST_ONCE

    @property
    def initial_retry_policy(self):
        return temporalio.common.RetryPolicy(maximum_attempts=2)


@pytest.mark.asyncio
async def test_invalid_config_too_many_retries():
    try:
        await run_page_processor(TooManyRetries)
    except ValueError as e:
        assert (
            str(e)
            == "@page_processor TooManyRetries: You cannot set initial_retry_policy.maximum_attempts to anything other than 1 (got 2) for retry_mode EXECUTE_AT_MOST_ONCE."
        )


@page_processor
class ExtendedRetriesWithNoRetries(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        raise ValueError("I'm a bad page processor.")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_MOST_ONCE

    @property
    def use_extended_retries(self):
        return True


@pytest.mark.asyncio
async def test_invalid_config_extended_retries_and_at_most_once():
    try:
        await run_page_processor(ExtendedRetriesWithNoRetries)
    except ValueError as e:
        assert (
            str(e)
            == "@page_processor ExtendedRetriesWithNoRetries: You cannot set use_extended_retries for retry_mode EXECUTE_AT_MOST_ONCE."
        )


if __name__ == "__main__":
    pytest.main(sys.argv)

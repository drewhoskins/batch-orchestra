from __future__ import annotations

import sys

try:
    import json
    import logging
    import uuid
    from asyncio import sleep
    from dataclasses import asdict, dataclass
    from datetime import datetime, timedelta
    from typing import Optional

    import pytest
    from temporalio.client import Client, WorkflowContinuedAsNewError, WorkflowFailureError, WorkflowHandle
    from temporalio.common import RetryPolicy
    from temporalio.exceptions import ApplicationError, TimeoutError
    from temporalio.service import RPCError
    from temporalio.worker import Worker

    from batch_orchestra.batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
    from batch_orchestra.batch_orchestrator_client import BatchOrchestratorClient, BatchOrchestratorHandle
    from batch_orchestra.batch_processor import BatchPage, BatchProcessorContext, PageProcessor, page_processor
except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print(
        "But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry."
    )
    print(f"Original error: {e}")
    sys.exit(1)


async def start_orchestrator(
    client: Client, task_queue_name: str, input: BatchOrchestratorInput, *, run_timeout: Optional[timedelta] = None
) -> BatchOrchestratorHandle:
    return await BatchOrchestratorClient(client).start(
        input, id=str(uuid.uuid4()), task_queue=task_queue_name, run_timeout=run_timeout
    )


@dataclass
class MyCursor:
    i: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str) -> MyCursor:
        return MyCursor(**json.loads(json_str))


@dataclass(kw_only=True)
class MyArgs:
    num_items_to_process: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str) -> MyArgs:
        return MyArgs(**json.loads(json_str))


def default_cursor():
    return MyCursor(0).to_json()


@page_processor
class ProcessesOnePage(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        return context.page.cursor_str


def batch_worker(client: Client, task_queue_name: str):
    return Worker(
        client,
        task_queue=task_queue_name,
        workflows=[BatchOrchestrator],
        activities=[process_page],
        # So people can add breakpoints to the workflow without it timing out.
        debug_mode=True,
    )


@pytest.mark.asyncio
async def test_one_page(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with batch_worker(client, task_queue_name):
        before_start_time = datetime.now()
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                args=MyArgs(num_items_to_process=1).to_json(),
                page_size=10,
                first_cursor_str=default_cursor(),
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 1
        assert result.max_parallelism_achieved == 1
        assert result.is_finished
        assert result.start_time() >= before_start_time
        assert result.start_time() <= datetime.now()


# Testing with spawns_second_page will ensure that the workflow is signaled and that it processes the second page
@pytest.mark.asyncio
async def test_two_pages(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                args=MyArgs(num_items_to_process=19).to_json(),
                page_size=10,
                first_cursor_str=default_cursor(),
            ),
            max_parallelism=10,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 2  # 10 first page, 9 second
        assert result.max_parallelism_achieved >= 1
        assert result.is_finished


@pytest.mark.asyncio
async def test_aggressive_continue_as_new(client: Client):
    task_queue_name = str(uuid.uuid4())
    # Unlikely users will put pages_per_run < max_parallelism, but we should support it
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                args=MyArgs(num_items_to_process=45).to_json(),
                page_size=10,
                first_cursor_str=default_cursor(),
            ),
            max_parallelism=10,
            pages_per_run=2,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        run_id_2 = None
        try:
            result = await handle.result(follow_runs=False)
        except WorkflowContinuedAsNewError as e:
            run_id_2 = e.new_execution_run_id
        assert run_id_2 is not None
        handle = BatchOrchestratorClient(client).get_handle(handle.workflow_handle.id, run_id=run_id_2)
        run_id_3 = None
        try:
            result = await handle.result(follow_runs=False)
        except WorkflowContinuedAsNewError as e:
            run_id_3 = e.new_execution_run_id
        assert run_id_3 is not None
        handle = BatchOrchestratorClient(client).get_handle(handle.workflow_handle.id, run_id=run_id_3)
        result = await handle.result()

        assert result.num_completed_pages == 5  # 4 pages * 10 + 1 page * 5
        assert result.max_parallelism_achieved >= 1
        assert result.is_finished


@pytest.mark.asyncio
async def test_continue_as_new(client: Client):
    task_queue_name = str(uuid.uuid4())
    # Unlikely pages_per_run < max_parallelism but we should support it
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                args=MyArgs(num_items_to_process=45).to_json(),
                page_size=10,
                first_cursor_str=default_cursor(),
            ),
            max_parallelism=2,
            pages_per_run=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        try:
            await handle.result(follow_runs=False)
        except WorkflowContinuedAsNewError as e:
            run_id = e.new_execution_run_id
            assert run_id is not None
            handle = BatchOrchestratorClient(client).get_handle(handle.workflow_handle.id, run_id=run_id)
            result = await handle.result()
        assert result.num_completed_pages == 5  # 4 pages * 10 + 1 page * 5
        assert result.max_parallelism_achieved >= 1
        assert result.is_finished
        assert handle.workflow_handle.result_run_id != handle.workflow_handle.first_execution_run_id


@pytest.mark.asyncio
async def test_start_paused_and_add_parallelism(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        # Start paused
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                args=MyArgs(num_items_to_process=19).to_json(),
                page_size=10,
                first_cursor_str=default_cursor(),
            ),
            max_parallelism=0,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        # Wait for a little while; should not complete
        try:
            result = await handle.result(rpc_timeout=timedelta(seconds=2))
        except RPCError as e:
            assert "Timeout expired" in str(e)

        # Begin processing
        await handle.set_max_parallelism(5)
        result = await handle.result()
        assert result.num_completed_pages == 2


@page_processor
class ProcessesNItems(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        page = context.page
        args = MyArgs.from_json(context.args_str)
        cursor = MyCursor.from_json(page.cursor_str)
        # Simulate whether we get an incomplete page.  If not, start a new page.
        if cursor.i + page.size < args.num_items_to_process - 1:
            await context.enqueue_next_page(BatchPage(MyCursor(cursor.i + page.size).to_json(), size=page.size))
            print(f"Signaled the workflow {page}")
        print(f"Processing page {page}")
        # pretend to do some processing
        await sleep(0.1)
        return cursor.i

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@pytest.mark.asyncio
async def test_max_parallelism(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        max_parallelism = 3
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                args=MyArgs(num_items_to_process=59).to_json(),
                page_size=10,
                first_cursor_str=default_cursor(),
            ),
            max_parallelism=max_parallelism,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 6  # 5 pages * 10 + 1 page * 9
        # While not a rock-hard guarantee, max_parallelism should be achieved in practice because I'm sleeping within the activity
        assert result.max_parallelism_achieved == max_parallelism


@pytest.mark.asyncio
async def test_page_size(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                args=MyArgs(num_items_to_process=49).to_json(),
                page_size=5,
                first_cursor_str=default_cursor(),
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 10  # 9 pages * 5 items + 1 page with 4


@page_processor
class AssertsTimeout(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        timeout = context._activity_info.start_to_close_timeout
        assert timeout is not None
        assert timeout.seconds == 123

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@pytest.mark.asyncio
async def test_timeout(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=AssertsTimeout.__name__, page_size=10, timeout_seconds=123, first_cursor_str=default_cursor()
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 1


did_attempt_fails_once = False


@page_processor
class FailsOnce(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        global did_attempt_fails_once
        if not did_attempt_fails_once:
            did_attempt_fails_once = True
            raise ValueError("I failed (note: this is expected during testing)")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    # Kick in extended retries immediately to make the test run faster
    @property
    def initial_retry_policy(self):
        return RetryPolicy(maximum_attempts=1)


@pytest.mark.asyncio
async def test_extended_retries(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsOnce.__name__, page_size=10, timeout_seconds=123, first_cursor_str=default_cursor()
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 1


@page_processor
class SignalsSamePageInfinitely(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        await context.enqueue_next_page(BatchPage("the everpage", 10))

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@pytest.mark.asyncio
async def test_ignores_subsequent_signals(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=SignalsSamePageInfinitely.__name__, page_size=10, first_cursor_str="page one"
            ),
            max_parallelism=3,
            batch_id="my_batch",
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 2


did_attempt_fails_before_signal = False


@page_processor
class FailsBeforeSignal(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        global did_attempt_fails_before_signal
        should_fail = not did_attempt_fails_before_signal
        did_attempt_fails_before_signal = True
        if should_fail:
            raise ValueError("I failed (note: this is expected during testing)")
        if context.page.cursor_str == "page one":
            await context.enqueue_next_page(BatchPage("page two", 10))

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    # Kick in extended retries immediately to make the test run faster
    @property
    def initial_retry_policy(self):
        return RetryPolicy(maximum_attempts=1)


@pytest.mark.asyncio
async def test_extended_retries_first_page_fails_before_signal(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id="my_batch",
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsBeforeSignal.__name__, page_size=10, first_cursor_str="page one"
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 2


@page_processor
class TwoPagesOneFailsNoRetries(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        if context.page.cursor_str == "page one":
            await context.enqueue_next_page(BatchPage("page two", 10))
            raise ValueError("I failed (note: this is expected during testing)")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_MOST_ONCE


@pytest.mark.asyncio
async def test_no_retries(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id="my_batch",
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=TwoPagesOneFailsNoRetries.__name__, page_size=10, first_cursor_str="page one"
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        # The first page signals for another page, then fails.  Second page succeeds.
        assert result.num_failed_pages == 1
        assert result.num_completed_pages == 1


did_attempt_fails_after_signal = False


@page_processor
class FailsAfterSignal(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        global did_attempt_fails_after_signal
        should_fail = not did_attempt_fails_after_signal
        did_attempt_fails_after_signal = True
        current_page = context.page
        if current_page.cursor_str == "page one":
            await context.enqueue_next_page(BatchPage("page two", current_page.size))
        await sleep(0.1)
        if should_fail:
            raise ValueError("I failed (note: this is expected during testing)")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    # Kick in extended retries immediately to make the test run faster
    @property
    def initial_retry_policy(self):
        return RetryPolicy(maximum_attempts=1)


def count_signal_calls(func):
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        return func(*args, **kwargs)

    wrapper.calls = 0
    return wrapper


@pytest.mark.asyncio
async def test_extended_retries_first_page_fails_after_signal(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        WorkflowHandle.signal = count_signal_calls(WorkflowHandle.signal)
        input = BatchOrchestratorInput(
            batch_id="my_batch",
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsAfterSignal.__name__, page_size=10, first_cursor_str="page one"
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        # We signal the orchestrator with a new page once during the initial retries.  Then when we restart the activity,
        # we will use that info avoid signaling again.
        assert WorkflowHandle.signal.calls == 1
        assert result.num_completed_pages == 2


class SomeNonRetryableException(Exception):
    pass


call_count = 0


@page_processor
class FailsWithNonRetryableException(PageProcessor):
    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    @property
    def initial_retry_policy(self):
        return RetryPolicy(non_retryable_error_types=["SomeNonRetryableException"])

    async def run(self, context: BatchProcessorContext):
        global call_count
        call_count += 1
        raise SomeNonRetryableException("I failed, please don't retry.")


@pytest.mark.asyncio
async def test_non_retryable_exceptions(client: Client):
    task_queue_name = str(uuid.uuid4())
    global call_count
    call_count = 0
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsWithNonRetryableException.__name__, page_size=10, first_cursor_str="page one"
            ),
            max_parallelism=3,
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert call_count == 1
        assert result.num_completed_pages == 0
        assert result.num_failed_pages == 1


@page_processor
class FailsWithNonRetryableApplicationError(PageProcessor):
    @count_signal_calls
    async def run(self, context: BatchProcessorContext):
        global call_count
        call_count += 1
        raise ApplicationError("I failed, please don't retry.", non_retryable=True)

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@pytest.mark.asyncio
async def test_non_retryable_application_error(client: Client):
    task_queue_name = str(uuid.uuid4())
    global call_count
    call_count = 0
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsWithNonRetryableApplicationError.__name__, page_size=10, first_cursor_str="page one"
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert call_count == 1
        assert result.num_completed_pages == 0
        assert result.num_failed_pages == 1


did_attempt_times_out_first_time = False


@page_processor
class TimesOutFirstTime(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        global call_count
        call_count += 1
        global did_attempt_times_out_first_time
        if not did_attempt_times_out_first_time:
            did_attempt_times_out_first_time = True
            await sleep(5)

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    # Kick in extended retries immediately to make the test run faster
    @property
    def initial_retry_policy(self):
        return RetryPolicy(maximum_attempts=1)


@pytest.mark.asyncio
async def test_timeout_then_extended_retry(client: Client):
    task_queue_name = str(uuid.uuid4())
    global call_count
    call_count = 0
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=TimesOutFirstTime.__name__, page_size=10, first_cursor_str="page one", timeout_seconds=1
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert call_count == 2
        assert result.num_completed_pages == 1
        assert result.num_failed_pages == 0


@page_processor
class TwoRetriesNoExtended(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        global call_count
        call_count += 1
        raise ValueError("I failed (note: this is expected during testing)")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    @property
    def initial_retry_policy(self):
        # something low to make the test run faster
        return RetryPolicy(maximum_attempts=2, backoff_coefficient=1.0)

    @property
    def use_extended_retries(self):
        return False


@pytest.mark.asyncio
async def test_no_extended_retries(client: Client):
    task_queue_name = str(uuid.uuid4())
    global call_count
    call_count = 0
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=TwoRetriesNoExtended.__name__, page_size=10, first_cursor_str="page one"
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 0
        assert result.num_failed_pages == 1
        assert call_count == 2


@page_processor
class FailsPermanently(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        global call_count
        call_count += 1
        raise ValueError("I failed (note: this is expected during testing)")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    @property
    def extended_retry_interval_seconds(self) -> int:
        return 1

    @property
    def initial_retry_policy(self):
        # something low to make the test run faster
        return RetryPolicy(maximum_attempts=1)


@pytest.mark.asyncio
async def test_extended_retries_infinitely_and_times_out(client: Client):
    task_queue_name = str(uuid.uuid4())
    global call_count
    call_count = 0

    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsPermanently.__name__, page_size=10, first_cursor_str="page one"
            ),
        )
        # Users may set the workflow to timeout, which isn't recommended but happens.
        handle = await start_orchestrator(client, task_queue_name, input, run_timeout=timedelta(seconds=10))
        try:
            result = await handle.result()
        except WorkflowFailureError as e:
            # Sometimes, the workflow execution times out, but sometimes it seems temporal times the activity out
            # and lets the workflow recover.
            assert "Workflow timed out" in str(e.__cause__)
        else:
            assert result.num_completed_pages == 0
            assert result.num_failed_pages == 1
        finally:
            # How to test that it would retry forever?
            # 4 calls = 1 for the original plus 3 extended retries.  Presumably 3 is a clue it will keep going.
            assert call_count >= 4


@pytest.mark.asyncio
async def test_workflow_times_out(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                page_size=10,
                first_cursor_str=default_cursor(),
                args=MyArgs(num_items_to_process=10000).to_json(),
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input, run_timeout=timedelta(seconds=1))
        try:
            await handle.result()
        except WorkflowFailureError as e:
            assert isinstance(e.__cause__, TimeoutError)
        else:
            raise AssertionError("Expected WorkflowFailureError")


class MyHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


@page_processor
class SpitsOutLogs(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        context.logger.info("I'm processing a page")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


@pytest.mark.asyncio
async def test_logging(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with batch_worker(client, task_queue_name):
        log_capture = MyHandler()
        workflow_logger = logging.getLogger('batch_orchestra.batch_orchestrator')
        workflow_logger.addHandler(log_capture)
        activity_logger = logging.getLogger('batch_orchestra.batch_processor')
        activity_logger.addHandler(log_capture)

        input = BatchOrchestratorInput(
            batch_id="my_batch",
            max_parallelism=10,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=SpitsOutLogs.__name__,
                page_size=10,
                first_cursor_str=default_cursor(),
                args=MyArgs(num_items_to_process=19).to_json(),
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        await handle.result()
        # Make sure workflow logs the batch ID.
        first_log = log_capture.records[0].__dict__
        assert first_log["msg"].startswith("Starting batch.")
        assert first_log["batch_id"] == "my_batch"

        # Now make sure the activity logs it
        activity_log = next(
            filter(lambda r: r.__dict__["msg"].startswith("I'm processing a page"), log_capture.records), None
        )
        assert activity_log is not None
        assert activity_log.__dict__["batch_id"] == "my_batch"


@pytest.mark.asyncio
async def test_current_progress_query(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id="my_batch",
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=ProcessesNItems.__name__,
                page_size=10,
                first_cursor_str=default_cursor(),
                args=MyArgs(num_items_to_process=19).to_json(),
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        interim_result = await handle.get_progress()
        assert interim_result.num_completed_pages in [0, 1, 2]
        result = await handle.result()
        assert result.num_completed_pages == 2


if __name__ == "__main__":
    pytest.main(sys.argv)

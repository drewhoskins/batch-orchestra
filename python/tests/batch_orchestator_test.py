from __future__ import annotations
from asyncio import sleep
from dataclasses import asdict, dataclass
import json
import logging
from unittest.mock import patch
import pytest
import uuid

from batch_processor import BatchPage
from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
from temporalio.common import RetryPolicy
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker
from temporalio.exceptions import ApplicationError
from batch_processor import BatchProcessorContext, page_processor

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
async def processes_one_page(context: BatchProcessorContext):
    return context.get_page().cursor_str

def batch_worker(client: Client, task_queue_name: str):
    return Worker(
        client,
        task_queue=task_queue_name,
        workflows=[BatchOrchestrator],
        activities=[process_page],
        # So people can add breakpoints to the workflow without it timing out.
        debug_mode=True
    )

@pytest.mark.asyncio
async def test_one_page(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor_name=processes_n_items.__name__, 
            max_parallelism=3, 
            page_size=10,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=1).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 1
        assert result.max_parallelism_achieved == 1

# Testing with spawns_second_page will ensure that the workflow is signaled and that it processes the second page
@pytest.mark.asyncio
async def test_two_pages(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor_name=processes_n_items.__name__, 
            max_parallelism=10, 
            page_size=10,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=19).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 2 # 10 first page, 9 second
        assert result.max_parallelism_achieved >= 1

@page_processor
async def processes_n_items(context: BatchProcessorContext):
    page = context.get_page()
    args = MyArgs.from_json(context.get_args())
    cursor = MyCursor.from_json(page.cursor_str)
    # Simulate whether we get an incomplete page.  If not, start a new page.
    if cursor.i + page.size < args.num_items_to_process - 1:
        await context.enqueue_next_page(
            BatchPage(MyCursor(cursor.i + page.size).to_json(), size=page.size)
        )
        print(f"Signaled the workflow {page}")
    print(f"Processing page {page}")
    # pretend to do some processing
    await sleep(0.1)
    return cursor.i


@pytest.mark.asyncio
async def test_max_parallelism(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        max_parallelism = 3
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=processes_n_items.__name__, 
            max_parallelism=max_parallelism, 
            page_size=10,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=59).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 6 # 5 pages * 10 + 1 page * 9
        # While not a rock-hard guarantee, max_parallelism should be achieved in practice because I'm sleeping within the activity
        assert result.max_parallelism_achieved == max_parallelism

@pytest.mark.asyncio
async def test_page_size(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=processes_n_items.__name__, 
            max_parallelism=3, 
            page_size=5,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=49).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 10 # 9 pages * 5 items + 1 page with 4

@page_processor
async def asserts_timeout(context: BatchProcessorContext):
    timeout = context._activity_info.start_to_close_timeout
    assert timeout is not None
    assert timeout.seconds == 123

@pytest.mark.asyncio
async def test_timeout(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=asserts_timeout.__name__, 
            max_parallelism=3, 
            page_size=10,
            page_timeout_seconds=123,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=49).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 1

did_attempt_fails_once = False
@page_processor
async def fails_once(context: BatchProcessorContext):
    global did_attempt_fails_once
    if not did_attempt_fails_once:
        did_attempt_fails_once = True
        raise ValueError("I failed")

@pytest.mark.asyncio
async def test_extended_retries(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=fails_once.__name__, 
            max_parallelism=3, 
            page_size=10,
            first_cursor_str="page one",
            # extended retries should pick it up.
            initial_retry_policy=RetryPolicy(maximum_attempts=1))
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 1

@page_processor
async def signals_same_page_infinitely(context: BatchProcessorContext):
    # The batch framework should detect this and ignore subsequent signals and terminate
    await context.enqueue_next_page(BatchPage("the everpage", 10))

@pytest.mark.asyncio
async def test_ignores_subsequent_signals(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=signals_same_page_infinitely.__name__, 
            max_parallelism=3,
            page_size=10,
            first_cursor_str="page one")
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 2

did_attempt_fails_before_signal = False
@page_processor
async def fails_before_signal(context: BatchProcessorContext):
    global did_attempt_fails_before_signal
    should_fail = not did_attempt_fails_before_signal
    did_attempt_fails_before_signal = True
    if should_fail:
        raise ValueError("I failed")
    if context.get_page().cursor_str == "page one":
        await context.enqueue_next_page(BatchPage("page two", 10))

@pytest.mark.asyncio
async def test_extended_retries_first_page_fails_before_signal(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=fails_before_signal.__name__, 
            max_parallelism=3, 
            page_size=10,
            first_cursor_str="page one",
            # one try so extended retries should pick it up immediately.
            initial_retry_policy=RetryPolicy(maximum_attempts=1))
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 2

did_attempt_fails_after_signal = False
@page_processor
async def fails_after_signal(context: BatchProcessorContext):
    global did_attempt_fails_after_signal
    should_fail = not did_attempt_fails_after_signal
    did_attempt_fails_after_signal = True
    current_page = context.get_page()
    if current_page.cursor_str == "page one":
        await context.enqueue_next_page(BatchPage("page two", current_page.size))
    await sleep(0.1)
    if should_fail:
        raise ValueError("I failed")

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
            batch_id='my_batch', 
            page_processor_name=fails_after_signal.__name__, 
            max_parallelism=3, 
            page_size=10,
            first_cursor_str="page one",
            # one try so extended retries should pick it up immediately.
            initial_retry_policy=RetryPolicy(maximum_attempts=1))
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        # We signal the orchestrator with a new page once during the initial retries.  Then when we restart the activity, 
        # we will use that info avoid signaling again.
        assert WorkflowHandle.signal.calls == 1
        assert result.num_pages_processed == 2

class SomeNonRetryableException(Exception):
    pass

call_count = 0

@page_processor
async def fails_with_non_retryable_exception(context: BatchProcessorContext):
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
            batch_id='my_batch', 
            page_processor_name=fails_with_non_retryable_exception.__name__, 
            max_parallelism=3, 
            page_size=10,
            first_cursor_str="page one",
            initial_retry_policy=RetryPolicy(non_retryable_error_types=['SomeNonRetryableException']))
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert call_count == 1
        assert result.num_pages_processed == 0
        assert result.num_failed_pages == 1

@page_processor
@count_signal_calls
async def fails_with_non_retryable_application_error(context: BatchProcessorContext):
    global call_count
    call_count += 1
    raise ApplicationError("I failed, please don't retry.", non_retryable=True)

@pytest.mark.asyncio
async def test_non_retryable_application_error(client: Client):
    task_queue_name = str(uuid.uuid4())
    global call_count
    call_count = 0
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=fails_with_non_retryable_application_error.__name__, 
            max_parallelism=3, 
            page_size=10,
            first_cursor_str="page one")
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert call_count == 1
        assert result.num_pages_processed == 0
        assert result.num_failed_pages == 1

did_attempt_times_out_first_time = False


@page_processor
async def times_out_first_time(context: BatchProcessorContext):
    global call_count 
    call_count += 1
    global did_attempt_times_out_first_time
    if not did_attempt_times_out_first_time:
        did_attempt_times_out_first_time = True
        await sleep(5)

@pytest.mark.asyncio
async def test_timeout_then_extended_retry(client: Client):
    task_queue_name = str(uuid.uuid4())
    global call_count
    call_count = 0
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_id='my_batch', 
            page_processor_name=times_out_first_time.__name__, 
            max_parallelism=3, 
            page_size=10,
            page_timeout_seconds=1, # Very aggressive to induce a timeout
            first_cursor_str="page one",
            initial_retry_policy=RetryPolicy(maximum_attempts=1))
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert call_count == 2
        assert result.num_pages_processed == 1
        assert result.num_failed_pages == 0

class MyHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)

@pytest.mark.asyncio
async def test_logging(client: Client):
    task_queue_name = str(uuid.uuid4())
    
    async with batch_worker(client, task_queue_name):
        logger = logging.getLogger('batch_orchestrator')
        log_capture = MyHandler()
        logger.addHandler(log_capture)

        input = BatchOrchestratorInput(
            batch_id='my_batch',
            page_processor_name=processes_n_items.__name__, 
            max_parallelism=10, 
            page_size=10,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=19).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        first_log = log_capture.records[0].__dict__
        assert first_log['msg'].startswith("Starting batch.")
        assert first_log['batch_id'] == 'my_batch'

from __future__ import annotations

import json
import uuid
from asyncio import sleep
from dataclasses import asdict, dataclass
from typing import Optional

import pytest
from temporalio.client import Client, WorkflowContinuedAsNewError
from temporalio.worker import Worker

from batch_orchestra.batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
from batch_orchestra.batch_orchestrator_client import BatchOrchestratorClient, BatchOrchestratorHandle
from batch_orchestra.batch_processor import BatchPage, BatchProcessorContext, PageProcessor, page_processor

#
# Tests for multi-stage pipelines: each page flows through several @page_processors, each with its own queue and
# parallelism limit.
#


@dataclass
class PipelineCursor:
    i: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str) -> PipelineCursor:
        return PipelineCursor(**json.loads(json_str))


@dataclass(kw_only=True)
class PipelineArgs:
    num_items_to_process: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str) -> PipelineArgs:
        return PipelineArgs(**json.loads(json_str))


# Stage 0: paginates, and reports which item numbers are on its page.
@page_processor
class FetchesItems(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        page = context.page
        args = PipelineArgs.from_json(context.args_str)
        cursor = PipelineCursor.from_json(page.cursor_str)
        if cursor.i + page.size < args.num_items_to_process - 1:
            await context.enqueue_next_page(BatchPage(PipelineCursor(cursor.i + page.size).to_json(), size=page.size))
        await sleep(0.1)
        last = min(cursor.i + page.size, args.num_items_to_process)
        return list(range(cursor.i, last))

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


# Stage 1: doubles what stage 0 handed it.
@page_processor
class DoublesItems(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        assert not context.is_first_stage()
        items = context.previous_stage_result
        await sleep(0.1)
        return [item * 2 for item in items]

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


# Stage 2: sums what stage 1 handed it.
@page_processor
class SumsItems(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        items = context.previous_stage_result
        await sleep(0.1)
        return sum(items)

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


# Fails the first time it sees each page so we can watch a *later* stage retry without re-running earlier stages.
_failed_once = set()


@page_processor
class FailsOncePerPage(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        key = (context.batch_id, context.page.cursor_str)
        if key not in _failed_once:
            _failed_once.add(key)
            raise Exception(f"Failing {context.page.cursor_str} once, on purpose.")
        return context.previous_stage_result

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


# Tries to paginate from a non-first stage, which isn't allowed.
@page_processor
class IllegallyPaginates(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        await context.enqueue_next_page(BatchPage(PipelineCursor(0).to_json(), size=10))

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_MOST_ONCE


def batch_worker(client: Client, task_queue_name: str):
    return Worker(
        client,
        task_queue=task_queue_name,
        workflows=[BatchOrchestrator],
        activities=[process_page],
        debug_mode=True,
    )


async def start_orchestrator(
    client: Client, task_queue_name: str, input: BatchOrchestratorInput
) -> BatchOrchestratorHandle:
    return await BatchOrchestratorClient(client).start(input, id=str(uuid.uuid4()), task_queue=task_queue_name)


def pipeline_input(
    *,
    num_items: int,
    page_size: int,
    stages: list[BatchOrchestratorInput.StageContext],
    max_parallelism: int = 3,
    pages_per_run: Optional[int] = None,
    batch_id: str = "",
) -> BatchOrchestratorInput:
    return BatchOrchestratorInput(
        page_processor=BatchOrchestratorInput.PageProcessorContext(
            name=FetchesItems.__name__,
            args=PipelineArgs(num_items_to_process=num_items).to_json(),
            page_size=page_size,
            first_cursor_str=PipelineCursor(0).to_json(),
        ),
        subsequent_stages=stages,
        max_parallelism=max_parallelism,
        pages_per_run=pages_per_run,
        batch_id=batch_id,
    )


@pytest.mark.asyncio
async def test_single_stage_pipeline_reports_one_stage(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        handle = await start_orchestrator(
            client, task_queue_name, pipeline_input(num_items=19, page_size=10, stages=[])
        )
        result = await handle.result()
        assert result.num_completed_pages == 2
        assert len(result.stages) == 1
        assert result.stages[0].stage_name == FetchesItems.__name__
        assert result.stages[0].num_completed_pages == 2
        assert result.is_finished


@pytest.mark.asyncio
async def test_three_stage_pipeline(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        handle = await start_orchestrator(
            client,
            task_queue_name,
            pipeline_input(
                num_items=45,
                page_size=10,
                stages=[
                    BatchOrchestratorInput.StageContext(name=DoublesItems.__name__),
                    BatchOrchestratorInput.StageContext(name=SumsItems.__name__),
                ],
            ),
        )
        result = await handle.result()
        # num_completed_pages counts pages that made it all the way through the pipeline.
        assert result.num_completed_pages == 5
        assert len(result.stages) == 3
        # Every page passed through every stage.
        for stage_num, stage in enumerate(result.stages):
            assert stage.num_completed_pages == 5, f"stage {stage_num} only finished {stage.num_completed_pages}"
            assert stage.num_pending_pages == 0
            assert stage.num_processing_pages == 0
            assert stage.num_failed_pages == 0
        assert result.stages[1].stage_name == DoublesItems.__name__
        assert result.stages[2].stage_name == SumsItems.__name__
        assert result.is_finished


@pytest.mark.asyncio
async def test_each_stage_gets_its_own_parallelism_budget(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        handle = await start_orchestrator(
            client,
            task_queue_name,
            pipeline_input(
                num_items=100,
                page_size=10,
                max_parallelism=3,
                stages=[
                    # This stage is throttled harder than the rest of the pipeline.
                    BatchOrchestratorInput.StageContext(name=DoublesItems.__name__, max_parallelism=1),
                    BatchOrchestratorInput.StageContext(name=SumsItems.__name__),
                ],
            ),
        )
        result = await handle.result()
        assert result.num_completed_pages == 10
        assert result.stages[0].max_parallelism == 3
        assert result.stages[0].max_parallelism_achieved <= 3
        assert result.stages[1].max_parallelism == 1
        assert result.stages[1].max_parallelism_achieved == 1
        assert result.stages[2].max_parallelism == 3
        # Stages run concurrently with each other, so the pipeline as a whole exceeds any one stage's limit.
        assert result.max_parallelism_achieved > 1


@pytest.mark.asyncio
async def test_backpressure_limits_the_queue_in_front_of_a_slow_stage(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        handle = await start_orchestrator(
            client,
            task_queue_name,
            pipeline_input(
                num_items=100,
                page_size=10,
                max_parallelism=5,
                stages=[
                    BatchOrchestratorInput.StageContext(
                        name=DoublesItems.__name__, max_parallelism=1, max_queued_pages=2
                    ),
                ],
            ),
        )
        # Sample the queue depth while it runs; the first stage should never pile up more than max_queued_pages.
        deepest_queue = 0
        while True:
            progress = await handle.get_progress()
            deepest_queue = max(deepest_queue, progress.stages[1].num_pending_pages)
            if progress.is_finished:
                break
            await sleep(0.2)
        result = await handle.result()
        assert result.num_completed_pages == 10
        # The pending queue can hold max_queued_pages plus the pages already in flight upstream when it filled.
        assert deepest_queue <= 2 + result.stages[0].max_parallelism


@pytest.mark.asyncio
async def test_a_later_stage_retries_without_rerunning_earlier_stages(client: Client):
    task_queue_name = str(uuid.uuid4())
    batch_id = f"retry-test-{uuid.uuid4()}"
    async with batch_worker(client, task_queue_name):
        handle = await start_orchestrator(
            client,
            task_queue_name,
            pipeline_input(
                num_items=19,
                page_size=10,
                batch_id=batch_id,
                stages=[
                    BatchOrchestratorInput.StageContext(name=FailsOncePerPage.__name__),
                    BatchOrchestratorInput.StageContext(name=SumsItems.__name__),
                ],
            ),
        )
        result = await handle.result()
        assert result.num_completed_pages == 2
        # The first stage ran once per page even though the second stage failed and retried.
        assert result.stages[0].num_completed_pages == 2
        assert result.stages[1].num_completed_pages == 2
        assert result.stages[2].num_completed_pages == 2
        assert result.num_failed_pages == 0


@pytest.mark.asyncio
async def test_pipeline_continues_as_new_mid_flight(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        handle = await start_orchestrator(
            client,
            task_queue_name,
            pipeline_input(
                num_items=45,
                page_size=10,
                max_parallelism=2,
                pages_per_run=2,
                stages=[
                    BatchOrchestratorInput.StageContext(name=DoublesItems.__name__),
                    BatchOrchestratorInput.StageContext(name=SumsItems.__name__),
                ],
            ),
        )
        # Follow the chain of runs to the end.
        while True:
            try:
                result = await handle.result(follow_runs=False)
                break
            except WorkflowContinuedAsNewError as e:
                assert e.new_execution_run_id is not None
                handle = BatchOrchestratorClient(client).get_handle(
                    handle.workflow_handle.id, run_id=e.new_execution_run_id
                )
        assert handle.workflow_handle.run_id != handle.workflow_handle.first_execution_run_id
        assert result.num_completed_pages == 5
        for stage in result.stages:
            assert stage.num_completed_pages == 5
        assert result.is_finished


@pytest.mark.asyncio
async def test_pausing_one_stage(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        handle = await start_orchestrator(
            client,
            task_queue_name,
            pipeline_input(
                num_items=45,
                page_size=10,
                stages=[BatchOrchestratorInput.StageContext(name=DoublesItems.__name__)],
            ),
        )
        # Pause only the second stage.  The first stage should keep fetching and its results should queue up.
        await handle.set_max_parallelism(0, stage_num=1)
        while True:
            progress = await handle.get_progress()
            if progress.stages[0].num_completed_pages == 5:
                break
            await sleep(0.2)
        await sleep(0.5)
        progress = await handle.get_progress()
        assert progress.num_completed_pages == 0, "the paused stage should not have finished any pages"
        assert progress.stages[1].num_pending_pages == 5, "pages should be waiting on the paused stage"

        await handle.restore_max_parallelism(stage_num=1)
        result = await handle.result()
        assert result.num_completed_pages == 5

from __future__ import annotations

import logging
from asyncio import Future
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set, Type

import inflect
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

from .batch_orchestrator_io import BatchOrchestratorInput, BatchOrchestratorProgress, StageProgress
from .batch_processor import BatchPage, PageProcessor, get_page_processor, process_page
from .batch_tracker import track_batch_progress
from .internal.state import ContinueAsNewState, EnqueuedPage, PageTrackerData, PipelineData, StageState

#
# batch_orchestrator library
#
# This file contains the orchestrator responsible for juggling pages of work and executing them in parallel.
# It works in conjunction with the BatchProcessor library
#
# The BatchOrchestrator:
#   * Will run on a Temporal worker as a workflow--but you don't have to write the workflow yourself!
#     You can start it from your client as you would any workflow.
#     See the [Temporal Python SDK docs](https://docs.temporal.io/dev-guide/python) for more details.
#   * Your main work will be to implement a @page_processor, so see     batch_processor.py for more details.
#   * All configuration and customization is passed in with BatchOrchestratorInput.
#
# Multi-stage pipelines:
#   * A batch can be a pipeline of stages: input.page_processor is stage 0 and input.subsequent_stages are the rest.
#   * Each page flows through every stage in order, and the value a stage returns is handed to the next stage as
#     context.previous_stage_result.
#   * Each stage has its own queue and its own parallelism limit, so a slow stage doesn't consume the budget of a
#     fast one, and stages of different pages run concurrently (page 3 can be in stage 0 while page 1 is in stage 2).
#   * Only stage 0 paginates (calls context.enqueue_next_page()).


# Sandbox is off so we can import the user's @page_processor classes.  We might could make this more selective
# (and therefore safe) by allowing the user to specify modules to import.
@workflow.defn(sandboxed=False)
class BatchOrchestrator:
    # Run this to process a batch of work with controlled parallelism and in a fault-tolerant way.
    # See the docs for BatchOrchestratorInput for customizations.
    # Then invoke it like so:
    # temporal_client = await Client.connect("localhost:7233")
    # handle: WorkflowHandle[Any, BatchOrchestratorProgress] = await BatchOrchestratorClient(temporal_client).start(
    #   BatchOrchestratorInput(
    #     batch_id="my_batch_id",
    #     page_processor=BatchOrchestratorInput.PageProcessorContext(
    #       name=my_page_processor.__name__,
    #       page_size=page_size),
    #     # Optional: keep going through more activities per page.
    #     subsequent_stages=[BatchOrchestratorInput.StageContext(name=my_second_stage.__name__)],
    #     max_parallelism=5),
    #   id=f"my_workflow_id-{str(uuid.uuid4())}",
    #   task_queue="my-task-queue")
    # Now wait for the workflow to finish.  Or see BatchOrchestratorClient for all your other options.
    # results = await handle.result()
    @workflow.run
    async def run(
        self, input: BatchOrchestratorInput, state: Optional[ContinueAsNewState]
    ) -> BatchOrchestratorProgress:
        self.start_progress_tracker()

        self.logger.info(f"Starting batch.  The pipeline has {len(self.pipeline.stages)} stage(s).")
        if not state:
            first_page = BatchPage(input.page_processor.first_cursor_str, input.page_processor.page_size)
            self.pipeline.first_stage.enqueue_page(first_page, 0)
        await self.pipeline.run()

        if self.pipeline.has_stuck_pages:
            self.logger.info(
                f"Moving to extended retries: {self.pipeline.num_stuck_pages} pages are stuck, "
                + f"while {self.pipeline.num_completed_pages} processed successfully."
            )

            self.pipeline.re_enqueue_stuck_pages()
            await self.pipeline.run()

        self.logger.info(f"BatchOrchestrator completed {self.pipeline.num_completed_pages} pages")

        self.pipeline.on_finished()
        await self.finalize_progress_tracker()
        return self.current_progress()

    # Query the current progress of the batch.  If you know how many records you have, you can even provide a progress bar.
    # Invoke it as you would any Temporal query.  For example
    # handle = await client.start_workflow(...) (as documented in the run method)
    # progress = await handle.query(BatchOrchestrator.current_progress)
    @workflow.query
    def current_progress(self) -> BatchOrchestratorProgress:
        return self.pipeline.current_progress(self.start_time.timestamp())

    # Use this to pause the batch (by setting it to 0) or otherwise increase/decrease the number of
    # @page_processors that can execute at once.
    # Also "pushes" the old parallelism onto a stack so that you can restore_max_parallelism it later.
    # By default this applies to every stage of the pipeline; pass stage_num to throttle just one of them.
    @workflow.signal
    async def set_max_parallelism(self, max_parallelism: int, stage_num: Optional[int] = None) -> None:
        for stage in self.pipeline.stages_matching(stage_num):
            self.logger.info(
                f"Changing max_parallelism of {stage.describe()} to {max_parallelism} from {stage.max_parallelism}."
            )
            stage.page_tracker.update_max_parallelism(max_parallelism)

    # "pops" to the previous max_parallelism value, if there was one.
    @workflow.signal
    async def restore_max_parallelism(self, stage_num: Optional[int] = None) -> None:
        for stage in self.pipeline.stages_matching(stage_num):
            current_parallelism = stage.max_parallelism
            new_parallelism = stage.page_tracker.restore_max_parallelism()
            self.logger.info(
                f"Restoring max_parallelism of {stage.describe()} to {new_parallelism} from {current_parallelism}."
            )

    # Receives signals that new pages are ready to process and enqueues them.
    # Don't call this directly; call context.enqueue_next_page() from your @page_processor.
    # Only the first stage paginates, so new pages always enter the pipeline at the front.
    @workflow.signal
    async def _signal_add_page(self, page: BatchPage, page_num: int) -> None:
        self.logger.info(f"Enqueuing {self.logger.describe_page(page_num, page)}.")
        self.pipeline.first_stage.enqueue_page(page, page_num)

    # Starts a user-specified background activity to track the progress of the batch.
    def start_progress_tracker(self) -> Optional[Future[None]]:
        self._progress_tracker: Optional[Future[None]] = None
        if self.input.batch_tracker is not None:
            self._progress_tracker = workflow.start_activity(
                track_batch_progress,
                args=[self.input.batch_tracker.name, self.input.batch_id, self.input.batch_tracker.args],
                start_to_close_timeout=timedelta(seconds=self.input.batch_tracker.timeout_seconds),
                retry_policy=RetryPolicy(
                    backoff_coefficient=1,
                    initial_interval=timedelta(seconds=self.input.batch_tracker.polling_interval_seconds),
                ),
            )
        return self._progress_tracker

    # Call once more when we're finished.
    async def finalize_progress_tracker(self) -> None:
        if self._progress_tracker is not None:
            # Cancel so that we don't have to wait for the next polling interval before exiting the workflow.
            self._progress_tracker.cancel()
            try:
                await self._progress_tracker
            except ActivityError as e:
                if isinstance(e.__cause__, CancelledError):
                    self.logger.info("Progress tracker was cancelled.")
                    # Now invoke the tracker one last time (without polling) so the developer can get a final update.
                    tracker_future = self.start_progress_tracker()
                    assert tracker_future is not None
                    await tracker_future
                else:
                    raise e

    class LoggerAdapter(workflow.LoggerAdapter):
        def __init__(self, input: BatchOrchestratorInput) -> None:
            self._batch_id = input.batch_id
            super().__init__(logging.getLogger(__name__), {})

        def process(self, msg, kwargs):
            msg, kwargs = super().process(msg, kwargs)
            if self._batch_id != "":
                extra_data = {"batch_id": self._batch_id}
                if "extra" in kwargs:
                    kwargs["extra"].update(extra_data)
                else:
                    kwargs["extra"] = extra_data
            return msg, kwargs

        def describe_page(self, page_num: int, page: BatchPage) -> str:
            ordinal = inflect.engine().ordinal(page_num + 1)  # type: ignore
            return f"the page with cursor {page.cursor_str}, the {ordinal} page"

    #
    # A Pipeline owns one StageQueue per stage and pumps pages through them.
    # Each StageQueue enforces its own max_parallelism, so the stages proceed independently; the Pipeline is just
    # the loop that starts work wherever any stage has room for it, plus the continue-as-new bookkeeping.
    #
    class Pipeline:
        def __init__(
            self,
            *,
            input: BatchOrchestratorInput,
            logger: BatchOrchestrator.LoggerAdapter,
            state: Optional[ContinueAsNewState],
        ) -> None:
            self.input = input
            self.logger = logger
            self.data = state.pipeline_data if state else PipelineData()
            self.stages: List[BatchOrchestrator.StageQueue] = [
                BatchOrchestrator.StageQueue(
                    stage_num=stage_num,
                    stage=stage,
                    input=input,
                    logger=logger,
                    pipeline=self,
                    state=state.stages[stage_num] if state else None,
                )
                for stage_num, stage in enumerate(input.stages)
            ]
            # Wire each stage to its successor so completed pages can flow forward.
            for stage, next_stage in zip(self.stages, self.stages[1:]):
                stage.next_stage = next_stage

        @property
        def first_stage(self) -> BatchOrchestrator.StageQueue:
            return self.stages[0]

        @property
        def last_stage(self) -> BatchOrchestrator.StageQueue:
            return self.stages[-1]

        def stages_matching(self, stage_num: Optional[int]) -> List[BatchOrchestrator.StageQueue]:
            if stage_num is None:
                return self.stages
            return [self.stages[stage_num]]

        #
        # Seed the first stage with pending pages, then run this.
        # It will run until every page has flowed through every stage (or failed, or is waiting to retry).
        #
        async def run(self) -> None:
            while not self.is_quiesced():
                # Wake up (or continue) when an activity signals us with more work, when one completes, or when
                # some stage is ready to process a new page.
                await workflow.wait_condition(lambda: self.is_ready_to_start_work() or self.is_quiesced())
                # Give every stage a chance to launch work.  Later stages first, so that pages already deep in the
                # pipeline drain out (and release downstream backpressure) before we admit more at the front.
                for stage in reversed(self.stages):
                    while stage.is_new_page_ready() and not self.should_continue_as_new():
                        stage.start_page_processor_activity(stage.next_pending_page())
            if self.has_pending_pages:
                workflow.continue_as_new(
                    args=[
                        self.input,
                        ContinueAsNewState(
                            pipeline_data=self.data, stages=[stage.get_state() for stage in self.stages]
                        ),
                    ]
                )

        # True when there's nothing left to do--either the pipeline is drained, or we've stopped admitting work
        # because we're about to continue as new and the last in-flight activities have settled.
        def is_quiesced(self) -> bool:
            if self.has_processing_pages:
                return False
            return not self.has_pending_pages or self.should_continue_as_new()

        def is_ready_to_start_work(self) -> bool:
            if self.should_continue_as_new():
                return False
            return any(stage.is_new_page_ready() for stage in self.stages)

        def should_continue_as_new(self) -> bool:
            if workflow.info().is_continue_as_new_suggested():
                return True
            if not self.input.pages_per_run:
                return False
            # Only the first stage admits new pages into the pipeline, so it's the one that counts.
            return self.first_stage.page_tracker.num_pages_enqueued_in_this_run > self.input.pages_per_run

        @property
        def has_pending_pages(self) -> bool:
            return any(stage.page_tracker.has_pending_pages for stage in self.stages)

        @property
        def has_processing_pages(self) -> bool:
            return any(stage.page_tracker.has_processing_pages for stage in self.stages)

        @property
        def has_stuck_pages(self) -> bool:
            return any(stage.page_tracker.stuck_page_nums for stage in self.stages)

        @property
        def num_stuck_pages(self) -> int:
            return sum(len(stage.page_tracker.stuck_page_nums) for stage in self.stages)

        @property
        def num_processing_pages(self) -> int:
            return sum(stage.page_tracker.num_processing_pages for stage in self.stages)

        # Pages that made it out the far end of the pipeline.
        @property
        def num_completed_pages(self) -> int:
            return self.last_stage.page_tracker.num_completed_pages

        def re_enqueue_stuck_pages(self) -> None:
            for stage in self.stages:
                stage.re_enqueue_stuck_pages()

        # Called by a StageQueue whenever it starts an activity, so we can report pipeline-wide concurrency.
        def on_page_started(self) -> None:
            self.data.max_parallelism_achieved = max(self.data.max_parallelism_achieved, self.num_processing_pages)

        def on_finished(self) -> None:
            for stage in self.stages:
                stage.assert_drained()
            self.data.is_finished = True

        def current_progress(self, start_timestamp: float) -> BatchOrchestratorProgress:
            return BatchOrchestratorProgress(
                num_completed_pages=self.num_completed_pages,
                max_parallelism_achieved=self.data.max_parallelism_achieved,
                num_processing_pages=self.num_processing_pages,
                num_stuck_pages=self.num_stuck_pages,
                num_failed_pages=sum(len(stage.page_tracker.failed_page_nums) for stage in self.stages),
                is_finished=self.data.is_finished,
                _start_timestamp=start_timestamp,
                stages=[stage.current_progress() for stage in self.stages],
            )

    #
    # One stage of the pipeline: the queue of pages waiting for it, the pages it's currently processing, and the
    # policy (parallelism, retries) it processes them with.
    #
    class StageQueue:
        # Indexes and counts for the pages this stage is managing
        class PageTracker:
            def __init__(self, data: PageTrackerData) -> None:
                self.data: PageTrackerData = data
                self._num_pages_enqueued_in_this_run = len(data.pending_page_nums)

            def update_max_parallelism(self, max_parallelism: int) -> None:
                self.data.previous_max_parallelisms.append(self.data.max_parallelism)
                self.data.max_parallelism = max_parallelism

            # Returns the previous (and new) max parallelism.
            def restore_max_parallelism(self) -> int:
                if self.data.previous_max_parallelisms:
                    self.data.max_parallelism = self.data.previous_max_parallelisms.pop()
                return self.data.max_parallelism

            #
            # Status fields
            #
            def has_capacity(self) -> bool:
                return self.num_processing_pages < self.data.max_parallelism

            def get_next_page_num(self) -> int:
                assert self.data.pending_page_nums
                return self.data.pending_page_nums[0]

            #
            # Count-based properties
            #
            @property
            def num_pages_ever_enqueued(self) -> int:
                return self.data.num_pages_ever_enqueued

            @property
            def num_pages_enqueued_in_this_run(self) -> int:
                return self._num_pages_enqueued_in_this_run

            @property
            def num_completed_pages(self) -> int:
                return self.data.num_completed_pages

            @property
            def stuck_page_nums(self) -> Set[int]:
                return self.data.stuck_page_nums.copy()

            @property
            def failed_page_nums(self) -> Set[int]:
                return self.data.failed_page_nums.copy()

            @property
            def has_processing_pages(self) -> bool:
                return bool(self.data.processing_page_nums)

            @property
            def has_pending_pages(self) -> bool:
                return bool(self.data.pending_page_nums)

            @property
            def num_pending_pages(self) -> int:
                return len(self.data.pending_page_nums)

            @property
            def num_processing_pages(self) -> int:
                return len(self.data.processing_page_nums)

            @property
            def max_parallelism(self) -> int:
                return self.data.max_parallelism

            @property
            def max_parallelism_achieved(self) -> int:
                return self.data.max_parallelism_achieved

            #
            # Triggers to track changes
            #
            def on_page_enqueued(self, page_num: int) -> None:
                if page_num not in self.data.stuck_page_nums:
                    self.data.num_pages_ever_enqueued += 1
                    self._num_pages_enqueued_in_this_run += 1
                self.data.pending_page_nums.append(page_num)

            def on_page_started(self, page_num: int) -> None:
                assert page_num in self.data.pending_page_nums
                self.data.pending_page_nums.remove(page_num)
                self.data.processing_page_nums.add(page_num)
                self.data.max_parallelism_achieved = max(self.data.max_parallelism_achieved, self.num_processing_pages)

            def on_page_completed(self, page_num: int) -> None:
                assert page_num in self.data.processing_page_nums
                if page_num in self.data.stuck_page_nums:
                    self.data.stuck_page_nums.remove(page_num)
                self.data.processing_page_nums.remove(page_num)
                self.data.num_completed_pages += 1

            def on_page_got_stuck(self, page_num: int) -> None:
                assert page_num in self.data.processing_page_nums
                self.data.processing_page_nums.remove(page_num)
                self.data.stuck_page_nums.add(page_num)

            def on_page_failed(self, page_num: int) -> None:
                assert page_num in self.data.processing_page_nums
                if page_num in self.data.stuck_page_nums:
                    self.data.stuck_page_nums.remove(page_num)
                self.data.processing_page_nums.remove(page_num)
                self.data.failed_page_nums.add(page_num)

        def __init__(
            self,
            *,
            stage_num: int,
            stage: BatchOrchestratorInput.StageContext,
            input: BatchOrchestratorInput,
            logger: Type["BatchOrchestrator.LoggerAdapter"],
            pipeline: BatchOrchestrator.Pipeline,
            state: Optional[StageState],
        ) -> None:
            self.stage_num = stage_num
            self.stage = stage
            self.input = input
            self.pipeline = pipeline
            self.page_processor: PageProcessor = get_page_processor(stage.name)
            self.next_stage: Optional[BatchOrchestrator.StageQueue] = None
            if state:
                page_tracker_data = state.page_tracker_data
                # json serializes the keys as strings.
                self.pages = {int(k): v for k, v in state.pages.items()}
            else:
                page_tracker_data = PageTrackerData(max_parallelism=input.max_parallelism_for(stage))
                self.pages: Dict[int, EnqueuedPage] = {}
            self.page_tracker = BatchOrchestrator.StageQueue.PageTracker(page_tracker_data)
            self.logger = logger

        def describe(self) -> str:
            return f"stage {self.stage_num} ({self.stage.name})"

        @property
        def is_last_stage(self) -> bool:
            return self.next_stage is None

        @property
        def max_parallelism(self) -> int:
            return self.page_tracker.max_parallelism

        def get_state(self) -> StageState:
            return StageState(page_tracker_data=self.page_tracker.data, pages=self.pages)  # type: ignore[arg-type]

        # Receive new work from the first stage's page processor.  Only the first stage paginates, so only it
        # needs to defend against duplicate signals.
        def enqueue_page(self, page: BatchPage, page_num: int) -> None:
            if page_num < self.page_tracker.num_pages_ever_enqueued:
                self.logger.warning(
                    f"Got re-signaled for {self.logger.describe_page(page_num, page)}, but skipping because it was already signaled for. "
                    + "This should be rare, so please report an issue if it persists.",
                    {
                        "old_cursor": self.pages[page_num].page.cursor_str,
                        "new_cursor": page.cursor_str,
                        page_num: page_num,
                    },
                )
                return
            duplicate = next(
                (
                    enqueued_page
                    for enqueued_page in self.pages.values()
                    if enqueued_page.page.cursor_str == page.cursor_str
                ),
                None,
            )
            if duplicate:
                self.logger.warning(
                    f"Got re-signaled for {self.logger.describe_page(page_num, page)}, but skipping because it was already signaled for. "
                    + "While it's possible for duplicate signals to be sent, it's rare. Did you mis-compute your next cursor?",
                    {"old_page_num": duplicate.page_num, "new_page_num": page_num, "cursor": page.cursor_str},
                )
                return

            self._enqueue(EnqueuedPage(page=page, page_num=page_num, stage_num=self.stage_num))

        # Receive a page that just finished the previous stage, along with whatever that stage returned.
        def accept_page_from_previous_stage(self, enqueued_page: EnqueuedPage) -> None:
            assert enqueued_page.stage_num == self.stage_num
            self.logger.info(
                f"Passing {self.logger.describe_page(enqueued_page.page_num, enqueued_page.page)} to {self.describe()}."
            )
            self._enqueue(enqueued_page)

        def _enqueue(self, enqueued_page: EnqueuedPage) -> None:
            self.pages[enqueued_page.page_num] = enqueued_page
            self.page_tracker.on_page_enqueued(enqueued_page.page_num)

        def re_enqueue_stuck_pages(self) -> None:
            for page_num in self.page_tracker.stuck_page_nums:
                self.page_tracker.on_page_enqueued(page_num)

        def next_pending_page(self) -> EnqueuedPage:
            return self.pages[self.page_tracker.get_next_page_num()]

        # This stage may start another page if it has parallelism budget left, has work waiting, and isn't about to
        # dump the result onto a downstream stage that's already backed up.
        def is_new_page_ready(self) -> bool:
            return (
                self.page_tracker.has_capacity()
                and self.page_tracker.has_pending_pages
                and not self._is_next_stage_backed_up()
            )

        def _is_next_stage_backed_up(self) -> bool:
            if self.next_stage is None:
                return False
            limit = self.next_stage.stage.max_queued_pages
            if limit is None:
                return False
            return self.next_stage.page_tracker.num_pending_pages >= limit

        def assert_drained(self) -> None:
            assert not self.page_tracker.has_processing_pages
            assert not self.page_tracker.stuck_page_nums
            assert not self.page_tracker.has_pending_pages

        def current_progress(self) -> StageProgress:
            return StageProgress(
                stage_num=self.stage_num,
                stage_name=self.stage.name,
                num_pending_pages=self.page_tracker.num_pending_pages,
                num_processing_pages=self.page_tracker.num_processing_pages,
                num_completed_pages=self.page_tracker.num_completed_pages,
                num_stuck_pages=len(self.page_tracker.stuck_page_nums),
                num_failed_pages=len(self.page_tracker.failed_page_nums),
                max_parallelism=self.max_parallelism,
                max_parallelism_achieved=self.page_tracker.max_parallelism_achieved,
            )

        def is_non_retryable(self, exception: BaseException) -> bool:
            if isinstance(exception, ApplicationError):
                if exception.non_retryable:
                    return True
                users_exception_type_str = exception.type
                return users_exception_type_str in (
                    self.page_processor.initial_retry_policy.non_retryable_error_types or set()
                )
            else:
                return False

        def on_page_failing(self, page: BatchPage, page_num: int, exception: BaseException) -> None:
            # If the page told us about its successor, we need to tell the page processor not to re-signal when it
            # is processed within the extended retries phase.  This will avoid extra signals filling up the workflow history.
            # Only the first stage paginates, so this is moot for the others.
            if self.stage_num > 0:
                did_signal_next_page = False
                signaled_text = ""
            elif (page_num + 1) in self.pages:
                did_signal_next_page = True
                signaled_text = "It signaled for the next page before it got stuck."
            else:
                did_signal_next_page = False
                signaled_text = (
                    "It did not signal with the next page before the failure and may be blocking further progress."
                )

            should_extended_retry = (
                self.page_processor.use_extended_retries
                and not self.is_non_retryable(exception)
                and not self.pages[page_num].is_stuck
            )
            if should_extended_retry:
                self.logger.info(
                    f"Batch orchestrator got stuck in {self.describe()} trying {self.logger.describe_page(page_num, page)}. {signaled_text} Will retry during extended retries.",
                    {"exception": exception},
                )
                self.page_tracker.on_page_got_stuck(page_num)
            else:
                if not self.page_processor.use_extended_retries:
                    explanation = f"Will not retry because {self.page_processor.__class__.__name__}().use_extended_retries is False."
                elif self.is_non_retryable(exception):
                    explanation = f"Will not retry because {exception.type} is non-retryable."
                else:
                    assert self.pages[page_num].is_stuck
                    explanation = "Will not retry because it hard failed within extended retries (perhaps due to a workflow timeout?)."
                self.logger.error(
                    f"BatchOrchestrator failed {self.logger.describe_page(page_num, page)} in {self.describe()}, permanently. {signaled_text} {explanation}",
                    {"exception": exception},
                )
                self.page_tracker.on_page_failed(page_num)

            self.pages[page_num].set_processing_got_stuck(exception, did_signal_next_page)

        def on_page_processed(self, future: Future[Any], page_num: int, page: BatchPage) -> None:
            exception = future.exception()
            if exception is not None:
                assert isinstance(exception, ActivityError)
                exception = exception.__cause__
                assert exception is not None
                self.on_page_failing(page, page_num, exception)
                return

            self.logger.info(
                f"Batch orchestrator completed {self.describe()} for {self.logger.describe_page(page_num, page)}."
            )
            enqueued_page = self.pages[page_num]
            enqueued_page.set_processing_finished()
            self.page_tracker.on_page_completed(page_num)
            if self.next_stage is not None:
                # Hand the page, and this stage's result, to the next stage of the pipeline.  Later stages don't
                # need to remember completed pages (only the first stage does, to detect duplicate signals), so drop
                # it to keep our page dict--and the continue-as-new payload--small.
                if self.stage_num > 0:
                    del self.pages[page_num]
                self.next_stage.accept_page_from_previous_stage(enqueued_page.to_next_stage(future.result()))

        # Initiate processing the page in this stage and register a callback to record that it finished
        def start_page_processor_activity(self, enqueued_page: EnqueuedPage) -> None:
            page = enqueued_page.page
            page_num = enqueued_page.page_num
            already_tried = enqueued_page.is_stuck
            workflow.logger.info(
                f"Starting {self.describe()} for {self.logger.describe_page(page_num, page)}.  Already tried: {already_tried}."
            )
            future = workflow.start_activity(
                process_page,
                args=[
                    self.stage.name,
                    self.input.batch_id,
                    page,
                    page_num,
                    self.stage.args,
                    enqueued_page.did_signal_next_page,
                    self.stage_num,
                    enqueued_page.stage_input,
                ],
                start_to_close_timeout=timedelta(seconds=self.stage.timeout_seconds),
                retry_policy=self._build_retry_policy(self.page_processor, already_tried),
            )
            future.add_done_callback(lambda future: self.on_page_processed(future, page_num, page))
            enqueued_page.set_processing_started(future)
            self.page_tracker.on_page_started(page_num)
            self.pipeline.on_page_started()

        def _build_retry_policy(self, page_processor: PageProcessor, is_extended_retries: bool) -> RetryPolicy:
            if is_extended_retries:
                return RetryPolicy(
                    backoff_coefficient=1.0,
                    initial_interval=timedelta(seconds=self.page_processor.extended_retry_interval_seconds),
                    non_retryable_error_types=page_processor.initial_retry_policy.non_retryable_error_types,
                    maximum_attempts=0,  # Infinite
                )
            else:
                return page_processor.initial_retry_policy

    @workflow.init
    def __init__(self, input: BatchOrchestratorInput, state: Optional[ContinueAsNewState]) -> None:
        self.input = input
        self.logger = BatchOrchestrator.LoggerAdapter(input)
        self.pipeline = BatchOrchestrator.Pipeline(input=input, logger=self.logger, state=state)
        self.start_time = workflow.now()

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

# This module concerns input and output (results/progress) for the BatchOrchestrator workflow.
from typing import List, Optional

from temporalio.common import RetryPolicy


def batch_orchestrator_input_default_initial_retry_policy():
    return RetryPolicy(maximum_attempts=10)


@dataclass(kw_only=True)
class BatchOrchestratorInput:
    # Configuration for the workhorse of your batch operation: the first (and often only) stage of your pipeline.
    # It is the only stage that paginates--it calls context.enqueue_next_page() to discover more work.
    page_processor: PageProcessorContext
    # Use this to manage load on your downstream dependencies such as DBs or APIs by limiting the number of pages
    # processed simultaneously.
    # It applies to *each stage separately*: with three stages and max_parallelism=5, you may have 5 pages in stage
    # one, 5 in stage two, and 5 in stage three, all in flight at once.  Override it per stage with
    # StageContext.max_parallelism.
    max_parallelism: int
    # Optional: additional stages that each page flows through after the page_processor, in order.
    # Each stage is a @page_processor class (they share the same registry), and each one receives the value returned
    # by the previous stage in context.previous_stage_result.
    # Because results are passed through the workflow's history, return small values, e.g. keys or a summary--not
    # whole rows.
    subsequent_stages: List[StageContext] = field(default_factory=list)
    # You may monitor the progress of your batch by providing a batch tracker to execute periodically on your worker.
    # You could, for example, use it to notify somebody of stuck pages or to check if the batch is taking too long.
    # (Note: if you'd rather track progress elsewhere, you can also query current_progress on BatchOrchestrator workflow handle.)
    batch_tracker: Optional[BatchTrackerContext] = None
    # Prepended to log messages to help you identify which batch is being processed.  Useful if the batch may requires
    # multiple workflows (with separate workflow IDs) to process.
    batch_id: str = ""
    # The maximum number of pages to process in a single workflow run before continuing as new.
    # None (recommended) indicates to let Temporal decide.
    pages_per_run: Optional[int] = None

    @dataclass(kw_only=True)
    class StageContext:
        # The class, annotated with @page_processor, that will be called on your worker for each page that reaches
        # this stage.
        name: str
        # Global arguments to pass into each page processor, such as configuration.  Many folks will use json to serialize.
        # Any arguments that need to vary per page should be included in your cursor or in the value returned by the
        # previous stage.
        args: Optional[str] = None
        # The start_to_close_timeout of the activity that runs this stage.
        # This should typically be within the drain allowance of the worker that runs it.  That
        # would allow your activity to finish in case of a graceful shutdown.
        timeout_seconds: int = 300
        # Overrides BatchOrchestratorInput.max_parallelism for this stage only.  Use it when, say, your fetch stage
        # can run wide but your write stage needs to be gentle with a database.
        max_parallelism: Optional[int] = None
        # Backpressure: the previous stage will stop launching new pages while this many pages are already waiting
        # here.  Without it, a fast stage will run far ahead of a slow one and pile up pages (and history) in front
        # of it.  None means unbounded.
        max_queued_pages: Optional[int] = None

    # The first stage of the pipeline.  In addition to processing a page, it is responsible for paginating.
    @dataclass(kw_only=True)
    class PageProcessorContext(StageContext):
        # The number of items per page, to process in series.  Choose an amount that you can comfortably
        # process within the page_timeout_seconds.
        page_size: int
        # The cursor, for example a database cursor, from which to start paginating.
        # Use this if you want to start a batch from a specific cursor such as where a previous run left off or if
        # you are dividing up a large dataset into multiple batches.
        # When sdk-python supports generics, we can add support for (serializable) cursor types here.
        first_cursor_str: str = ""

    @dataclass(kw_only=True)
    class BatchTrackerContext:
        # A Callable that is called periodically with a BatchOrchestratorProgress object.
        name: Optional[str] = None
        # Global arguents to pass into your batch tracker, such as configuration.  Many folks will use json to serialize.
        args: Optional[str] = None
        polling_interval_seconds: int = 300
        timeout_seconds: int = 270  # less than the polling interval

    # All stages of the pipeline, in the order pages flow through them.
    @property
    def stages(self) -> List[BatchOrchestratorInput.StageContext]:
        return [self.page_processor, *self.subsequent_stages]

    def max_parallelism_for(self, stage: BatchOrchestratorInput.StageContext) -> int:
        return stage.max_parallelism if stage.max_parallelism is not None else self.max_parallelism


# Per-stage detail within BatchOrchestratorProgress.
@dataclass
class StageProgress:
    # The stage's position in the pipeline; 0 is the page_processor.
    stage_num: int
    # The name of the @page_processor class running this stage.
    stage_name: str
    # Pages that have arrived at this stage but haven't started, e.g. because of max_parallelism.
    num_pending_pages: int
    num_processing_pages: int
    # Pages that finished *this* stage (and so have moved on to the next one, if any).
    num_completed_pages: int
    # Pages which are failing to process but are still being retried.
    num_stuck_pages: int
    # Pages which have permanently failed (perhaps because they raised a non_retryable error).
    num_failed_pages: int
    max_parallelism: int
    max_parallelism_achieved: int


# Provides a snapshot of how many pages the orchestrator has processed.  You can get this information in two ways.
# 1. You can [query](https://docs.temporal.io/dev-guide/python/features#send-query) the get_progress method on the BatchOrchestrator workflow handle from any client.
# 2. You can define a @batch_tracker callback and provide it in BatchOrchestratorInput.  The workflow will periodically
#    call your tracker.
#
# The top-level counts summarize the whole pipeline; see stages for a breakdown of where pages are.
@dataclass
class BatchOrchestratorProgress:
    # Summed across all stages.
    # TODO - report a list of stuck pages with exceptions
    num_stuck_pages: int
    # Summed across all stages: every page currently inside an activity, anywhere in the pipeline.
    num_processing_pages: int
    # Pages that made it all the way through the last stage of the pipeline.
    num_completed_pages: int
    # Summed across all stages.
    num_failed_pages: int
    is_finished: bool
    _start_timestamp: float
    # You can monitor this to ensure you are getting as much parallel processing as you hoped for.
    # This is the most pages that were ever in flight at one time across the entire pipeline, so with multiple
    # stages it can exceed max_parallelism.  See StageProgress.max_parallelism_achieved for per-stage numbers.
    max_parallelism_achieved: int
    # One entry per stage, in pipeline order.
    stages: List[StageProgress] = field(default_factory=list)

    # The second when the BatchOrchestrator workflow began executing.
    def start_time(self) -> datetime:
        return datetime.fromtimestamp(self._start_timestamp)

    # Pages which have started but not finished the pipeline: they are in some stage's queue or activity.
    @property
    def num_in_flight_pages(self) -> int:
        return sum(stage.num_pending_pages + stage.num_processing_pages for stage in self.stages)

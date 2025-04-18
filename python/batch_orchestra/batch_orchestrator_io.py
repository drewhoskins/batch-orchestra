from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

# This module concerns input and output (results/progress) for the BatchOrchestrator workflow.
from typing import Optional

from temporalio.common import RetryPolicy


def batch_orchestrator_input_default_initial_retry_policy():
    return RetryPolicy(maximum_attempts=10)


@dataclass(kw_only=True)
class BatchOrchestratorInput:
    # Configuration for the workhorse of your batch operation.
    page_processor: PageProcessorContext
    # Use this to manage load on your downstream dependencies such as DBs or APIs by limiting the number of pages
    # processed simultaneously.
    max_parallelism: int
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
    class PageProcessorContext:
        # The function, annotated with @page_processor, that will be called on your worker for each page
        name: str
        # The number of items per page, to process in series.  Choose an amount that you can comfortably
        # process within the page_timeout_seconds.
        page_size: int
        # Global arguments to pass into each page processor, such as configuration.  Many folks will use json to serialize.
        # Any arguments that need to vary per page should be included in your cursor.
        args: Optional[str] = None
        # The cursor, for example a database cursor, from which to start paginating.
        # Use this if you want to start a batch from a specific cursor such as where a previous run left off or if
        # you are dividing up a large dataset into multiple batches.
        # When sdk-python supports generics, we can add support for (serializable) cursor types here.
        first_cursor_str: str = ""
        # The start_to_close_timeout of the activity that runs your page processor.
        # This should typically be within the drain allowance of the worker that runs your page processor.  That
        # would allow your activity to finish in case of a graceful shutdown.
        timeout_seconds: int = 300

    @dataclass(kw_only=True)
    class BatchTrackerContext:
        # A Callable that is called periodically with a BatchOrchestratorProgress object.
        name: Optional[str] = None
        # Global arguents to pass into your batch tracker, such as configuration.  Many folks will use json to serialize.
        args: Optional[str] = None
        polling_interval_seconds: int = 300
        timeout_seconds: int = 270  # less than the polling interval


# Provides a snapshot of how many pages the orchestrator has processed.  You can get this information in two ways.
# 1. You can [query](https://docs.temporal.io/dev-guide/python/features#send-query) the get_progress method on the BatchOrchestrator workflow handle from any client.
# 2. You can define a @batch_tracker callback and provide it in BatchOrchestratorInput.  The workflow will periodically
#    call your tracker.
@dataclass
class BatchOrchestratorProgress:
    # Pages which are failing to process but are still being retried.
    # TODO - report a list of stuck pages with exceptions
    num_stuck_pages: int
    num_processing_pages: int
    num_completed_pages: int
    # Pages which have permanently failed (perhaps because they raised a non_retryable error).
    num_failed_pages: int
    is_finished: bool
    _start_timestamp: float
    # You can monitor this to ensure you are getting as much parallel processing as you hoped for.
    max_parallelism_achieved: int

    # The second when the BatchOrchestrator workflow began executing.
    def start_time(self) -> datetime:
        return datetime.fromtimestamp(self._start_timestamp)

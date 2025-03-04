from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
import inspect
import logging
from typing import Any, Optional
from enum import Enum
from batch_worker import BatchWorkerContext

from temporalio import activity
from temporalio.common import RetryPolicy

#
# batch_processor library
#
# This file contains the interfaces used by the processor you write to process a page of work.
# It works in conjunction with the batch_orchestrator library.
#
# Your page processor function:
#   * Will run on a Temporal worker as part of an activity.
#   * Must be registered with the @page_processor decorator to allow the batch orchestrator to find it safely.
#   * Must be async.
#   * Take a BatchProcessorContext as its only argument.
#   * It has two goals
#       1. To get the next page of work and enqueue it on the BatchOrchestrator
#       2. To process a page of work
#   * BatchPage represents a page.
#   * It will use the BatchProcessorContext class to access the cursor and any args passed to it.
#   * It must first call BatchProcessorContext.enqueue_next_page() to enqueue the next page of work before it processes the contents of the page.
#     this will allow pages to be executed in parallel.


class PageProcessor(ABC):
    def __init__(self) -> None:
        self._validate()

    class RetryMode(Enum):
        # (RECOMMENDED).  Your page processor should be idempotent or you should be comfortable with the possibility of it running more than once.
        # Please test the idempotency of your operation (ie run it twice in a test and make sure the output is the same both times).
        EXECUTE_AT_LEAST_ONCE = 1
        # Your page processor may not execute or may fail permanently.
        EXECUTE_AT_MOST_ONCE = 2

    # You must choose whether your page processor can be retried in the event of a failure or timeout.
    @property
    @abstractmethod
    def retry_mode(self):
        pass

    @abstractmethod
    async def run(self, context: BatchProcessorContext):
        pass

    # By default we retry ten times with exponential backoff, and then if it's still failing, we'll kick
    # it to the extended retry queue (see the comment on use_extended_retries)
    @property
    def initial_retry_policy(self):
        if self.retry_mode == PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE:
            return RetryPolicy(maximum_attempts=10)
        else:
            return RetryPolicy(maximum_attempts=1)

    # Extended retries happen once we've exhausted the initial_retry_policy for all pages in the batch.
    # They continue indefinitely at extended_retry_interval_seconds.
    # This prevents stuck pages from "gumming up" the queue (which has a max_parallelism) and blocking progress on other pages.
    # You can set this to false if you want to stop retrying after initial_retry_policy is exhausted.
    @property
    def use_extended_retries(self):
        return self.retry_mode == PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    @property
    def extended_retry_interval_seconds(self) -> int:
        # Having exhausted the (by default, 10) initial retries:
        # By default, retry five minutes after a failure or timeout, in perpetuity.
        # Choose whatever you want, balancing spamminess and cost with responsiveness of getting unstuck after (say) pushing a code patch.
        return 300

    def _validate(self):
        if self.retry_mode == PageProcessor.RetryMode.EXECUTE_AT_MOST_ONCE:
            if self.use_extended_retries:
                raise ValueError(
                    f"@page_processor {self.__class__.__name__}: You cannot set use_extended_retries for retry_mode EXECUTE_AT_MOST_ONCE."
                )
            if self.initial_retry_policy.maximum_attempts != 1:
                raise ValueError(
                    f"@page_processor {self.__class__.__name__}: You cannot set initial_retry_policy.maximum_attempts to anything other than 1 "
                    + f"(got {self.initial_retry_policy.maximum_attempts}) for retry_mode EXECUTE_AT_MOST_ONCE."
                )


# The Registry
# You must declare your page processor functions with the @page_processor to allow the batch orchestrator to find them safely.
_page_processor_registry = {}


def page_processor(page_processor_class):
    message = f"The @page_processor annotation must go on a class that subclasses batch_processor.PageProcessor, not: {page_processor_class}"
    assert inspect.isclass(page_processor_class), message
    assert issubclass(page_processor_class, PageProcessor), message
    _page_processor_registry[page_processor_class.__name__] = page_processor_class
    return page_processor_class


def list_page_processors():
    return list(_page_processor_registry.keys())


def get_page_processor(page_processor_class_name: str) -> PageProcessor:
    user_provided_page_processor = _page_processor_registry.get(page_processor_class_name)
    if not user_provided_page_processor:
        raise ValueError(
            f"You passed page_processor_name '{page_processor_class_name}' into the BatchOrchestrator, but it was not registered on "
            + "your worker. Please annotate a class inheriting from batch_processor.PageProcessor with @page_processor and make sure its module is imported. "
            + f"Available classes: {list_page_processors()}"
        )
    return user_provided_page_processor()


# In your batch jobs, you'll chunk them into pages of work that run in parallel with one another.
# Each page, represented by this class, processes in series.
# Choose a page size that can run in under five minutes.
@dataclass
class BatchPage:
    # Your cursor serialized as a string.  You might use json for example.
    # When sdk-python supports generics, we can add support for (serializable) cursor types here.
    cursor_str: str
    # The number of records to process.
    size: int


# convert batchPageProcessorName to a function and call it with the page
# Returns whatever the page processor returns, which should be serialized or serializable (perhaps using a temporal data converter)
@activity.defn
async def process_page(
    page_processor_class_name: str,
    batch_id: Optional[str],
    page: BatchPage,
    page_num: int,
    args: Optional[str],
    did_signal_next_page: bool,
) -> Any:
    context = await BatchProcessorContext(
        batch_id=batch_id,
        page=page,
        page_num=page_num,
        args=args,
        activity_info=activity.info(),
        did_signal_next_page=did_signal_next_page,
    ).async_init()

    user_provided_page_processor = get_page_processor(page_processor_class_name)
    return await user_provided_page_processor.run(context)


class LoggerAdapter(activity.LoggerAdapter):
    def __init__(self, context: BatchProcessorContext) -> None:
        self._batch_id: Optional[str] = None
        if context.has_batch_id():
            self._batch_id = context.batch_id
        super().__init__(logging.getLogger(__name__), {})

    def process(self, msg, kwargs):
        msg, kwargs = super().process(msg, kwargs)
        if self._batch_id is not None:
            extra_data = {"batch_id": self._batch_id}
            if "extra" in kwargs:
                kwargs["extra"].update(extra_data)
            else:
                kwargs["extra"] = extra_data
        return msg, kwargs


# This class is the only argument passed to your page processor function but contains everything you need.
class BatchProcessorContext(BatchWorkerContext):
    class NextPageSignaled(Enum):
        NOT_SIGNALED = 0
        INITIAL_PHASE = 1
        THIS_RUN = 2
        PREVIOUS_RUN = 3

    def __init__(
        self,
        *,
        batch_id: Optional[str],
        page: BatchPage,
        page_num: int,
        args: Optional[str],
        activity_info: activity.Info,
        did_signal_next_page: bool,
    ):
        super().__init__(activity_info)
        self._batch_id = batch_id
        self._page = page
        self._page_num = page_num
        self._args = args
        self._logger = LoggerAdapter(self)
        if did_signal_next_page:
            self._next_page_signaled = BatchProcessorContext.NextPageSignaled.INITIAL_PHASE
        elif "signaled_next_page" in activity.info().heartbeat_details:
            self._next_page_signaled = BatchProcessorContext.NextPageSignaled.PREVIOUS_RUN
        else:
            self._next_page_signaled = BatchProcessorContext.NextPageSignaled.NOT_SIGNALED

    # Prints to the Worker's logs.  If you are developing locally and want to see the logs, run the Worker in the foreground and with debug_mode=True.
    @property
    def logger(self):
        return self._logger

    @property
    def page(self) -> BatchPage:
        return self._page

    # Gets global, user-provided args passed in BatchOrchestratorInput.page_processor_args.
    # Any values that can differ per page should insted go into your cursor inside BatchPage.
    # Suggested usage: use JSON and deserialize the args into a dataclass.
    @property
    def args_str(self) -> str:
        result = self._args
        if result is None:
            raise ValueError(
                "You cannot use get_args because you did not pass any args into BatchOrchestratorInput.page_processor_args"
            )
        return result

    # The identifier for the batch, potentially across multiple workflows.
    #
    @property
    def batch_id(self) -> str:
        assert self._batch_id is not None, (
            "You're getting the batch ID but didn't pass one in to BatchOrchestratorInput.batch_id."
        )
        return self._batch_id

    # Checks whether you passed anything into BatchOrchestratorInput.batch_id when you created the workflow.
    def has_batch_id(self) -> bool:
        return self._batch_id is not None

    # Call this with your next cursor before you process the page to enqueue the next chunk on the BatchOrchestrator.
    async def enqueue_next_page(self, page: BatchPage) -> None:
        assert self._parent_workflow is not None, (
            "BatchProcessorContext.async_init() was never called.  This class should only be "
            + "instantiated by the batch-orchestra library."
        )
        assert self._next_page_signaled != BatchProcessorContext.NextPageSignaled.THIS_RUN, (
            "You cannot call enqueue_next_page twice in the same page_processor.  Each processed page "
            + "is responsible for enqueuing the following page."
        )

        # Minimize the chance of a re-signal when the activity fails and retries, by checking if we recorded that we already signaled.
        if self._did_signal_next_page():
            return

        await self._parent_workflow.signal(
            "_signal_add_page",  # use string instead of literal to avoid upward dependency between this file and batch_orchestrator.py
            args=[page, self._page_num + 1],
        )
        self._next_page_signaled = BatchProcessorContext.NextPageSignaled.THIS_RUN
        activity.heartbeat("signaled_next_page")

    def _did_signal_next_page(self) -> bool:
        return self._next_page_signaled != BatchProcessorContext.NextPageSignaled.NOT_SIGNALED

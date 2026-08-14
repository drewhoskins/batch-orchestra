# When a run of BatchOrchestrator gets a history that's too long, it will continue as new.
# This state is passed to the new run
from asyncio import Future
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from ..batch_processor import BatchPage


# One page of work as it sits in one stage of the pipeline.  A page gets a fresh EnqueuedPage for each stage
# it flows through.
@dataclass(kw_only=True)
class EnqueuedPage:
    page: BatchPage
    page_num: int
    # Which stage of the pipeline this page is waiting in or being processed by.
    stage_num: int = 0
    # Whatever the previous stage's page processor returned.  None for the first stage.
    stage_input: Optional[Any] = None
    did_signal_next_page: bool = False
    last_exception: Optional[BaseException] = None
    future: Optional[Future[Any]] = None

    def set_processing_started(self, future: Future[Any]) -> None:
        self.future = future

    def set_processing_finished(self) -> None:
        self.future = None

    def set_processing_got_stuck(self, exception: BaseException, did_signal_next_page: bool) -> None:
        self.last_exception = exception
        self.future = None
        self.did_signal_next_page = did_signal_next_page

    # Hand this page off to the next stage, carrying the result of this stage as its input.
    def to_next_stage(self, stage_input: Any) -> "EnqueuedPage":
        return EnqueuedPage(
            page=self.page,
            page_num=self.page_num,
            stage_num=self.stage_num + 1,
            stage_input=stage_input,
        )

    @property
    def is_stuck(self) -> bool:
        return self.last_exception is not None


# The bookkeeping for a single stage's queue.
@dataclass(kw_only=True)
class PageTrackerData:
    max_parallelism: int
    num_pages_ever_enqueued: int = 0
    num_completed_pages: int = 0
    max_parallelism_achieved: int = 0
    pending_page_nums: List[int] = field(default_factory=list)
    processing_page_nums: Set[int] = field(default_factory=set)
    stuck_page_nums: Set[int] = field(default_factory=set)
    failed_page_nums: Set[int] = field(default_factory=set)
    previous_max_parallelisms: List[int] = field(default_factory=list)


# Everything one stage of the pipeline needs to pick up where it left off.
@dataclass(kw_only=True)
class StageState:
    page_tracker_data: PageTrackerData
    pages: Dict[str, EnqueuedPage]


# Pipeline-wide bookkeeping that isn't owned by any one stage.
@dataclass(kw_only=True)
class PipelineData:
    max_parallelism_achieved: int = 0
    is_finished: bool = False


@dataclass(kw_only=True)
class ContinueAsNewState:
    pipeline_data: PipelineData
    # One entry per stage, in pipeline order.
    stages: List[StageState]

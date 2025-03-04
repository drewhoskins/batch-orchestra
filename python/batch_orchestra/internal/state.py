# When a run of BatchOrchestrator gets a history that's too long, it will continue as new.
# This state is passed to the new run
from asyncio import Future
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from ..batch_processor import BatchPage


@dataclass(kw_only=True)
class EnqueuedPage:
    page: BatchPage
    page_num: int
    did_signal_next_page: bool = False
    last_exception: Optional[BaseException] = None
    future: Optional[Future] = None

    def set_processing_started(self, future: Future) -> None:
        self.future = future

    def set_processing_finished(self) -> None:
        self.future = None

    def set_processing_got_stuck(self, exception: BaseException, did_signal_next_page: bool) -> None:
        self.last_exception = exception
        self.future = None
        self.did_signal_next_page = did_signal_next_page

    @property
    def is_stuck(self) -> bool:
        return self.last_exception is not None


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
    is_finished: bool = False
    previous_max_parallelisms: List[int] = field(default_factory=list)


@dataclass(kw_only=True)
class ContinueAsNewState:
    page_tracker_data: Optional[PageTrackerData]
    pages: Dict[str, EnqueuedPage]

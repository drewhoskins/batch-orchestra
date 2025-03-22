from __future__ import annotations

import inspect
from typing import Any, Optional

from temporalio import activity

from .batch_processor import BatchPage, BatchProcessorContext
from .batch_reduce import PageReducer

# The Registry
# You must declare your page processor functions with the @page_processor to allow the batch orchestrator to find them safely.
_page_reducer_registry = {}


def page_reducer(page_reducer_class: type[PageReducer]):
    message = f"The @page_reducer annotation must go on a class that subclasses batch_processor.PageReducer, not: {page_reducer_class}"
    assert inspect.isclass(page_reducer_class), message
    assert issubclass(page_reducer_class, PageReducer), message
    _page_reducer_registry[page_reducer_class.__name__] = page_reducer_class
    return page_reducer_class


def list_page_reducers():
    return list(_page_reducer_registry.keys())


def get_page_reducer(page_reducer_class_name: str) -> PageReducer:
    user_provided_page_reducer = _page_reducer_registry.get(page_reducer_class_name)
    if not user_provided_page_reducer:
        raise ValueError(
            f"You passed page_processor_name '{page_reducer_class_name}' into the BatchOrchestrator, but it was not registered on "
            + "your worker. Please annotate a class inheriting from batch_processor.PageReducer with @page_reducer and make sure its module is imported. "
            + f"Available classes: {list_page_reducers()}"
        )
    return user_provided_page_reducer()


# convert batchPageProcessorName to a function and call it with the page
# Returns whatever the page processor returns, which should be serialized or serializable (perhaps using a temporal data converter)
@activity.defn
async def reduce_page(
    page_reducer_class_name: str,
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

    user_provided_page_reducer = get_page_reducer(page_reducer_class_name)
    return await user_provided_page_reducer.reduce_batch(context)

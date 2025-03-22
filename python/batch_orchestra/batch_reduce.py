"""
A reduce workflow performed as pages come back with some given window size. For example, a window size of 10 means that the reduce activity is
called with batches of 10 items at a time. This allows you to begin reducing early, provides checkpointing. To implement, you must define a
single function `reduce` which is called with a serializable accumulator and a batch of items from the map step.

# PITFALLS

If you are collecting the data with a static side, an excessively large accumulation (such as collecting all the results) will result in
outsized RAM consumption, as you end up with a triangle number where each batch stores each previous value under accumulation.

# OPTIONS

- define an encoder to store the data in some memory store instead of commiting it to the
  workflow history
- do not checkpoint the reduce step by specifying a window size of `math.inf`
- stream the data via some queue (kafka etc) which an activity can consume. the activity
  does not acknowledge the messages until after the data completes

Tools to do this are provided in this module.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(kw_only=True)
class PageReducer[Input, Output](ABC):
    """The number of entries to process at a time. Defaults to `None` which implies reduce is run once at the end of the job."""

    window_size: int | None = None

    @abstractmethod
    def base_case(self) -> Any:
        pass

    @abstractmethod
    async def reduce_batch(self, acc: Output, batch: list[Input]) -> Output:
        """
        Called for every `window_size` elements.
        """
        pass

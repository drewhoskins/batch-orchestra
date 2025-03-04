from batch_processor_reduce import page_reducer
from batch_reduce import PageReducer


@page_reducer
class MyBatchReduce(PageReducer[int, int]):
    def base_case(self):
        return 0

    async def reduce_batch(self, acc: int, batch: list[int]) -> int:
        for item in batch:
            acc += item
        return acc

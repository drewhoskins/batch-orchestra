from typing import Any

from temporalio import workflow

from .batch_orchestrator_io import BatchOrchestratorInput
from .batch_processor_reduce import get_page_reducer
from .batch_reduce import PageReducer


# Sandbox is off so we can import the user's @page_reducer classes.  We might could make this more selective
# (and therefore safe) by allowing the user to specify modules to import.
@workflow.defn(sandboxed=False)
class BatchOrchestratorReduce:
    def __init__(self):
        self.is_done = False

    @workflow.run
    async def run(self, input: BatchOrchestratorInput.PageReducerContext):
        self.input = input
        self.page_reducer: PageReducer = get_page_reducer(input.name)
        self.state = self.page_reducer.base_case()

        await workflow.wait_condition(lambda: self.is_done)

        return self.state

    @workflow.signal
    async def update(self, data: list[Any]):
        self.state = await self.page_reducer.reduce_batch(acc=self.state, batch=data)

    @workflow.signal
    async def done(self):
        self.is_done = True

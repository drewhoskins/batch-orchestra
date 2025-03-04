from temporalio import workflow


class ReduceHandle[T, U]:
    def __init__(self, handle: workflow.ChildWorkflowHandle):
        self.handle = handle

    async def send(self, data: list[T]):
        await self.handle.signal("update", data)

    async def done(self):
        await self.handle.signal("done")
        return await self.handle

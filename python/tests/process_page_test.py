import pytest
from temporalio.testing import ActivityEnvironment

import batch_orchestrator
from batch_orchestrator_page import BatchOrchestratorPage, MyCursor
from batch_page_processor_context import BatchPageProcessorContext
from batch_page_processor_registry import page_processor

@page_processor
async def returns_cursor(context: BatchPageProcessorContext):
    return context.getPage().cursor.i


@pytest.mark.asyncio
async def test_page_processor():
    env = ActivityEnvironment()
    result = await env.run(batch_orchestrator.process_page, 'returns_cursor', BatchOrchestratorPage(MyCursor(5), 10))
    assert result == 5

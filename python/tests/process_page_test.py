from __future__ import annotations
import json

import pytest
from temporalio.testing import ActivityEnvironment

import batch_orchestrator
from batch_processor import BatchProcessorContext, BatchPage, page_processor

@page_processor
async def returns_cursor(context: BatchProcessorContext):
    assert context.get_args() == "some_args"
    return context.get_page().cursor_str


@pytest.mark.asyncio
async def test_page_processor():
    env = ActivityEnvironment()
    result = await env.run(batch_orchestrator.process_page, returns_cursor.__name__, BatchPage("some_cursor", 10), "some_args")
    assert result == "some_cursor"

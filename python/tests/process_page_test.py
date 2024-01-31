from __future__ import annotations
from dataclasses import dataclass
from unittest.mock import patch

import pytest
from temporalio.client import WorkflowHandle
from temporalio.testing import ActivityEnvironment
from temporalio.activity import info
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


@page_processor
async def starts_new_page(context: BatchProcessorContext):
    await context.enqueue_next_page(
        context.get_page().cursor_str + "_the_second")

async def on_signal(parent_workflow, signal: str, cursor):
    assert signal == "signal_add_page"
    assert cursor == "some_cursor_the_second"

@pytest.mark.asyncio
async def test_signal():
    with patch.object(WorkflowHandle, 'signal', new=on_signal) as signal_mock:
        env = ActivityEnvironment()
        result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, BatchPage("some_cursor", 10), "some_args")

expected_heartbeat_details = "signaled_next_page"

def on_heartbeat(details):
    assert details == expected_heartbeat_details

@pytest.mark.asyncio
async def test_heartbeat():
    with patch.object(WorkflowHandle, 'signal', new=on_signal) as signal_mock:
        env = ActivityEnvironment()
        env.on_heartbeat = on_heartbeat
        result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, BatchPage("some_cursor", 10), "some_args")

@pytest.mark.asyncio
async def test_idempotency():
    # Simulate the first time the page is processed and we should signal the workflow
    with patch.object(WorkflowHandle, 'signal') as signal_mock:
            env = ActivityEnvironment()
            result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, BatchPage("some_cursor", 10), "some_args")
        
            signal_mock.assert_called_once()

    # And the second time, we shouldn't.
    with patch.object(WorkflowHandle, 'signal') as signal_mock, patch('temporalio.activity.info') as mock_activity_info:
            instance = mock_activity_info.return_value
            instance.heartbeat_details = [expected_heartbeat_details]
            env = ActivityEnvironment()
            result = await env.run(batch_orchestrator.process_page, starts_new_page.__name__, BatchPage("some_cursor", 10), "some_args")
        
            signal_mock.assert_not_called()

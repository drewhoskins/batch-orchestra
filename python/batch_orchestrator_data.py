from typing import Optional
from dataclasses import dataclass, asdict
import dataclasses
from datetime import timedelta
import json
from typing import Any, Dict, List, Optional, Set, Type

import temporalio.converter
from temporalio.converter import (
    CompositePayloadConverter,
    DefaultPayloadConverter,
    EncodingPayloadConverter
)
from temporalio.api.common.v1 import Payload
from temporalio.common import RetryPolicy

@dataclass(kw_only=True)
class BatchOrchestratorInput:
    batch_name: str
    # The function, annotated with @page_processor, that will be called on your worker for each page
    page_processor: str
    # Use this to manage load on your downstream dependencies such as DBs or APIs by limiting the number of pages
    # processed simultaneously.
    max_parallelism: int
    # The number of items per page, to process in series.  Choose an amount that you can comfortably
    # process within the page_timeout_seconds.
    page_size: int
    # The start_to_close_timeout of the activity that runs your page processor.
    # This should typically be within the drain allowance of the worker that runs your page processor.  That 
    # would allow your activity to finish in case of a graceful shutdown.
    page_timeout_seconds: int = 300
    # The cursor, for example a database cursor, from which to start paginating.
    # Use this if you want to start a batch from a specific cursor such as where a previous run left off or if
    # you are dividing up a large dataset into multiple batches.
    # When sdk-python supports generics, we can add support for (serializable) cursor types here.
    first_cursor_str: str = ""
    # Global arguments to pass into each page processor, such as configuration.  Many will use json to serialize.
    # Any arguments that need to vary per page should be included in your cursor.
    page_processor_args: Optional[str] = None
    # By default we retry ten times with exponential backoff, and then if it's still failing, we'll kick
    # it to the extended retry queue which will continue to retry indefinitely once the working pages are finished.
    # This is to avoid the queue--which maxes out at max_parallelism concurrent page processors--getting filled up
    # with failing pages for a long time and blocking progress on other pages.
    initial_retry_policy: RetryPolicy = RetryPolicy(maximum_attempts=10)
    use_extended_retries: bool = True
    # By default, retry every five minutes in perpetuity.
    extended_retry_interval_seconds: int = 300

@dataclass
class BatchOrchestratorResults:
    num_pages_processed: int
    # You can monitor this to ensure you are getting as much parallel processing as you hoped for.
    max_parallelism_achieved: int
    num_failed_pages: int


class BatchOrchestratorEncodingPayloadConverter(EncodingPayloadConverter):
    @property
    def encoding(self) -> str:
        return "text/batch-orchestrator-encoding"

    def to_payload(self, value: Any) -> Optional[Payload]:
        if isinstance(value, BatchOrchestratorInput):
            dict_value = asdict(value)
            if dict_value["initial_retry_policy"]["initial_interval"]:
                dict_value["initial_retry_policy"]["initial_interval"] = dict_value["initial_retry_policy"]["initial_interval"].total_seconds()
            if dict_value["initial_retry_policy"]["maximum_interval"]:
                dict_value["initial_retry_policy"]["maximum_interval"] = dict_value["initial_retry_policy"]["maximum_interval"].total_seconds()

            return Payload(
                metadata={"encoding": self.encoding.encode()},
                data=json.dumps(dict_value).encode(),
            )
        else:
            return None

    def from_payload(self, payload: Payload, type_hint: Optional[Type] = None) -> Any:
        # TODO(drewhoskins) why is this assert not working?  And how can I ensure that this dataconverter doesn't get used for other types?
        # assert not type_hint or type_hint is BatchOrchestratorInput
        decoded_results = json.loads(payload.data.decode())
        if decoded_results["initial_retry_policy"]["initial_interval"]:
            decoded_results["initial_retry_policy"]["initial_interval"] = timedelta(seconds=decoded_results["initial_retry_policy"]["initial_interval"])
        if decoded_results["initial_retry_policy"]["maximum_interval"]:
            decoded_results["initial_retry_policy"]["maximum_interval"] = timedelta(seconds=decoded_results["initial_retry_policy"]["maximum_interval"])
        decoded_results["initial_retry_policy"] = RetryPolicy(**decoded_results["initial_retry_policy"])
        return BatchOrchestratorInput(**decoded_results)


class BatchOrchestratorPayloadConverter(CompositePayloadConverter):
    def __init__(self) -> None:
        # Just add ours as first before the defaults
        super().__init__(
            BatchOrchestratorEncodingPayloadConverter(),
            # TODO(drewhoskins): Update this to make use of fix for https://github.com/temporalio/sdk-python/issues/139
            *DefaultPayloadConverter().converters.values(),
        )

# Use the default data converter, but change the payload converter.
batch_orchestrator_data_converter = dataclasses.replace(
    temporalio.converter.default(),
    payload_converter_class=BatchOrchestratorPayloadConverter,
)

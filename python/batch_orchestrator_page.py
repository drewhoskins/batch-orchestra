from dataclasses import dataclass
from typing import Generic, TypeVar


# In your batch jobs, you'll chunk them into pages of work that run in parallel with one another. 
# Each page, represented by this class, processes in series.
# Choose a page size that can run in under five minutes.
@dataclass
class BatchOrchestratorPage:
    # Your cursor serialized as a string.  You might use json for example.
    # When sdk-python supports generics, you'll be able to use your cursor type here.
    cursor: str
    page_size: int

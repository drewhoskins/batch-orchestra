from dataclasses import dataclass
from typing import Generic, TypeVar


TCursor = TypeVar("TCursor")

@dataclass
class MyCursor:
    i: int

# In your batch jobs, you'll chunk them into pages of work that run in parallel with one another. 
# Each page, represented by this class, processes in series.
# Choose a page size that can run in under five minutes.
@dataclass
class BatchOrchestratorPage:
    cursor: MyCursor
    pageSize: int

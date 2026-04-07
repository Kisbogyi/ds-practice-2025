import asyncio
from types import CoroutineType
from typing import Any, Dict, Literal

# TODO locking
class OrderResult:
    def __init__(self):
        self.completion_event: asyncio.Event = asyncio.Event()
        self.transaction_passed: bool = None
        self.verefication_passed: bool = None
        self.suggestions: Dict = None
        self.error: Exception = None
        self.vc: Dict = {}
    
    def _check_compleation(self):
        if self.error or self.verefication_passed and \
                self.verefication_passed and \
                self.suggestions is not None:
            self.completion_event.set()  # finish only when all stages are finished

    def fail(self, error: Exception):
        self.error = error
        self._check_compleation()

    def pass_verefication(self):
        self.verefication_passed = True
        self._check_compleation()

    def pass_transaction(self):
        self.transaction_passed = True
        self._check_compleation()

    def set_suggestions(self, suggestions: Dict):
        self.suggestions = suggestions if suggestions is not None else {}
        self._check_compleation()

    async def wait(self) -> Literal[True]:
        return await self.completion_event.wait()

    def has_errors(self) -> bool:
        return self.error is not None

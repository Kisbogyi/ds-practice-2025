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

    def _merge_clocks(self, incoming_vc: Dict[str, int]) -> Dict[str, int]:
        for k, v in incoming_vc:
            self.vc[k] = max(self.vc.get(k, 0), v)
    
    def _check_compleation(self):
        if self.error or self.verefication_passed and \
                self.verefication_passed and \
                self.suggestions is not None:
            self.completion_event.set()  # finish only when all stages are finished

    def fail(self, error: Exception):
        self.error = error
        self._check_compleation()

    def pass_verefication(self, incoming_vc: Dict):
        self.verefication_passed = True
        self._merge_clocks(incoming_vc)
        self._check_compleation()

    def pass_transaction(self, incoming_vc: Dict):
        self.transaction_passed = True
        self._merge_clocks(incoming_vc)
        self._check_compleation()

    def set_suggestions(self, incoming_vc: Dict, suggestions: Dict):
        self.suggestions = suggestions if suggestions is not None else {}
        self._merge_clocks(incoming_vc)
        self._check_compleation()

    def wait(self) -> CoroutineType[Any, Any, Literal[True]]:
        return self.completion_event.wait()

    def has_errors(self) -> bool:
        return self.error is not None

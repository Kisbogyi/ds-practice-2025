import asyncio
from typing import Dict, Literal
import logging

# TODO locking
logger = logging.getLogger(__name__)

class OrderResult:
    def __init__(self):
        self.completion_event: asyncio.Event = asyncio.Event()
        self.transaction_passed: bool = False
        self.verification_passed: bool = False
        self.suggestions: list = None
        self.error: Exception | None = None
        self.vc: Dict = {}

    def _check_compleation(self):
        all_tasks_successful = (
            self.verification_passed and
            self.transaction_passed and
            self.suggestions is not None
        )
        if self.error or all_tasks_successful:
            logger.info("Subtasks complete! Triggering event...")
            self.completion_event.set()

    def fail(self, error: Exception):
        self.error = error
        self._check_compleation()

    def pass_verefication(self):
        self.verification_passed = True
        self._check_compleation()

    def pass_transaction(self):
        self.transaction_passed = True
        self._check_compleation()

    def set_suggestions(self, suggestions: list):
        self.suggestions = suggestions if suggestions is not None else []
        self._check_compleation()

    async def wait(self) -> Literal[True]:
        return await self.completion_event.wait()

    def has_errors(self) -> bool:
        return self.error is not None

from typing import Any, Dict, List
import asyncio
from broadcast import broadcast

VECTOR_CLOCK = "vector_clock"
TARGET_CLOCK = "target_clock"

# TODO double check locking
# TODO const services + vc to array


class OrderStateManager:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.order_store: Dict[str, dict] = {}
        self.locks: Dict[str, asyncio.Lock] = {}
        self.global_lock = asyncio.Lock()
        self.services = [
            "orchestrator",
            "verification_service",
            "fraud_detectiion_service",
            "suggestions_service",
        ]  # FIXME switch to int or some config via docker ???
        try:
            self.service_idx = self.services.index(service_name)
        except ValueError:
            raise ValueError(
                f"Service {service_name} not found in services list")

    async def _get_lock(self, order_id: str, create: bool = False) -> asyncio.Lock:
        async with self.global_lock:
            if order_id not in self.locks:
                if not create:
                    return self.global_lock
                self.locks[order_id] = asyncio.Lock()
            return self.locks[order_id]

    def _init_vc(self) -> List[int]:
        return [0] * len(self.services)

    def _increment_clock(self, vc: List[int], ticks=1):
        vc[self.service_idx] += ticks

    def merge_clocks(self, *clocks: List[int]) -> List[int]:
        valid_clocks = [c for c in clocks if c is not None and len(
            c) == len(self.services)]
        if not valid_clocks:
            return self._init_vc()
        return [max(vals) for vals in zip(*clocks)]

    async def store_data(self, order_id: str, order_data: Dict[str, Any], target_vc: List[int] = None):
        async with self._get_lock(order_id, create=True):
            if order_id not in self.order_store:
                order_data[VECTOR_CLOCK] = self._init_vc()
                order_data[TARGET_CLOCK] = target_vc if target_vc else self._init_vc()
                self.order_store[order_id] = order_data

    async def get_final_vc(self, order_id: str, ticks: int) -> List[int]:
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                vc = list(self.order_store[order_id][TARGET_CLOCK])
                self._increment_clock(vc, ticks)
                return vc
            raise KeyError(f"Order {order_id} not found")

    async def is_vc_triggered(self, order_id: str, incoming_vc: List[int], tick: int) -> bool:
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                order = self.order_store[order_id]
                vc = self.merge_clocks(order[VECTOR_CLOCK], incoming_vc)
                target_vc = order[TARGET_CLOCK]
                if tick == 0:
                    return all(v == t for v, t in zip(vc, target_vc))
                else:
                    return vc[self.service_idx] == (target_vc[self.service_idx] + tick)
            return False

    async def get_data(self, order_id: str):
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                return dict(self.order_store[order_id])
            raise KeyError(f"Order {order_id} not found")

    async def clear_data(self, order_id: str, incoming_vc: List[int]) -> bool:
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                vc = self.order_store[order_id][VECTOR_CLOCK]
                is_valid = all(s <= i for s, i in zip(vc, incoming_vc))
                del self.order_store[order_id]
                del self.locks[order_id]
                return is_valid
            return False

    async def process_event(self, order_id: str, incoming_vc: List[int] = None):
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                order = self.order_store[order_id]
                order[VECTOR_CLOCK] = self.merge_clocks(
                        order[VECTOR_CLOCK], incoming_vc)
                self._increment_clock(order[VECTOR_CLOCK])
                broadcast(order_id, order[VECTOR_CLOCK])

    async def get_vc(self, order_id: str) -> List[int]:
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                return self.order_store[order_id][VECTOR_CLOCK]
            raise KeyError(f"Order {order_id} not found")

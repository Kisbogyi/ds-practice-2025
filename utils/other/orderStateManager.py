from typing import Any, Dict, List
import asyncio
from broadcast_service import broadcast

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
        lock = None
        async with self.global_lock:
            if order_id not in self.locks:
                if create:
                    lock = self.locks[order_id] = asyncio.Lock()
            else:
                lock = self.locks[order_id]
        return lock

    def _init_vc(self) -> List[int]:
        return [0] * len(self.services)

    def _increment_clock(self, vc: List[int], ticks=1):
        vc[self.service_idx] += ticks

    def merge_clocks(self, *clocks: List[int]) -> List[int]:
        valid_clocks = [c for c in clocks if c is not None and len(
            c) == len(self.services)]
        if not valid_clocks:
            return self._init_vc()
        return [max(vals) for vals in zip(*valid_clocks)]

    async def store_data(self, order_id: str, order_data: Dict[str, Any], target_vc: List[int] = None):
        lock = await self._get_lock(order_id, create=True)
        if lock is not None:
            async with lock:
                order_data[VECTOR_CLOCK] = self._init_vc()
                order_data[TARGET_CLOCK] = target_vc if target_vc else self._init_vc()
                self.order_store[order_id] = order_data

    async def get_final_vc(self, order_id: str, ticks: int) -> List[int]:
        lock = await self._get_lock(order_id)
        if lock is not None:
            async with lock:
                vc = list(self.order_store[order_id][TARGET_CLOCK])
                self._increment_clock(vc, ticks)
                return vc
        raise KeyError(f"Order {order_id} not found")

    async def is_vc_triggered(self, order_id: str, incoming_vc: List[int], tick: int) -> bool:
        lock = await self._get_lock(order_id)
        if lock is not None:
            async with lock:
                order = self.order_store[order_id]
                vc = self.merge_clocks(order[VECTOR_CLOCK], incoming_vc)
                target_vc = order[TARGET_CLOCK]
                if tick == 0:
                    return all(v == t for v, t in zip(vc, target_vc))
                else:
                    return incoming_vc[self.service_idx] == (target_vc[self.service_idx] + tick)
        return False

    async def get_data(self, order_id: str):
        lock = await self._get_lock(order_id)
        if lock is not None:
            async with lock:
                return dict(self.order_store[order_id])
        raise KeyError(f"Order {order_id} not found")

    async def clear_data(self, order_id: str, incoming_vc: List[int] = None) -> bool:
        lock = await self._get_lock(order_id)
        if lock is not None:
            async with lock:
                vc = self.order_store[order_id][VECTOR_CLOCK]
                is_valid = incoming_vc is None or all(s <= i for s, i in zip(vc, incoming_vc))
                del self.order_store[order_id]
                del self.locks[order_id]
                return is_valid
            return False

    async def process_event(self, order_id: str, incoming_vc: List[int] = None):
        lock = await self._get_lock(order_id)
        if lock is not None:
            async with lock:
                order = self.order_store[order_id]
                order[VECTOR_CLOCK] = self.merge_clocks(
                        order[VECTOR_CLOCK], incoming_vc)
                self._increment_clock(order[VECTOR_CLOCK])
                await broadcast(order_id, order[VECTOR_CLOCK])

    async def get_vc(self, order_id: str) -> List[int]:
        lock = await self._get_lock(order_id)
        if lock is not None:
            async with lock:
                return self.order_store[order_id][VECTOR_CLOCK]
        raise KeyError(f"Order {order_id} not found")

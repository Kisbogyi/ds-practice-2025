from typing import Any, Dict
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
        self.target_vcs: Dict[str, asyncio.Lock] = {}
        self.services = [
            "orchestrator",
            "verification_service",
            "fraud_detectiion_service",
            "suggestions_service",
        ]  # FIXME switch to int or some config via docker ???

    def _get_lock(self, order_id: str) -> asyncio.Lock:
        if order_id not in self.locks:
            self.locks[order_id] = asyncio.Lock()
        return self.locks[order_id]

    def _init_vector_clock(self) -> Dict[str, int]:
        return {service: 0 for service in self.services}

    def _increment_clock(self, vc: Dict[str, int]):
        vc[self.service_name] += 1

    def _merge_clocks(self, *clocks: Dict[str, int]) -> Dict[str, int]:
        merged = {}
        for service in self.services:
            merged[service] = max(clock.get(service, 0) for clock in clocks)
        return merged

    async def store_data(self, order_id: str, order_data: Dict[str, Any], target_vc: Dict[str, int] = None):
        async with self._get_lock(order_id):
            if order_id not in self.order_store:
                order_data[VECTOR_CLOCK] = self._init_vector_clock()
                target_vc[TARGET_CLOCK] = target_vc
                self.order_store[order_id] = order_data

    async def match_target_vc(self, order_id: str, incoming_vc: Dict[str, int]):
        async with self._get_lock(order_id):
            for k, v in self.target_vcs[order_id]:
                if incoming_vc[k] != v:
                    return False
            return True

    async def get_data(self, order_id: str, order_data: Dict[str, Any], incoming_vc: Dict[str, int] = None):
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                return dict(self.order_store[order_id])
            raise KeyError(f"Order {order_id} not found")

    async def clear_data(self, order_id: str, incoming_vc: Dict[str, int]):
        async with self._get_lock(order_id):
            if order_id in self.target_vcs:
                del self.target_vcs[order_id]
            if order_id in self.locks:
                del self.locks[order_id]
            if order_id in self.order_store:
                order = self.order_store.pop(order_id)
                for s in self.services:
                    if order[s] > incoming_vc[s]:
                        return False
                return True
        return False

    async def process_event(self, order_id: str, incoming_vc: Dict[str, int] = None):
        async with self._get_lock(order_id):
            order = self.order_store[order_id]
            order[VECTOR_CLOCK] = self._merge_clocks(
                order[VECTOR_CLOCK], incoming_vc)
            self._increment_clock(order[VECTOR_CLOCK])
            broadcast(order_id, order[VECTOR_CLOCK])

    async def get_vector_clock(self, order_id: str) -> Dict[str, int]:
        if order_id in self.order_store:
            return self.order_store[order_id][VECTOR_CLOCK]
        raise KeyError(f"Order {order_id} not found")

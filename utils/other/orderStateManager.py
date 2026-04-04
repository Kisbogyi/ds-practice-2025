from typing import Any, Dict
import asyncio

VECTOR_CLOCK = "vector_clock"

# TODO double check locking
class OrderStateManager:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.order_store: Dict[str, dict] = {}
        self.locks: Dict[str, asyncio.Lock] = {}
        self.services = [
            "orchestrator",
            "verification_service",
            "payment_service",
            "inventory_service",
        ]  # FIXME switch to int or some config via docker ???

    def _get_lock(self, order_id: str) -> asyncio.Lock:
        if order_id not in self.locks:
            self.locks[order_id] = asyncio.Lock()
        return self.locks[order_id]

    def _init_vector_clock(self) -> Dict[str, int]:
        return {service: 0 for service in self.services}

    def _increment_clock(self, vc: Dict[str, int]):
        vc[self.service_name] += 1

    def _merge_clocks(self, local_vc: Dict[str, int], incoming_vc: Dict[str, int]) -> Dict[str, int]:
        merged = {}
        for service in self.services:
            merged[service] = max(local_vc.get(
                service, 0), incoming_vc.get(service, 0))
        return merged

    async def get_or_create_order(self, order_id: str, incoming_vc: Dict[str, int], order_data: Dict[str, Any]):
        async with self._get_lock(order_id):
            if order_id not in self.order_store:
                order_data[VECTOR_CLOCK] = incoming_vc if incoming_vc is not None else self._init_vector_clock()
                self.order_store[order_id] = order_data
            else:
                self.order_store[order_id][VECTOR_CLOCK] = self._merge_clocks(
                    self.order_store[order_id][VECTOR_CLOCK], incoming_vc)
                # for key, value in order_data.items():
                #     if key != VECTOR_CLOCK:
                #         self.order_store[order_id][key] = value
            # return copy to prevent mutations
            return dict(self.order_store[order_id])

    async def process_event(self, order_id: str, incoming_vc: Dict[str, int] = None, order_data: Dict[str, Any] = None):
        await self.get_or_create_order(order_id, incoming_vc, order_data)
        async with self._get_lock(order_id):
            order = self.order_store[order_id]
            self._increment_clock(order[VECTOR_CLOCK])
            return dict(order)  # return copy to prevent mutations

    async def get_vector_clock(self, order_id: str) -> Dict[str, int]:
        if order_id in self.order_store:
            return self.order_store[order_id][VECTOR_CLOCK]
        raise KeyError(f"Order {order_id} not found")

    async def clear_order(self, order_id: str, incoming_vc: Dict[str, int]):
        # TODO verefy correctnes of incoming_vc if not report (bonus week 5)
        async with self._get_lock(order_id):
            if order_id in self.order_store:
                del self.order_store[order_id]
            if order_id in self.locks:
                del self.locks[order_id]

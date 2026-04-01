from typing import Any, Dict

VECTOR_CLOCK = "vector_clock"


class OrderStateManager:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.order_store: Dict[str, dict] = {}
        self.services = [
            "verification_service",
            "payment_service",
            "inventory_service",
        ]  # FIXME switch to int or some config via docker ???

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

    def get_or_create_order(self, order_id: str, **order: Dict[str, Any]):
        if order_id not in self.order_store:
            order[VECTOR_CLOCK] = self._init_vector_clock()
            self.order_store[order_id] = order
        return self.order_store[order_id]

    def process_event(self, order_id: str, incoming_vc: Dict[str, int] = None, **order: Dict[str, Any]):
        order = self.get_or_create_order(order_id, **order)
        if incoming_vc:
            order[VECTOR_CLOCK] = self._merge_clocks(
                order[VECTOR_CLOCK], incoming_vc)
        self._increment_clock(order[VECTOR_CLOCK])
        return order

    def get_vector_clock(self, order_id: str) -> Dict[str, int]:
        return self.order_store[order_id][VECTOR_CLOCK]

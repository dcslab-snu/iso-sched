# coding: UTF-8

import logging
from typing import Dict, List, Sized, Type

from isolating_controller.isolation.policies import IsolationPolicy
from isolating_controller.workload import Workload


class PendingQueue(Sized):
    def __init__(self, policy_type: Type[IsolationPolicy], max_pending: int) -> None:
        self._policy_type: Type[IsolationPolicy] = policy_type
        self._max_pending: int = max_pending

        self._cur_ready: int = 0
        self._ready_q: Dict[int, List[Workload]] = dict()  # key: socket id, value: workloads
        self._pending_list: List[IsolationPolicy] = list()

    def __len__(self) -> int:
        return len(tuple(
                filter(lambda x: len(x.foreground_workload.metrics) > 0 and len(x.background_workload.metrics) > 0,
                       self._pending_list)))

    def add(self, workload: Workload) -> None:
        logger = logging.getLogger(__name__)
        logger.debug(f'self._cur_ready: {self._cur_ready}')

        self._ready_q[workload.cur_socket_id()].append(workload)
        self._cur_ready += 1
        if self._cur_ready == self._max_pending:
            self._dump_to_pending_list()

    def pop(self) -> IsolationPolicy:
        if len(self) is 0:
            raise IndexError(f'{self} is empty')
        return self._pending_list.pop()

    def _dump_to_pending_list(self) -> None:
        logger = logging.getLogger(__name__)
        logger.debug('Dumping workloads to pending list!')

        for socket_id, workloads in self._ready_q.items():
            # FIXME: hard coded
            if len(workloads) is 2 and workloads[0].wl_type != workloads[1].wl_type:
                if workloads[0].wl_type == 'fg':
                    fg = workloads[0]
                    bg = workloads[1]
                else:
                    fg = workloads[1]
                    bg = workloads[0]
                new_group = self._policy_type(fg, bg)
                self._pending_list.append(new_group)

    def update_max_pending(self, new_max_pending: int):
        self._max_pending = new_max_pending

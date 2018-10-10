# coding: UTF-8

import logging
from collections import defaultdict
from typing import DefaultDict, Dict, List, Sized, Tuple, Type

from isolating_controller.isolation.policies import IsolationPolicy
from isolating_controller.workload import Workload


class PendingQueue(Sized):
    def __init__(self, policy_type: Type[IsolationPolicy]) -> None:
        self._policy_type: Type[IsolationPolicy] = policy_type

        self._bg_q: Dict[Tuple[int, ...], Workload] = dict()
        self._fg_q: Dict[Tuple[int, ...], Workload] = dict()
        self._ready_queue: DefaultDict[int, List[Workload]] = defaultdict(list)
        self._pending_list: List[IsolationPolicy] = list()

    def __len__(self) -> int:
        return len(tuple(
                filter(lambda x: len(x.foreground_workload.metrics) > 0 and len(x.background_workload.metrics) > 0,
                       self._pending_list)))

    def add(self, workload: Workload) -> None:
        logger = logging.getLogger('monitoring.pending_queue')
        logger.info(f'{workload} is ready for active')

        ready_queue = self._ready_queue[workload.cur_socket_id()]
        ready_queue.append(workload)

        # FIXME: hard coded
        if len(ready_queue) is 2 and ready_queue[0].wl_type != ready_queue[1].wl_type:
            if ready_queue[0].wl_type == 'fg':
                fg = ready_queue[0]
                bg = ready_queue[1]
            else:
                fg = ready_queue[1]
                bg = ready_queue[0]

            new_group = self._policy_type(fg, bg)
            self._pending_list.append(new_group)

            self._ready_queue[workload.cur_socket_id()] = list()

    def pop(self) -> IsolationPolicy:
        if len(self) is 0:
            raise IndexError(f'{self} is empty')
        return self._pending_list.pop()

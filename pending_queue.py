# coding: UTF-8

import logging
from typing import Dict, List, Sized, Tuple, Type

from isolating_controller.isolation.policies import IsolationPolicy
from isolating_controller.workload import Workload


class PendingQueue(Sized):
    def __init__(self, policy_type: Type[IsolationPolicy]) -> None:
        self._policy_type: Type[IsolationPolicy] = policy_type

        self._bg_q: Dict[Tuple[int, ...], Workload] = dict()
        self._fg_q: Dict[Tuple[int, ...], Workload] = dict()
        self._pending_list: List[IsolationPolicy] = list()

    def __len__(self) -> int:
        return len(tuple(
                filter(lambda x: len(x.foreground_workload.metrics) > 0 and len(x.background_workload.metrics) > 0,
                       self._pending_list)))

    def add_bg(self, workload: Workload) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'{workload.name} (pid: {workload.pid}) is ready for active as Background')

        # FIXME: hard coded
        other_cpuset = tuple(map(lambda x: x - 8, workload.cpuset))

        if other_cpuset in self._fg_q:
            new_group = self._policy_type(self._fg_q[other_cpuset], workload)
            self._pending_list.append(new_group)
            del self._fg_q[other_cpuset]

        else:
            self._bg_q[workload.cpuset] = workload

    def add_fg(self, workload: Workload) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'{workload.name} (pid: {workload.pid}) is ready for active as Foreground')

        # FIXME: hard coded
        other_cpuset = tuple(map(lambda x: x + 8, workload.cpuset))

        if other_cpuset in self._bg_q:
            new_group = self._policy_type(self._bg_q[other_cpuset], workload)
            self._pending_list.append(new_group)
            del self._bg_q[other_cpuset]

        else:
            self._fg_q[workload.cpuset] = workload

    def pop(self) -> IsolationPolicy:
        if len(self) is 0:
            raise IndexError(f'{self} is empty')
        return self._pending_list.pop()

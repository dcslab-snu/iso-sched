# coding: UTF-8

import logging
from collections import defaultdict
from itertools import chain, groupby
from typing import DefaultDict, Dict, List, Sized, Tuple, Type

from libs.isolation.policies import IsolationPolicy
from libs.workload import Workload


class PendingQueue(Sized):
    def __init__(self, policy_type: Type[IsolationPolicy]) -> None:
        self._policy_type: Type[IsolationPolicy] = policy_type

        self._ready_queue: DefaultDict[int, List[Workload]] = defaultdict(list)
        self._pending_list: List[Tuple[Workload, List[Workload]]] = list()

    @staticmethod
    def _group_ready(group: Tuple[Workload, List[Workload]]) -> bool:
        return all(len(wl.metrics) > 0 for wl in chain(group[1], (group[0],)))

    def __len__(self) -> int:
        return sum(1 for _ in filter(self._group_ready, self._pending_list))

    def add(self, workload: Workload) -> None:
        logger = logging.getLogger('monitoring.pending_queue')
        logger.info(f'{workload} is ready for active')

        for fg, bgs in self._pending_list:
            if fg.cur_socket_id() == workload.cur_socket_id():
                logger.info(f'Merging {workload} into the group of {fg} as background workload')
                bgs.append(workload)
                return

        ready_queue = self._ready_queue[workload.cur_socket_id()]
        ready_queue.append(workload)

        group_wl: Dict[str, List[Workload]] = {k: list(v) for k, v in groupby(ready_queue, lambda w: w.wl_type)}

        if not ('fg' in group_wl and 'bg' in group_wl):
            return
        elif len(group_wl['fg']) is not 1:
            raise NotImplementedError('Multiple FGs on a socket is not supported')

        self._pending_list.append((group_wl['fg'][0], group_wl['bg']))

        self._ready_queue[workload.cur_socket_id()].clear()

    def pop(self) -> IsolationPolicy:
        if len(self) is 0:
            raise IndexError(f'{self} is empty')

        fg, bgs = self._pending_list.pop()
        return self._policy_type(fg, tuple(bgs))

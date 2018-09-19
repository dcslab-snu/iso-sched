# coding: UTF-8

import logging
from threading import RLock

from typing import Dict, List, Sized, Type

from isolating_controller.isolation.policies import IsolationPolicy
from isolating_controller.workload import Workload
from isolating_controller.utils.numa_topology import NumaTopology

class PendingQueue(Sized):
    def __init__(self, policy_type: Type[IsolationPolicy], max_pending: int) -> None:
        self._policy_type: Type[IsolationPolicy] = policy_type
        self._max_pending: int = max_pending

        self._cur_pending: int = 0
        self._bg_q: Dict[int, Workload] = dict()
        self._fg_q: Dict[int, Workload] = dict()
        self._pending_list: List[IsolationPolicy] = list()

    def __len__(self) -> int:
        return len(tuple(
                filter(lambda x: len(x.foreground_workload.metrics) > 0 and len(x.background_workload.metrics) > 0,
                       self._pending_list)))

    def add_bg(self, workload: Workload) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'{workload} is ready for active as Background')
        logger.info(f'self._cur_pending: {self._cur_pending}')

        self._bg_q[workload.pid] = workload
        self._cur_pending += 1
        if self._cur_pending == self._max_pending:
            self.dump_to_pending_list()


    def add_fg(self, workload: Workload) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'{workload} is ready for active as Foreground')
        logger.info(f'self._cur_pending: {self._cur_pending}')

        self._fg_q[workload.pid] = workload
        self._cur_pending += 1
        if self._cur_pending == self._max_pending:
            self.dump_to_pending_list()

    def pop(self) -> IsolationPolicy:
        if len(self) is 0:
            raise IndexError(f'{self} is empty')
        return self._pending_list.pop()

    def dump_to_pending_list(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info('Dumping workloads to pending list!')

        fg_pids = list(self._fg_q.keys())
        bg_pids = list(self._bg_q.keys())
        all_pids = list()
        for i in range(len(self._fg_q)):
            all_pids.append(fg_pids[i])
        for i in range(len(self._bg_q)):
            all_pids.append(bg_pids[i])

        node_list = NumaTopology.get_node_topo()
        group_pids = dict()  # Dict. for grouping the fg and bg
        for node in node_list:
            group_pids[node] = set()

        for pid in all_pids:
            if pid in fg_pids:
                skt_id = self._fg_q[pid].get_socket_id()
                group_pids[skt_id].add(pid)
            elif pid in bg_pids:
                skt_id = self._bg_q[pid].get_socket_id()
                group_pids[skt_id].add(pid)

        logger.info('Trying to create new groups!')
        #
        # Grouping pids based on their types and skt_id
        for node in node_list:
            node_pidset = group_pids[node]
            pid = node_pidset.pop()
            print(f'Pop {pid}!')
            if pid in fg_pids:
                bg_pid = node_pidset.pop()
                print(f'Pop {bg_pid}!')
                new_group = self._policy_type(self._fg_q[pid], self._bg_q[bg_pid], node)
                self._pending_list.append(new_group)
                del self._fg_q[pid]
                del self._bg_q[bg_pid]
            elif pid in bg_pids:
                fg_pid = node_pidset.pop()
                print(f'Pop {fg_pid}!')
                new_group = self._policy_type(self._fg_q[fg_pid], self._bg_q[pid], node)
                self._pending_list.append(new_group)
                del self._fg_q[fg_pid]
                del self._bg_q[pid]
        return

    def update_max_pending(self, new_max_pending: int):
        self._max_pending = new_max_pending

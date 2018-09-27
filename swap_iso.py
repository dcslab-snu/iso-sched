# coding: UTF-8

import os
import signal
import logging

from enum import IntEnum
from typing import Dict, Optional, Tuple

from isolating_controller.workload import Workload
from isolating_controller.isolation.policies.base_policy import IsolationPolicy
from isolating_controller.utils.cgroup import Cgroup


class SwapNextStep(IntEnum):
    OUT = 0
    IN = 1


class SwapIsolator:
    # FIXME: This threshold needs tests (How small diff is right for swapping workloads?)
    # "-0.5" means the IPCs of workloads in a group drop 50% compared to solo-run
    _IPC_DIFF_THRESHOLD = -0.5

    def __init__(self, isolation_groups: Dict[int, IsolationPolicy]) -> None:
        """

        :param isolation_groups: Dict. Key is the index of group and Value is the group itself
        """
        self._all_groups: Dict[int, IsolationPolicy] = isolation_groups
        self._swap_candidates: Dict[SwapNextStep, Workload] = dict()

        self._most_cont_group: Optional[IsolationPolicy] = None
        self._least_cont_group: Optional[IsolationPolicy] = None

        self._most_cont_workload: Optional[Workload] = None
        self._least_cont_workload: Optional[Workload] = None

        self.aggr_ipc_diffs: Dict[float, int] = dict()   # key:val = aggr_ipc_diff:grp_idx

    def __del__(self):
        logger = logging.getLogger(__name__)
        print('SwapIsolator is closed...')

    def update_cont_group(self) -> None:
        """
        Most contentious group is the group which shows "the LOWEST aggr. ipc diff"
        Least contentious group is the group which shows "the HIGHEST aggr. ipc diff"

        Assumption : Swap Isolator swaps workloads between the most cont. group and the least cont. group
        """
        all_ipc_diffs = list()

        # Update Aggr. IPC Diffs of All Groups
        for grp_idx, group in self._all_groups.items():
            group.update_aggr_ipc()
            aggr_ipc_diff = group.aggr_ipc
            all_ipc_diffs.append(aggr_ipc_diff)
            self.aggr_ipc_diffs[aggr_ipc_diff] = grp_idx

        max_aggr_ipc_diff = max(all_ipc_diffs)
        min_aggr_ipc_diff = min(all_ipc_diffs)

        # Lower ipc diff means lower performance relative to solo-run
        swap_out_grp = self.aggr_ipc_diffs[min_aggr_ipc_diff]
        swap_in_grp = self.aggr_ipc_diffs[max_aggr_ipc_diff]

        self._most_cont_group = swap_out_grp
        self._least_cont_group = swap_in_grp

    def choose_swap_candidates(self):
        swap_out_grp = self._most_cont_group
        swap_in_grp = self._least_cont_group

        # FIXME: This part depends on the swap policy (Which one is selected for swapping)
        # TODO: Need Tests for Swap Overhead
        swap_out_wl = swap_out_grp.least_mem_bw_workload
        swap_in_wl = swap_in_grp.least_mem_bw_workload  # It selects the bg workload in swap_in group

        self._swap_candidates[SwapNextStep.OUT] = swap_out_wl
        self._swap_candidates[SwapNextStep.IN] = swap_in_wl

    def first_decision(self):
        return

    def swap_is_needed(self) -> bool:
        #aggr_ipc_diff_list = list()
        #for _, group in self._all_groups.items():
        #    aggr_ipc_diff_list.append(group.aggr_ipc)

        #min_ipc_diff = min(aggr_ipc_diff_list)
        #avg_min_ipc_diff = min_ipc_diff/2
        # FIXME: We used the average ipc diff value (We assume two workloads in a group at most)
        avg_min_ipc_diff = self._most_cont_group.aggr_ipc/2

        # TODO: Test the _IPC_DIFF_THRESHOLD
        if avg_min_ipc_diff < self._IPC_DIFF_THRESHOLD:
            return True
        else:
            return False

    def do_swap(self) -> None:
        # Enable CPUSET memory migration
        out_proc, in_proc = self.pre_swap_setup()

        out_cpuset = self._swap_candidates[SwapNextStep.OUT].cpuset
        in_cpuset = self._swap_candidates[SwapNextStep.IN].cpuset
        out_skt = self._swap_candidates[SwapNextStep.OUT].socket_id
        in_skt = self._swap_candidates[SwapNextStep.OUT].socket_id

        # Suspend Procs and Enforce Swap Conf.
        os.kill(self._swap_candidates[SwapNextStep.OUT].pid, signal.SIGSTOP)
        os.kill(self._swap_candidates[SwapNextStep.IN].pid, signal.SIGSTOP)

        out_proc.assign_cpus(set(in_cpuset))
        out_proc.assign_mems(set(out_skt))
        in_proc.assign_cpus(set(out_cpuset))
        in_proc.assign_mems(set(in_skt))

        # Resume Procs
        os.kill(self._swap_candidates[SwapNextStep.OUT].pid, signal.SIGCONT)
        os.kill(self._swap_candidates[SwapNextStep.IN].pid, signal.SIGCONT)

    def pre_swap_setup(self) -> Tuple[Cgroup, Cgroup]:
        swap_out_workload = self._swap_candidates[SwapNextStep.OUT]
        swap_in_workload = self._swap_candidates[SwapNextStep.IN]

        swap_out_grp_name = f'{swap_out_workload.name}_{swap_out_workload.pid}'
        swap_in_grp_name = f'{swap_in_workload.name}_{swap_in_workload.pid}'

        out_proc = Cgroup(swap_out_grp_name, 'cpuset,cpu')
        in_proc = Cgroup(swap_in_grp_name, 'cpuset,cpu')

        out_proc.enable_memory_migrate()
        in_proc.enable_memory_migrate()

        return out_proc, in_proc

    def try_swap(self) -> None:
        self.update_cont_group()
        self.choose_swap_candidates()
        if self.swap_is_needed:
            self.do_swap()



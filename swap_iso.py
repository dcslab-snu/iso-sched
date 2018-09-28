# coding: UTF-8

import logging
import os
import signal
from enum import IntEnum
from typing import Dict, Optional

from isolating_controller.isolation.policies.base_policy import IsolationPolicy
from isolating_controller.workload import Workload


class SwapNextStep(IntEnum):
    OUT = 0
    IN = 1


class SwapIsolator:
    # FIXME: This threshold needs tests (How small diff is right for swapping workloads?)
    # "-0.5" means the IPCs of workloads in a group drop 50% compared to solo-run
    _IPC_DIFF_THRESHOLD = -0.5

    def __init__(self, isolation_groups: Dict[IsolationPolicy, int]) -> None:
        """

        :param isolation_groups: Dict. Key is the index of group and Value is the group itself
        """
        self._all_groups: Dict[IsolationPolicy, int] = isolation_groups
        self._swap_candidates: Dict[SwapNextStep, Workload] = dict()

        self._most_cont_group: Optional[IsolationPolicy] = None
        self._least_cont_group: Optional[IsolationPolicy] = None

        self._most_cont_workload: Optional[Workload] = None
        self._least_cont_workload: Optional[Workload] = None

    def __del__(self):
        logger = logging.getLogger(__name__)
        logger.info('SwapIsolator is closed...')

    def update_cont_group(self) -> None:
        """
        Most contentious group is the group which shows "the LOWEST aggr. ipc diff"
        Least contentious group is the group which shows "the HIGHEST aggr. ipc diff"

        Assumption : Swap Isolator swaps workloads between the most cont. group and the least cont. group
        """

        swap_in_grp: Optional[IsolationPolicy] = None
        swap_out_grp: Optional[IsolationPolicy] = None

        for group in self._all_groups.keys():
            if swap_in_grp is None:
                swap_in_grp = group
            if swap_out_grp is None:
                swap_out_grp = group

            group.update_aggr_instr()
            swap_in_grp = max(swap_in_grp, group, key=lambda x: x.aggr_ipc)
            swap_out_grp = min(swap_out_grp, group, key=lambda x: x.aggr_ipc)

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
        # FIXME: We used the average ipc diff value (We assume two workloads in a group at most)
        avg_min_ipc_diff = self._most_cont_group.aggr_ipc / 2

        # TODO: Test the _IPC_DIFF_THRESHOLD
        if avg_min_ipc_diff < self._IPC_DIFF_THRESHOLD:
            return True
        else:
            return False

    def do_swap(self) -> None:
        # Enable CPUSET memory migration
        out_wl = self._swap_candidates[SwapNextStep.OUT]
        in_wl = self._swap_candidates[SwapNextStep.IN]

        # Suspend Procs and Enforce Swap Conf.
        os.kill(out_wl.pid, signal.SIGSTOP)
        os.kill(in_wl.pid, signal.SIGSTOP)

        out_cpus = out_wl.bound_cores
        out_mems = out_wl.mems
        in_cpus = in_wl.bound_cores
        in_mems = in_wl.mems

        out_wl.bound_cores = in_cpus
        out_wl.mems = in_mems
        in_wl.bound_cores = out_cpus
        in_wl.mems = out_mems

        # Resume Procs
        os.kill(out_wl.pid, signal.SIGCONT)
        os.kill(in_wl.pid, signal.SIGCONT)

    def pre_swap_setup(self) -> None:
        swap_out_workload = self._swap_candidates[SwapNextStep.OUT]
        swap_in_workload = self._swap_candidates[SwapNextStep.IN]

        swap_out_workload.cgroup_cpuset.set_memory_migrate(True)
        swap_in_workload.cgroup_cpuset.set_memory_migrate(True)

    def try_swap(self) -> None:
        if len(self._all_groups) < 2:
            return

        self.update_cont_group()
        self.choose_swap_candidates()
        if self.swap_is_needed:
            self.do_swap()

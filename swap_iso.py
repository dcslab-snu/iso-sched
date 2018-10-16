# coding: UTF-8

import logging
import subprocess
from enum import IntEnum
from typing import Dict, Optional, Set

import psutil

from isolating_controller.isolation.policies.base import IsolationPolicy
from isolating_controller.workload import Workload


class SwapNextStep(IntEnum):
    OUT = 0
    IN = 1


class SwapIsolator:
    # FIXME: This threshold needs tests (How small diff is right for swapping workloads?)
    # "-0.5" means the IPCs of workloads in a group drop 50% compared to solo-run
    _IPC_DIFF_THRESHOLD = -0.5
    _VIOLATION_THRESHOLD = 5

    def __init__(self, isolation_groups: Dict[IsolationPolicy, int]) -> None:
        """
        :param isolation_groups: Dict. Key is the index of group and Value is the group itself
        """
        self._all_groups: Dict[IsolationPolicy, int] = isolation_groups
        self._swap_candidates: Dict[SwapNextStep, Workload] = dict()

        self._most_cont_group: Optional[IsolationPolicy] = None
        self._least_cont_group: Optional[IsolationPolicy] = None

        self._prev_wls: Set[Workload] = set()
        self._violation_count: int = 0

    def __del__(self):
        logger = logging.getLogger(__name__)
        logger.info('SwapIsolator is closed...')

    def select_cont_group(self) -> None:
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
            swap_in_grp = max(swap_in_grp, group, key=lambda x: x.aggr_inst)
            swap_out_grp = min(swap_out_grp, group, key=lambda x: x.aggr_inst)

        self._most_cont_group = swap_out_grp
        self._least_cont_group = swap_in_grp

    def swap_is_needed(self) -> bool:
        self.select_cont_group()

        # FIXME: We used the average ipc diff value (We assume two workloads in a group at most)
        avg_min_ipc_diff = self._most_cont_group.aggr_inst / 2

        # TODO: Test the _IPC_DIFF_THRESHOLD
        if avg_min_ipc_diff > self._IPC_DIFF_THRESHOLD:
            self._prev_wls.clear()
            self._violation_count = 0
            return False

        if len(self._prev_wls) is 2 \
                and self._most_cont_group.background_workload in self._prev_wls \
                and self._least_cont_group.background_workload in self._prev_wls:
            self._violation_count += 1
            print(
                    f'violation count of {self._most_cont_group.background_workload}, '
                    f'{self._least_cont_group.background_workload} is {self._violation_count}')
            return self._violation_count >= SwapIsolator._VIOLATION_THRESHOLD

        else:
            self._prev_wls.clear()
            self._prev_wls.add(self._most_cont_group.background_workload)
            self._prev_wls.add(self._least_cont_group.background_workload)
            self._violation_count = 1
            return False

    def do_swap(self) -> None:
        # Enable CPUSET memory migration
        self.pre_swap_setup()

        out_wl = self._most_cont_group.background_workload
        in_wl = self._least_cont_group.background_workload

        print(f'swap {out_wl}, {in_wl}')

        try:
            # Suspend Procs and Enforce Swap Conf.
            out_wl.pause()
            in_wl.pause()

            in_tmp, out_tmp = in_wl.orig_bound_mems, out_wl.orig_bound_mems
            in_wl.orig_bound_mems, out_wl.orig_bound_mems = out_tmp, in_tmp
            in_tmp, out_tmp = in_wl.orig_bound_cores, out_wl.orig_bound_cores
            in_wl.orig_bound_cores, out_wl.orig_bound_cores = out_tmp, in_tmp

            in_tmp, out_tmp = in_wl.bound_mems, out_wl.bound_mems
            in_wl.bound_mems, out_wl.bound_mems = out_tmp, in_tmp
            in_tmp, out_tmp = in_wl.bound_cores, out_wl.bound_cores
            in_wl.bound_cores, out_wl.bound_cores = out_tmp, in_tmp

            self._most_cont_group.background_workload = in_wl
            self._least_cont_group.background_workload = out_wl

        except (psutil.NoSuchProcess, subprocess.CalledProcessError, ProcessLookupError) as e:
            print(e)

        finally:
            # Resume Procs
            out_wl.resume()
            in_wl.resume()
            self._violation_count = 0
            self._prev_wls.clear()

    def pre_swap_setup(self) -> None:
        swap_out_workload = self._most_cont_group.background_workload
        swap_in_workload = self._least_cont_group.background_workload

        swap_out_workload.cgroup_cpuset.set_memory_migrate(True)
        swap_in_workload.cgroup_cpuset.set_memory_migrate(True)

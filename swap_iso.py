# coding: UTF-8

import os
import signal
import logging

from enum import IntEnum
from typing import Dict, Optional, Tuple

from isolating_controller.workload import Workload
from isolating_controller.isolation.policies.base_policy import IsolationPolicy
from isolating_controller.utils.cgroup import Cgroup
from isolating_controller.isolation.isolators import CacheIsolator, CoreIsolator, MemoryIsolator


class SwapNext(IntEnum):
    OUT = 0
    IN = 1


class IsoSetting:
    def __init__(self, wl: Workload):
        self._wl = wl
        self._skt_id = wl.socket_id
        self._pid = wl.pid
        self._cpuset = wl.cgroup.get_cpu_affinity_from_group()
        self._mems = wl.cgroup.get_mem_affinity_from_group()
        self._cpufreq = wl.dvfs.cpufreq()
        self._llc_masks = wl.resctrl.get_llc_mask()

    @property
    def workload(self):
        return self._wl

    @property
    def socket_id(self):
        return self._skt_id

    @property
    def pid(self):
        return self._pid

    @property
    def cpuset(self):
        return self._cpuset

    @property
    def mems(self):
        return self._mems

    @property
    def cpufreq(self):
        return self._cpufreq

    @property
    def llc_masks(self):
        return self._llc_masks


class SwapIsolator:
    # FIXME: This threshold needs tests (How small diff is right for swapping workloads?)
    # "-0.5" means the IPCs of workloads in a group drop 50% compared to solo-run
    _IPC_DIFF_THRESHOLD = -0.5

    def __init__(self, isolation_groups: Dict[int, IsolationPolicy]) -> None:
        """
        :param isolation_groups: Dict. Key is the index of group and Value is the group itself
        """
        self._all_groups: Dict[int, IsolationPolicy] = isolation_groups
        self._swap_candidates: Dict[SwapNext, Workload] = dict()

        self._most_cont_group: Optional[IsolationPolicy] = None
        self._least_cont_group: Optional[IsolationPolicy] = None

        self._most_cont_workload: Optional[Workload] = None
        self._least_cont_workload: Optional[Workload] = None

        # FIXME: Aggr. IPC Diffs may be changed to Agg. Inst. Diffs
        self.aggr_ipc_diffs: Dict[float, IsolationPolicy] = dict()   # key:val = aggr_ipc_diff:group
        self._saved_group_setting: Tuple[IsoSetting, IsoSetting] = None

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
            self.aggr_ipc_diffs[aggr_ipc_diff] = group

        max_aggr_ipc_diff = max(all_ipc_diffs)
        min_aggr_ipc_diff = min(all_ipc_diffs)

        # Lower ipc diff means lower performance relative to solo-run
        swap_out_grp = self.aggr_ipc_diffs[min_aggr_ipc_diff]
        swap_in_grp = self.aggr_ipc_diffs[max_aggr_ipc_diff]

        self._most_cont_group = swap_out_grp
        self._least_cont_group = swap_in_grp

    def choose_swap_candidates(self) -> None:
        swap_out_grp = self._most_cont_group
        swap_in_grp = self._least_cont_group

        # FIXME: This part depends on the swap policy (Which one is selected for swapping)
        # TODO: Need Tests for Swap Overhead
        swap_out_wl = swap_out_grp.least_mem_bw_workload
        swap_in_wl = swap_in_grp.least_mem_bw_workload  # It selects the bg workload in swap_in group

        self._swap_candidates[SwapNext.OUT] = swap_out_wl
        self._swap_candidates[SwapNext.IN] = swap_in_wl

    def swap_is_needed(self) -> bool:
        # FIXME: We used the average ipc diff value (We assume two workloads in a group at most)
        avg_min_ipc_diff = self._most_cont_group.aggr_ipc/2

        # TODO: Test the _IPC_DIFF_THRESHOLD
        if avg_min_ipc_diff < self._IPC_DIFF_THRESHOLD:
            return True
        else:
            return False

    def do_swap(self) -> None:
        # Enable CPUSET memory migration
        self.set_memory_migrate_on()

        out_iso_conf = self._saved_group_setting[SwapNext.OUT]
        in_iso_conf = self._saved_group_setting[SwapNext.IN]

        # Suspend Procs and Enforce Swap Conf.
        os.kill(out_iso_conf.pid, signal.SIGSTOP)
        os.kill(in_iso_conf.pid, signal.SIGSTOP)

        self.apply_saved_iso_setting()

        # Resume Procs
        os.kill(out_iso_conf.pid, signal.SIGCONT)
        os.kill(in_iso_conf.pid, signal.SIGCONT)

    def set_memory_migrate_on(self) -> None:
        swap_out_workload = self._swap_candidates[SwapNext.OUT]
        swap_in_workload = self._swap_candidates[SwapNext.IN]

        swap_out_grp_name = f'{swap_out_workload.name}_{swap_out_workload.pid}'
        swap_in_grp_name = f'{swap_in_workload.name}_{swap_in_workload.pid}'

        out_cgroup = Cgroup(swap_out_grp_name, 'cpuset,cpu')
        in_cgroup = Cgroup(swap_in_grp_name, 'cpuset,cpu')

        out_cgroup.enable_memory_migrate()
        in_cgroup.enable_memory_migrate()

    def try_swap(self) -> None:
        self.update_cont_group()
        self.choose_swap_candidates()
        if self.swap_is_needed:
            self.save_group_setting()
            self.do_swap()

    def save_group_setting(self) -> None:
        # TODO: Before do_swap, swapper should save the group's isolation setting
        out_proc: Workload = self._swap_candidates[SwapNext.OUT]
        in_proc: Workload = self._swap_candidates[SwapNext.IN]
        out_iso_conf = IsoSetting(out_proc)
        in_iso_conf = IsoSetting(in_proc)
        self._saved_group_setting = (out_iso_conf, in_iso_conf)

    def apply_saved_iso_setting(self) -> None:
        # TODO: After do_swap, swapper should load the group's isolation setting
        out_iso_conf = self._saved_group_setting[SwapNext.OUT]
        in_iso_conf = self._saved_group_setting[SwapNext.IN]

        swap_out_wl = out_iso_conf.workload
        swap_in_wl = in_iso_conf.workload

        # Apply CPUSET
        swap_out_wl.cgroup.assign_cpus(in_iso_conf.cpuset)
        swap_in_wl.cgroup.assign_cpus(out_iso_conf.cpuset)

        # Apply Mems
        swap_out_wl.cgroup.assign_mems(in_iso_conf.mems)
        swap_in_wl.cgroup.assign_mems(out_iso_conf.mems)

        # Apply CPU freq
        swap_out_wl.dvfs.set_freq_cgroup(in_iso_conf.cpufreq)
        swap_in_wl.dvfs.set_freq_cgroup(out_iso_conf.cpufreq)

        # Apply llc masks
        swap_out_wl.resctrl.assign_llc(in_iso_conf.llc_masks)
        swap_in_wl.resctrl.assign_llc(out_iso_conf.llc_masks)










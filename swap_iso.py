# coding: UTF-8

import logging

from enum import IntEnum
from typing import Dict, Set, Optional

from isolating_controller.workload import Workload
from isolating_controller.isolation.policies.base_policy import IsolationPolicy


class SwapNextStep(IntEnum):
    OUT = 0
    IN = 1


class SwapIsolator:
    # FIXME: This threshold needs tests (How big diff is right for swapping workloads?)
    _DIFF_THRESHOLD = 0.001

    def __init__(self, isolation_groups: Dict[int, IsolationPolicy]) -> None:
        """

        :param isolation_groups: Dict. Key is the number of group and Value is the group itself
        """
        self._all_groups = isolation_groups
        self._swap_candidates: Dict[SwapNextStep, Workload] = dict()

        self._most_cont_group: Optional[IsolationPolicy] = None
        self._least_cont_group: Optional[IsolationPolicy] = None

        self._most_cont_workload: Optional[Workload] = None
        self._least_cont_workload: Optional[Workload] = None

        self.ipc_diffs: Dict[float, int] = dict()   # key:val = aggr_ipc_diff:grp_idx

    def __del__(self):
        logger = logging.getLogger(__name__)

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
            self.ipc_diffs[aggr_ipc_diff] = grp_idx

        max_aggr_ipc_diff = max(all_ipc_diffs)
        min_aggr_ipc_diff = min(all_ipc_diffs)

        swap_out_grp = self.ipc_diffs[max_aggr_ipc_diff]
        swap_in_grp = self.ipc_diffs[min_aggr_ipc_diff]

        self._most_cont_group = swap_out_grp
        self._least_cont_group = swap_in_grp

    def choose_swap_candidates(self):
        swap_out_grp = self._most_cont_group
        swap_in_grp = self._least_cont_group

        # FIXME: This part depends on the swap policy (Which one is selected for swapping)
        swap_out_wl = swap_out_grp.most_cont_workload
        swap_in_wl = swap_in_grp.most_cont_workload  # It selects the bg workload in swap_in group

        self._swap_candidates[SwapNextStep.OUT] = swap_out_wl
        self._swap_candidates[SwapNextStep.IN] = swap_in_wl

    def first_decision(self):
        return

    def enforce(self):
        return


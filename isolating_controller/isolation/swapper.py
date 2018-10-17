# coding: UTF-8

import logging
import subprocess
from typing import Dict, Optional, Set, Tuple

import psutil

from .policies.base import IsolationPolicy


class SwapIsolator:
    # FIXME: This threshold needs tests (How small diff is right for swapping workloads?)
    # "-0.5" means the IPCs of workloads in a group drop 50% compared to solo-run
    _INST_DIFF_THRESHOLD = -1
    _VIOLATION_THRESHOLD = 5

    def __init__(self, isolation_groups: Dict[IsolationPolicy, int]) -> None:
        """
        :param isolation_groups: Dict. Key is the index of group and Value is the group itself
        """
        self._all_groups: Dict[IsolationPolicy, int] = isolation_groups

        self._prev_wls: Set[IsolationPolicy] = set()
        self._violation_count: int = 0

    def _select_cont_groups(self) -> Optional[Tuple[IsolationPolicy, IsolationPolicy]]:
        """
        Most contentious group is the group which shows "the LOWEST aggr. ipc diff"
        Least contentious group is the group which shows "the HIGHEST aggr. ipc diff"

        Assumption : Swap Isolator swaps workloads between the most cont. group and the least cont. group
        """

        contentions: Dict[IsolationPolicy, Tuple[float, float]] = {
            group: (
                group.foreground_workload.calc_metric_diff().instruction_ps,
                group.background_workload.calc_metric_diff().instruction_ps,
            )
            for group in self._all_groups.keys()
        }

        # TODO: more efficient implementation
        for group1, (g1_fg_cont, g1_bg_cont) in contentions.items():
            for group2, (g2_fg_cont, g2_bg_cont) in contentions.items():
                if group1 == group2:
                    continue

                if g1_fg_cont + g1_bg_cont <= self._INST_DIFF_THRESHOLD < g2_fg_cont + g2_bg_cont \
                        and g1_fg_cont + g2_bg_cont > self._INST_DIFF_THRESHOLD \
                        and g2_fg_cont + g1_bg_cont > self._INST_DIFF_THRESHOLD:
                    logging.getLogger(__name__).debug(f'{group1} and {group2} is selected as swap candidate')
                    return group1, group2

        return None

    def swap_is_needed(self) -> bool:
        logger = logging.getLogger(__name__)
        groups = self._select_cont_groups()

        if groups is None:
            self._prev_wls.clear()
            self._violation_count = 0
            return False

        if len(self._prev_wls) is 2 \
                and groups[0] in self._prev_wls \
                and groups[1] in self._prev_wls:
            self._violation_count += 1
            logger.debug(
                    f'violation count of {groups[0].background_workload}, '
                    f'{groups[1].background_workload} is {self._violation_count}')
            return self._violation_count >= self._VIOLATION_THRESHOLD

        else:
            self._prev_wls.clear()
            self._prev_wls.add(groups[0])
            self._prev_wls.add(groups[1])
            self._violation_count = 1
            return False

    def do_swap(self) -> None:
        logger = logging.getLogger(__name__)
        group1, group2 = tuple(self._prev_wls)
        logger.info(f'Starting swaption between {group1.background_workload} and {group2.background_workload}...')

        workload1 = group1.background_workload
        workload2 = group2.background_workload

        # Enable CPUSET memory migration
        workload1.cgroup_cpuset.set_memory_migrate(True)
        workload2.cgroup_cpuset.set_memory_migrate(True)

        try:
            # Suspend Procs and Enforce Swap Conf.
            workload1.pause()
            workload2.pause()

            tmp1, tmp2 = workload2.orig_bound_mems, workload1.orig_bound_mems
            workload2.orig_bound_mems, workload1.orig_bound_mems = tmp2, tmp1
            tmp1, tmp2 = workload2.orig_bound_cores, workload1.orig_bound_cores
            workload2.orig_bound_cores, workload1.orig_bound_cores = tmp2, tmp1

            group1.background_workload = workload2
            group2.background_workload = workload1

        except (psutil.NoSuchProcess, subprocess.CalledProcessError, ProcessLookupError) as e:
            logger.warning('Error occurred during swaption', e)

        finally:
            # Resume Procs
            workload1.resume()
            workload2.resume()
            self._violation_count = 0
            self._prev_wls.clear()

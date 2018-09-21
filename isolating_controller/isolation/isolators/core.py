# coding: UTF-8

import logging

from typing import Tuple, Set, Dict

from .base_isolator import Isolator
from .. import NextStep
from ...workload import Workload
from ...utils import Cgroup
from ...utils import NumaTopology
from ...utils import hyphen

class CoreIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._fg_cpuset: Tuple[int] = foreground_wl.cpuset
        self._bg_cpuset: Tuple[int] = background_wl.cpuset
        self._cur_bg_step: int = min(self._bg_cpuset)
        self._cur_fg_step: int = max(self._fg_cpuset)

        self._fg_next_step = NextStep.IDLE
        self._bg_next_step = NextStep.IDLE

        self._fg_grp_name: str = f'{foreground_wl.name}_{foreground_wl.pid}'
        self._bg_grp_name: str = f'{background_wl.name}_{background_wl.pid}'

        self._prev_fg_affinity: Tuple[int] = foreground_wl.cpuset
        self._prev_bg_affinity: Tuple[int] = background_wl.cpuset

        self._fg_cgroup = Cgroup(self._fg_grp_name, 'cpuset,cpu')
        self._bg_cgroup = Cgroup(self._bg_grp_name, 'cpuset,cpu')

        cpu_topo, mem_topo = NumaTopology.get_numa_info()
        self._cpu_topo: Dict[int, Set[int]] = cpu_topo
        self._mem_topo: Set[int] = mem_topo

    def __del__(self) -> None:
        if self._background_wl.is_running:
            self._bg_cgroup.assign_cpus(set(self._prev_bg_affinity))
        if self._foreground_wl.is_running:
            self._fg_cgroup.assign_cpus(set(self._prev_fg_affinity))

    def strengthen(self) -> 'CoreIsolator':
        """
        Strengthen reduces the number of CPUs assigned to BG workloads and increase that of FG workload
        TODO: Changing step size, if needed
        :return:
        """
        # NOTE: Caller is assumed that BG workload
        if self._bg_next_step == NextStep.STRENGTHEN:
            self._cur_bg_step += 1
            bg_cpuset = set(self._bg_cpuset)
            bg_cpuset.remove(self._cur_bg_step)
            self._bg_cpuset = tuple(bg_cpuset)
        if self._fg_next_step == NextStep.WEAKEN:
            self._cur_fg_step += 1
            fg_cpuset = set(self._fg_cpuset)
            fg_cpuset.add(self._cur_fg_step)
            self._fg_cpuset = tuple(fg_cpuset)
        return self

    def weaken(self) -> 'CoreIsolator':
        """
        Weaken increase the number of CPUs assigned to BG workloads and decrease that of FG workload
        TODO: Changing step size, if needed
        :return:
        """
        # NOTE: Caller is assumed that BG workload
        if self._bg_next_step == NextStep.WEAKEN:
            self._cur_bg_step -= 1
            bg_cpuset = set(self._bg_cpuset)
            bg_cpuset.add(self._cur_bg_step)
            self._bg_cpuset = tuple(bg_cpuset)
        if self._fg_next_step == NextStep.STRENGTHEN:
            self._cur_fg_step -= 1
            fg_cpuset = set(self._fg_cpuset)
            fg_cpuset.remove(self._cur_fg_step)
            self._fg_cpuset = tuple(fg_cpuset)
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: How about first condition is true but the other is false?
        if self._bg_next_step == NextStep.STRENGTHEN:
            return self._cur_bg_step == max(self._cpu_topo[self._background_wl.socket_id])
        if self._fg_next_step == NextStep.WEAKEN:
            return self._cur_fg_step == self._cur_bg_step-1

    @property
    def is_min_level(self) -> bool:
        # FIXME: How about first condition is true but the other is false?
        if self._bg_next_step == NextStep.WEAKEN:
            return self._cur_bg_step == self._cur_fg_step+1
        if self._fg_next_step == NextStep.STRENGTHEN:
            return self._cur_fg_step == min(self._cpu_topo[self._foreground_wl.socket_id])

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'affinity of background is {hyphen.convert_to_hyphen(self._bg_cpuset)}')
        logger.info(f'affinity of foreground is {hyphen.convert_to_hyphen(self._fg_cpuset)}')

        self._bg_cgroup.assign_cpus(set(self._bg_cpuset))
        self._fg_cgroup.assign_cpus(set(self._fg_cpuset))

    def _first_decision(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()
        curr_diff = metric_diff.local_mem_util_ps

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        ## FIXME: Specifying fg's strengthen/weaken condition (related to fg's performance)
        fg_strengthen_cond = None
        fg_weaken_cond = None
        if curr_diff < 0:
            if self.is_max_level:
                self._bg_next_step = NextStep.STOP
                return NextStep.STOP
            else:
                self._bg_next_step = NextStep.STRENGTHEN
                if fg_weaken_cond:
                    self._fg_next_step = NextStep.WEAKEN
                return NextStep.STRENGTHEN
        elif curr_diff <= CoreIsolator._FORCE_THRESHOLD:
            self._bg_next_step = NextStep.STOP
            return NextStep.STOP
        else:
            if self.is_min_level:
                self._bg_next_step = NextStep.STOP
                return NextStep.STOP
            else:
                self._bg_next_step = NextStep.WEAKEN
                if fg_strengthen_cond:
                    self._fg_next_step = NextStep.STRENGTHEN
                return NextStep.WEAKEN

    def _monitoring_result(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()

        curr_diff = metric_diff.local_mem_util_ps
        prev_diff = self._prev_metric_diff.local_mem_util_ps
        diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        # FIXME: Specifying fg's strengthen/weaken condition (related to fg's performance)
        fg_strengthen_cond = None
        fg_weaken_cond = None
        max_bg_cpuid = max(self._cpu_topo[self._background_wl.socket_id])
        min_bg_cpuid = min(self._cpu_topo[self._background_wl.socket_id])
        if not (min_bg_cpuid < self._cur_bg_step < max_bg_cpuid) \
                or abs(diff_of_diff) <= CoreIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= CoreIsolator._DOD_THRESHOLD:
            self._bg_next_step = NextStep.STOP
            self._fg_next_step = NextStep.STOP
            return NextStep.STOP

        elif curr_diff > 0:
            self._bg_next_step = NextStep.WEAKEN
            if fg_strengthen_cond:
                self._fg_next_step = NextStep.STRENGTHEN
            return NextStep.WEAKEN

        else:
            self._bg_next_step = NextStep.STRENGTHEN
            if fg_weaken_cond:
                self._fg_next_step = NextStep.WEAKEN
            return NextStep.STRENGTHEN

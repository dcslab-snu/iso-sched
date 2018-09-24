# coding: UTF-8

import logging

from typing import Tuple, Set, Dict, Optional

from .base_isolator import Isolator
from .. import NextStep, ResourceType
from ...workload import Workload
from ...utils import Cgroup
from ...utils import NumaTopology
from ...utils import hyphen


class CoreIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload, cont_resource: Optional[ResourceType]) -> None:
        super().__init__(foreground_wl, background_wl, cont_resource)

        self._fg_cpuset: Tuple[int] = foreground_wl.cpuset
        self._bg_cpuset: Tuple[int] = background_wl.cpuset
        self._cur_bg_step: int = min(self._bg_cpuset)
        self._cur_fg_step: int = max(self._fg_cpuset)

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
        logger = logging.getLogger(__name__)
        logger.info(f'self._cur_bg_step: {self._cur_bg_step}')
        logger.info(f'self._cur_fg_step: {self._cur_fg_step}')
        logger.info(f'self._bg_next_step: {self._bg_next_step.name}')
        logger.info(f'self._fg_next_step: {self._fg_next_step.name}')

        if self._bg_next_step == NextStep.STRENGTHEN:
            bg_cpuset = set(self._bg_cpuset)
            bg_cpuset.remove(self._cur_bg_step)
            self._bg_cpuset = tuple(bg_cpuset)
            self._cur_bg_step += 1
        if self._fg_next_step == NextStep.WEAKEN:
            fg_cpuset = set(self._fg_cpuset)
            self._cur_fg_step += 1
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
        logger = logging.getLogger(__name__)
        logger.info(f'self._cur_bg_step: {self._cur_bg_step}')
        logger.info(f'self._cur_fg_step: {self._cur_fg_step}')
        logger.info(f'self._bg_next_step: {self._bg_next_step.name}')
        logger.info(f'self._fg_next_step: {self._fg_next_step.name}')

        if self._bg_next_step == NextStep.WEAKEN:
            bg_cpuset = set(self._bg_cpuset)
            self._cur_bg_step -= 1
            bg_cpuset.add(self._cur_bg_step)
            self._bg_cpuset = tuple(bg_cpuset)
        if self._fg_next_step == NextStep.STRENGTHEN:
            fg_cpuset = set(self._fg_cpuset)
            fg_cpuset.remove(self._cur_fg_step)
            self._fg_cpuset = tuple(fg_cpuset)
            self._cur_fg_step -= 1
        return self

    @property
    def is_max_level(self) -> bool:
        logger = logging.getLogger(__name__)
        logger.info(f'bg max cpuset: {max(self._cpu_topo[self._background_wl.socket_id])}')
        logger.info(f'self._cur_bg_step: {self._cur_bg_step}')
        logger.info(f'self._cur_fg_step: {self._cur_fg_step}')
        logger.info(f'self._bg_next_step: {self._bg_next_step.name}')
        logger.info(f'self._fg_next_step: {self._fg_next_step.name}')
        # FIXME: How about first condition is true but the other is false?
        if self._cur_bg_step == max(self._cpu_topo[self._background_wl.socket_id]):
            self._bg_next_step = NextStep.STOP
            return True
        #if self._cur_fg_step == self._cur_bg_step-1:
        #    self._fg_next_step = NextStep.STOP
        else:
            return False

    @property
    def is_min_level(self) -> bool:
        logger = logging.getLogger(__name__)
        logger.info(f'self._cur_bg_step: {self._cur_bg_step}')
        logger.info(f'self._cur_fg_step: {self._cur_fg_step}')
        logger.info(f'self._bg_next_step: {self._bg_next_step.name}')
        logger.info(f'self._fg_next_step: {self._fg_next_step.name}')

        # FIXME: How about first condition is true but the other is false?
        if self._cur_bg_step == self._cur_fg_step+1:
            return True
        #if self._cur_fg_step == min(self._cpu_topo[self._foreground_wl.socket_id]):
        #    return True
        else:
            return False

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'after enforcing : self._cur_bg_step is {self._cur_bg_step}')
        logger.info(f'after enforcing : self._cur_fg_step is {self._cur_fg_step}')
        logger.info(f'after enforcing : affinity of background is {hyphen.convert_to_hyphen(self._bg_cpuset)}')
        logger.info(f'after enforcing : affinity of foreground is {hyphen.convert_to_hyphen(self._fg_cpuset)}')

        self._bg_cgroup.assign_cpus(set(self._bg_cpuset))
        self._fg_cgroup.assign_cpus(set(self._fg_cpuset))

    def _first_decision(self) -> NextStep:
        curr_diff = None
        metric_diff = self._foreground_wl.calc_metric_diff()

        if self._contentious_resource == ResourceType.MEMORY:
            curr_diff = metric_diff.local_mem_util_ps
        elif self._contentious_resource == ResourceType.CPU:
            curr_diff = metric_diff.ipc

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        # FIXME: Specifying fg's strengthen/weaken condition (related to fg's performance)
        fg_strengthen_cond = self.fg_strengthen_cond(metric_diff.ipc)
        fg_weaken_cond = self.fg_weaken_cond(metric_diff.ipc)
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
        curr_diff = None
        diff_of_diff = None
        logger = logging.getLogger(__name__)
        logger.info(f'self._contentious_resource: {self._contentious_resource.name}')
        if self._contentious_resource == ResourceType.MEMORY:
            curr_diff = metric_diff.local_mem_util_ps
            prev_diff = self._prev_metric_diff.local_mem_util_ps
            diff_of_diff = curr_diff - prev_diff
        elif self._contentious_resource == ResourceType.CPU:
            curr_diff = metric_diff.ipc
            prev_diff = self._prev_metric_diff.ipc
            diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        # FIXME: Specifying fg's strengthen/weaken condition (related to fg's performance)
        fg_strengthen_cond = self.fg_strengthen_cond(metric_diff.ipc)
        fg_weaken_cond = self.fg_weaken_cond(metric_diff.ipc)

        logger = logging.getLogger(__name__)
        logger.info(f'metric_diff.ipc: {metric_diff.ipc}')
        logger.info(f'self.fg_strengthen_cond: {fg_strengthen_cond}')
        logger.info(f'self.fg_weaken_cond: {fg_weaken_cond}')

        # Case1 : diff is too small to perform isolation
        if abs(diff_of_diff) <= CoreIsolator._DOD_THRESHOLD \
            or abs(curr_diff) <= CoreIsolator._DOD_THRESHOLD:
            self._bg_next_step = NextStep.STOP
            # self._fg_next_step = NextStep.STOP # This line depends on bg status
            return NextStep.STOP

        # Case2 : FG shows lower contention than solo-run -> Slower FG or Faster BG
        elif curr_diff > 0:
            self._bg_next_step = NextStep.WEAKEN
            if self.bg_outside_boundary():
                self._bg_next_step = NextStep.STOP
            if fg_strengthen_cond is True:
                self._fg_next_step = NextStep.STRENGTHEN
            elif fg_strengthen_cond is False:
                self._fg_next_step = NextStep.STOP
            return NextStep.WEAKEN

        # Case3 : FG shows higher contention than solo-run
        else:
            self._bg_next_step = NextStep.STRENGTHEN
            if self.bg_outside_boundary():
                self._bg_next_step = NextStep.STOP
            if fg_weaken_cond:
                self._fg_next_step = NextStep.WEAKEN
            elif fg_weaken_cond is False:
                self._fg_next_step = NextStep.STOP
            return NextStep.STRENGTHEN

    def bg_outside_boundary(self) -> bool:
        # FIXME: Assumption about fg's cpuset IDs are smaller than bg's ones. (kind of hard coded)
        max_bg_cpuid = max(self._cpu_topo[self._background_wl.socket_id])
        min_bg_cpuid = max(self._fg_cpuset)+1
        if not (min_bg_cpuid < self._cur_bg_step < max_bg_cpuid):
            return True
        else:
            return False

    def fg_strengthen_cond(self, fg_ipc_diff) -> bool:
        min_skt_cpuid = min(self._cpu_topo[self._foreground_wl.socket_id])
        if fg_ipc_diff > 0 and self._cur_fg_step > min_skt_cpuid:
            return True
        else:
            return False

    def fg_weaken_cond(self, fg_ipc_diff) -> bool:
        if fg_ipc_diff <= 0:
            free_cpu = self._cur_bg_step - self._cur_fg_step
            if (free_cpu > 0 and self._bg_next_step != NextStep.WEAKEN) \
                    or (free_cpu == 0 and self._bg_next_step == NextStep.STOP):
                return True
        else:
            return False

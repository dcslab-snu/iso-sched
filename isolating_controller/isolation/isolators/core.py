# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import NextStep, ResourceType
from ...workload import Workload


class CoreIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1
    _INST_PS_THRESHOLD = -0.5

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded (contiguous allocation)
        self._cur_fg_step: int = foreground_wl.orig_bound_cores[-1]
        self._cur_bg_step: int = background_wl.orig_bound_cores[0]

        self._bg_next_step: NextStep = NextStep.IDLE
        self._fg_next_step: NextStep = NextStep.IDLE

        self._contentious_resource: ResourceType = ResourceType.MEMORY

    def strengthen(self) -> 'CoreIsolator':
        """
        Strengthen reduces the number of CPUs assigned to BG workloads and increase that of FG workload
        TODO: Changing step size, if needed
        """
        # NOTE: Caller is assumed that BG workload

        if self._bg_next_step == NextStep.STRENGTHEN:
            self._cur_bg_step += 1

        if self._fg_next_step == NextStep.WEAKEN:
            self._cur_fg_step += 1

        return self

    def weaken(self) -> 'CoreIsolator':
        """
        Weaken increase the number of CPUs assigned to BG workloads and decrease that of FG workload
        TODO: Changing step size, if needed
        """
        # NOTE: Caller is assumed that BG workload

        if self._bg_next_step == NextStep.WEAKEN:
            self._cur_bg_step -= 1

        if self._fg_next_step == NextStep.STRENGTHEN:
            self._cur_fg_step -= 1

        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded (contiguous allocation)
        return self._cur_bg_step == self._background_wl.orig_bound_cores[-1] and \
               self._cur_fg_step == self._cur_bg_step - 1

    @property
    def is_min_level(self) -> bool:
        return self._cur_bg_step == self._background_wl.orig_bound_cores[0] and \
               self._cur_fg_step == self._foreground_wl.orig_bound_cores[-1]

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.debug(f'fg affinity : {self._foreground_wl.orig_bound_cores[0]}-{self._cur_fg_step}')
        logger.debug(f'bg affinity : {self._cur_bg_step}-{self._background_wl.orig_bound_cores[-1]}')

        # FIXME: hard coded (contiguous allocation)
        self._foreground_wl.bound_cores = range(self._foreground_wl.orig_bound_cores[0], self._cur_fg_step + 1)
        self._background_wl.bound_cores = range(self._cur_bg_step, self._background_wl.orig_bound_cores[-1] + 1)

    def _first_decision(self) -> NextStep:
        curr_diff = None
        metric_diff = self._foreground_wl.calc_metric_diff()

        if self._contentious_resource == ResourceType.MEMORY:
            curr_diff = metric_diff.local_mem_util_ps
        elif self._contentious_resource == ResourceType.CPU:
            curr_diff = metric_diff.instruction_ps

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        # FIXME: Specifying fg's strengthen/weaken condition (related to fg's performance)
        if curr_diff < 0:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return self._strengthen_condition(metric_diff.instruction_ps)

        elif curr_diff <= CoreIsolator._FORCE_THRESHOLD:
            return NextStep.STOP

        else:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return self._weaken_condition(metric_diff.instruction_ps)

    def _monitoring_result(self) -> NextStep:
        logger = logging.getLogger(__name__)
        logger.info(f'self._contentious_resource: {self._contentious_resource.name}')

        metric_diff = self._foreground_wl.calc_metric_diff()
        curr_diff = None
        diff_of_diff = None
        if self._contentious_resource == ResourceType.MEMORY:
            curr_diff = metric_diff.local_mem_util_ps
            prev_diff = self._prev_metric_diff.local_mem_util_ps
            diff_of_diff = curr_diff - prev_diff
        elif self._contentious_resource == ResourceType.CPU:
            curr_diff = metric_diff.instruction_ps
            prev_diff = self._prev_metric_diff.instruction_ps
            diff_of_diff = curr_diff - prev_diff

        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        # Case1 : diff is too small to perform isolation
        if self.is_max_level or self.is_min_level \
                or abs(diff_of_diff) <= CoreIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= CoreIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        # Case2 : FG shows lower contention than solo-run -> Slower FG or Faster BG
        elif curr_diff > 0:
            return self._weaken_condition(metric_diff.instruction_ps)

        # Case3 : FG shows higher contention than solo-run
        else:
            return self._strengthen_condition(metric_diff.instruction_ps)

    def _weaken_condition(self, fg_instruction_ps: float) -> NextStep:
        fg_not_used_cores = len(self._foreground_wl.bound_cores) - self._foreground_wl.number_of_threads
        # BG Next Step Decision
        # ResourceType.CPU - If FG workload not fully use all its assigned cores..., then BG can weaken!
        if self._contentious_resource == ResourceType.CPU:
            if fg_not_used_cores == 0:
                self._bg_next_step = NextStep.IDLE
            elif fg_not_used_cores > 0:
                self._bg_next_step = NextStep.WEAKEN
        # ResourceType.MEMORY - If BG workload was strengthened than its assigned cores, then BG can weaken!
        elif self._contentious_resource == ResourceType.MEMORY:
            if self._cur_bg_step == self._background_wl.orig_bound_cores[0]:
                self._bg_next_step = NextStep.IDLE
            else:
                self._bg_next_step = NextStep.WEAKEN

        # FIXME: Specifying fg's strengthen/weaken condition (related to fg's performance)
        # FIXME: hard coded (contiguous allocation)
        # FG Next Step Decision
        if fg_instruction_ps > self._INST_PS_THRESHOLD and self._foreground_wl.orig_bound_cores[-1] < self._cur_fg_step:
            self._fg_next_step = NextStep.STRENGTHEN
        else:
            self._fg_next_step = NextStep.IDLE

        if self._bg_next_step is NextStep.IDLE and self._fg_next_step is NextStep.IDLE:
            return NextStep.STOP
        else:
            return NextStep.WEAKEN

    def _strengthen_condition(self, fg_instruction_ps: float) -> NextStep:
        logger = logging.getLogger(__name__)

        # BG Next Step Decision
        # ResourceType.CPU - If FG workload shows low performance and FG's threads are larger than its assigned cores,
        # then BG can strengthen!
        if self._contentious_resource == ResourceType.CPU:
            if fg_instruction_ps > self._INST_PS_THRESHOLD:
                self._bg_next_step = NextStep.IDLE
            elif fg_instruction_ps <= self._INST_PS_THRESHOLD and \
                    self._foreground_wl.number_of_threads > len(self._foreground_wl.bound_cores):
                self._bg_next_step = NextStep.STRENGTHEN
        # ResourceType.MEMORY - If BG workload can strengthen its cores... , then strengthen BG's cores!
        elif self._contentious_resource == ResourceType.MEMORY:
            if self._cur_bg_step == self._background_wl.orig_bound_cores[-1]:
                self._bg_next_step = NextStep.IDLE
            else:
                self._bg_next_step = NextStep.STRENGTHEN

        # FIXME: hard coded (contiguous allocation)
        # FG Next Step Decision
        logger.debug(f'FG threads: {self._foreground_wl.number_of_threads}, '
                     f'orig_bound_cores: {self._foreground_wl.orig_bound_cores}')
        if fg_instruction_ps < self._INST_PS_THRESHOLD \
                and (self._bg_next_step is NextStep.STRENGTHEN or self._cur_bg_step - self._cur_fg_step > 1) \
                and self._foreground_wl.number_of_threads > len(self._foreground_wl.orig_bound_cores):
            self._fg_next_step = NextStep.WEAKEN
        else:
            self._fg_next_step = NextStep.IDLE

        if self._bg_next_step is NextStep.IDLE and self._fg_next_step is NextStep.IDLE:
            return NextStep.STOP
        else:
            return NextStep.STRENGTHEN

    def reset(self) -> None:
        if self._background_wl.is_running:
            self._background_wl.bound_cores = self._background_wl.orig_bound_cores
        if self._foreground_wl.is_running:
            self._foreground_wl.bound_cores = self._foreground_wl.orig_bound_cores

    @staticmethod
    def _is_more_core_benefit(wl: Workload) -> bool:
        wl_threads = wl.number_of_threads
        wl_cpus= len(wl.cgroup_cpuset.read_cpus())
        print(f'{wl.wl_type}, {wl.name}, threads : {wl_threads}, len(cpuset): {wl_cpus}')
        if wl_threads > wl_cpus:
            return True
        else:
            return False

    @staticmethod
    def _is_less_core_benefit(wl: Workload) -> bool:
        wl_threads = wl.number_of_threads
        wl_cpus= len(wl.cgroup_cpuset.read_cpus())
        print(f'{wl.wl_type}, {wl.name}, threads : {wl_threads}, len(cpuset): {wl_cpus}')
        if wl_threads < wl_cpus:
            return True
        else:
            return False

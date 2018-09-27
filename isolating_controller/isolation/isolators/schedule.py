# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import NextStep
from ...utils.cgroup.cpuset import CpuSet
from ...workload import Workload


class SchedIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded
        if background_wl.cur_socket_id() is 1:
            self._cur_step = 24
        else:
            self._cur_step = 8

        # FIXME: hard coded
        self._prev_bg_affinity = range(8, 16) if background_wl.cur_socket_id() is 0 else range(24, 32)

        self._bg_grp = CpuSet(background_wl.group_name)

    def strengthen(self) -> 'SchedIsolator':
        self._cur_step += 1
        return self

    def weaken(self) -> 'SchedIsolator':
        self._cur_step -= 1
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        if self._background_wl.cur_socket_id() is 1:
            return self._cur_step == 31
        else:
            return self._cur_step == 15

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        if self._background_wl.cur_socket_id() is 1:
            return self._cur_step == 24
        else:
            return self._cur_step == 8

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        # FIXME: hard coded
        if self._background_wl.cur_socket_id() is 1:
            logger.info(f'affinity of background is {self._cur_step}-31')
        else:
            logger.info(f'affinity of background is {self._cur_step}-15')

        # FIXME: hard coded
        self._bg_grp.assign_cpus(range(self._cur_step, 32 if self._background_wl.cur_socket_id() is 1 else 16))

    def _first_decision(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()
        curr_diff = metric_diff.local_mem_util_ps

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        if curr_diff < 0:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN
        elif curr_diff <= SchedIsolator._FORCE_THRESHOLD:
            return NextStep.STOP
        else:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

    def _monitoring_result(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()

        curr_diff = metric_diff.local_mem_util_ps
        prev_diff = self._prev_metric_diff.local_mem_util_ps
        diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        # FIXME: hard coded
        if (self._background_wl.cur_socket_id() is 1 and not (24 < self._cur_step < 31) or
            self._background_wl.cur_socket_id() is 0 and not (8 < self._cur_step < 15)) \
                or abs(diff_of_diff) <= SchedIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= SchedIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            return NextStep.WEAKEN

        else:
            return NextStep.STRENGTHEN

    def reset(self) -> None:
        if self._background_wl.is_running:
            self._bg_grp.assign_cpus(self._prev_bg_affinity)

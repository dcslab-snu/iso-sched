# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import NextStep
from ...workload import Workload


class SchedIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded
        self._cur_step = background_wl.orig_bound_cores[0]

    def strengthen(self) -> 'SchedIsolator':
        self._cur_step += 1
        return self

    def weaken(self) -> 'SchedIsolator':
        self._cur_step -= 1
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step == self._background_wl.orig_bound_cores[-1]

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step == self._background_wl.orig_bound_cores[0]

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        # FIXME: hard coded
        logger.info(f'affinity of background is {self._cur_step}-{self._background_wl.orig_bound_cores[-1]}')

        # FIXME: hard coded
        self._background_wl.bound_cores = range(self._cur_step, self._background_wl.orig_bound_cores[-1] + 1)

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
        if self.is_min_level or self.is_max_level \
                or abs(diff_of_diff) <= SchedIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= SchedIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            return NextStep.WEAKEN

        else:
            return NextStep.STRENGTHEN

    def reset(self) -> None:
        if self._background_wl.is_running:
            self._background_wl.bound_cores = self._background_wl.orig_bound_cores

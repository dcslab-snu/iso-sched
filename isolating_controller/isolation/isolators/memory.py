# coding: UTF-8

import logging
from itertools import chain

from .base_isolator import Isolator
from .. import NextStep
from ...utils import DVFS
from ...workload import Workload


class MemoryIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._bg_affinity = background_wl.cpuset

        # FIXME: hard coded
        self._cur_step = DVFS.MAX

    def __del__(self) -> None:
        DVFS.set_freq(DVFS.MAX, chain(self._bg_affinity))

    def strengthen(self) -> 'MemoryIsolator':
        self._cur_step -= DVFS.STEP
        return self

    def weaken(self) -> 'MemoryIsolator':
        self._cur_step += DVFS.STEP
        return self

    @property
    def is_max_level(self) -> bool:
        return self._cur_step - DVFS.STEP < DVFS.MIN

    @property
    def is_min_level(self) -> bool:
        return self._cur_step == DVFS.MAX

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'frequency of cpuset {self._background_wl.cpuset} is {self._cur_step / 1_000_000}GHz')

        DVFS.set_freq(self._cur_step, self._background_wl.cpuset)

    def _try_scheduled(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()
        curr_diff = metric_diff.local_mem_util_ps

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        if curr_diff < 0:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN
        elif curr_diff <= MemoryIsolator._FORCE_THRESHOLD:
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

        self._prev_metric_diff = metric_diff

        if not (DVFS.MIN < self._cur_step < DVFS.MAX) \
                or abs(diff_of_diff) <= MemoryIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= MemoryIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            # FIXME: hard coded
            if DVFS.MAX <= self._cur_step - DVFS.STEP:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

        else:
            # FIXME: hard coded
            if self._cur_step - DVFS.STEP <= DVFS.MIN:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN

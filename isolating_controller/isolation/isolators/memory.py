# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import NextStep
from ...utils import DVFS
from ...workload import Workload


class MemoryIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded
        self._cur_step = DVFS.MAX

    def increase(self) -> 'MemoryIsolator':
        self._cur_step -= DVFS.STEP
        return self

    def decrease(self) -> 'MemoryIsolator':
        self._cur_step += DVFS.STEP
        return self

    def _enforce(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f'frequency of cpuset {self._background_wl.cpuset} is {self._cur_step / 1_000_000}GHz')

        DVFS.set_freq(self._cur_step, self._background_wl.cpuset)

    def monitoring_result(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()

        curr_diff = metric_diff.local_mem_util
        prev_diff = self._prev_metric_diff.local_mem_util
        diff_of_diff = curr_diff - prev_diff

        # TODO: remove
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f'diff of diff is {diff_of_diff}')
        logger.info(f'current diff: {curr_diff}, previous diff: {prev_diff}')

        self._prev_metric_diff = metric_diff

        if not (DVFS.MIN < self._cur_step < DVFS.MAX) \
                or abs(curr_diff) <= MemoryIsolator._THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            # FIXME: hard coded
            if DVFS.MAX <= self._cur_step - DVFS.STEP:
                return NextStep.STOP
            else:
                return NextStep.DECREASE

        else:
            # FIXME: hard coded
            if self._cur_step - DVFS.STEP <= DVFS.MIN:
                return NextStep.STOP
            else:
                return NextStep.INCREASE

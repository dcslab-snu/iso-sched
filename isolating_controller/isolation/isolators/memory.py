# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import IsolationResult
from ...metric_container.basic_metric import MetricDiff
from ...utils import DVFS
from ...workload import Workload


class MemoryIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coding
        self._cur_step = DVFS.MAX - DVFS.STEP

    def increase(self) -> 'MemoryIsolator':
        self._cur_step += DVFS.STEP
        return self

    def decrease(self) -> 'MemoryIsolator':
        self._cur_step -= DVFS.STEP
        return self

    def _enforce(self) -> None:
        # FIXME: hard coding
        DVFS.set_freq(self._cur_step, range(8, 16))

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        logger = logging.getLogger(self.__class__.__name__)

        curr = metric_diff.local_mem_util
        prev = self._prev_metric_diff.local_mem_util
        diff = curr - prev

        # TODO: remove
        logger.info(f'monitoring diff is {diff}')
        logger.info(f'current diff: {curr}, prev diff: {prev}')

        if not (DVFS.MIN <= self._cur_step <= DVFS.MAX) or abs(diff) <= MemoryIsolator._THRESHOLD:
            return IsolationResult.STOP
        elif diff > 0:
            return IsolationResult.INCREASE
        else:
            return IsolationResult.DECREASE

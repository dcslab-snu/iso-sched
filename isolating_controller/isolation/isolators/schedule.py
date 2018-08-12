# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import IsolationResult
from ...metric_container.basic_metric import MetricDiff
from ...utils import CgroupCpuset
from ...workload import Workload


class SchedIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coding
        self._cur_step = 9

        CgroupCpuset.create_group(str(self._background_wl.pid))
        CgroupCpuset.add_task(str(self._background_wl.pid), self._background_wl.pid)

    def __del__(self):
        CgroupCpuset.remove_group(str(self._background_wl.pid))

    def increase(self) -> 'SchedIsolator':
        self._cur_step -= 1
        return self

    def decrease(self) -> 'SchedIsolator':
        self._cur_step += 1
        return self

    def _enforce(self) -> None:
        # FIXME: hard coding
        CgroupCpuset.assign(str(self._background_wl.pid), set(range(self._cur_step, 16)))

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        logger = logging.getLogger(self.__class__.__name__)

        curr = metric_diff.local_mem_util
        prev = self._prev_metric_diff.local_mem_util
        diff = curr - prev

        # TODO: remove
        logger.info(f'monitoring diff is {diff}')
        logger.info(f'current diff: {curr}, prev diff: {prev}')

        if not (8 <= self._cur_step <= 15) or abs(diff) <= SchedIsolator._THRESHOLD:
            return IsolationResult.STOP
        elif diff > 0:
            return IsolationResult.INCREASE
        else:
            return IsolationResult.DECREASE

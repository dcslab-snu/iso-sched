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

        # FIXME: hard coded
        self._cur_step = 9

        CgroupCpuset.create_group(str(self._background_wl.pid))
        CgroupCpuset.add_task(str(self._background_wl.pid), self._background_wl.pid)

    def increase(self) -> 'SchedIsolator':
        self._cur_step += 1
        return self

    def decrease(self) -> 'SchedIsolator':
        self._cur_step -= 1
        return self

    def _enforce(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        # FIXME: hard coded
        logger.info(f'affinity of background is {self._cur_step}-15')

        # FIXME: hard coded
        CgroupCpuset.assign(str(self._background_wl.pid), set(range(self._cur_step, 16)))

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        logger = logging.getLogger(self.__class__.__name__)

        curr_diff = metric_diff.local_mem_util
        prev_diff = self._prev_metric_diff.local_mem_util
        diff_of_diff = curr_diff - prev_diff

        # TODO: remove
        logger.info(f'diff of diff is {diff_of_diff}')
        logger.info(f'current diff: {curr_diff}, previous diff: {prev_diff}')

        # FIXME: hard coded
        if not (8 < self._cur_step < 15) \
                or abs(diff_of_diff) <= SchedIsolator._THRESHOLD \
                or abs(curr_diff) <= SchedIsolator._THRESHOLD:
            return IsolationResult.STOP

        elif curr_diff > 0:
            return IsolationResult.DECREASE

        else:
            return IsolationResult.INCREASE

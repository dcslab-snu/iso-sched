# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import IsolationResult
from ...metric_container.basic_metric import MetricDiff
from ...utils import CAT
from ...workload import Workload


class CacheIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._cur_step = CAT.MAX // 2 + CAT.STEP
        self._acceleration = CAT.STEP

        CAT.create_group(str(foreground_wl.pid))
        CAT.add_task(str(foreground_wl.pid), foreground_wl.pid)
        CAT.create_group(str(self._background_wl.pid))
        CAT.add_task(str(self._background_wl.pid), self._background_wl.pid)

    def increase(self) -> 'CacheIsolator':
        self._cur_step += 1
        self._acceleration *= 2
        return self

    def decrease(self) -> 'CacheIsolator':
        self._cur_step -= 1
        self._acceleration //= 2
        # TODO: suggest `self._acceleration = CAT.STEP`
        return self

    def _enforce(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f'current step : {self._cur_step}, accel: {self._acceleration}')
        logger.info(f'foreground : background = {self._cur_step} : {CAT.MAX - self._cur_step}')

        # FIXME: hard coded
        fg_mask = CAT.gen_mask(0, self._cur_step)
        CAT.assign(str(self._foreground_wl.pid), fg_mask, '1')

        # FIXME: hard coded
        bg_mask = CAT.gen_mask(self._cur_step)
        CAT.assign(str(self._background_wl.pid), bg_mask, '1')

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        logger = logging.getLogger(self.__class__.__name__)

        curr_diff = metric_diff.l3_hit_ratio
        prev_diff = self._prev_metric_diff.l3_hit_ratio
        diff_of_diff = curr_diff - prev_diff

        # TODO: remove
        logger.info(f'diff of diff is {diff_of_diff}')
        logger.info(f'current diff: {curr_diff}, previous diff: {prev_diff}')

        if not (CAT.MIN < self._cur_step < CAT.MAX) \
                or abs(diff_of_diff) <= CacheIsolator._THRESHOLD \
                or abs(curr_diff) <= CacheIsolator._THRESHOLD:
            return IsolationResult.STOP

        elif curr_diff > 0:
            return IsolationResult.DECREASE

        else:
            return IsolationResult.INCREASE

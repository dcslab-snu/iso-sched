# coding: UTF-8

import logging
import subprocess

from .base_isolator import Isolator
from .. import NextStep
from ...utils import CgroupCpuset
from ...workload import Workload


class SchedIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._prev_bg_affinity = background_wl.cpuset
        # FIXME: hard coded
        self._cur_step = 24

        CgroupCpuset.create_group(str(background_wl.pid))
        CgroupCpuset.add_task(str(background_wl.pid), background_wl.pid)
        # FIXME: hard coded
        CgroupCpuset.assign(str(background_wl.pid), set(range(self._cur_step, 32)))

    def __del__(self) -> None:
        if self._background_wl.is_running:
            CgroupCpuset.assign(str(self._background_wl.pid), set(self._prev_bg_affinity))
        else:
            CgroupCpuset.remove_group(str(self._background_wl.pid))

    def strengthen(self) -> 'SchedIsolator':
        self._cur_step += 1
        return self

    def weaken(self) -> 'SchedIsolator':
        self._cur_step -= 1
        return self

    def _enforce(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        # FIXME: hard coded
        logger.info(f'affinity of background is {self._cur_step}-31')

        # FIXME: hard coded
        CgroupCpuset.assign(str(self._background_wl.pid), set(range(self._cur_step, 32)))

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

        # FIXME: hard coded
        if not (24 < self._cur_step < 31) \
                or abs(curr_diff) <= SchedIsolator._THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            return NextStep.WEAKEN

        else:
            return NextStep.STRENGTHEN

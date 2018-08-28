# coding: UTF-8

import logging

from .base_isolator import Isolator
from .. import NextStep
from ...utils import CgroupCpuset
from ...workload import Workload


class SchedIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded
        self._cur_step = 24
        self._prev_bg_affinity = set(range(self._cur_step, 32))

        self._bg_grp_name = f'{background_wl.name}_{background_wl.pid}'

        CgroupCpuset.create_group(self._bg_grp_name)
        CgroupCpuset.add_task(self._bg_grp_name, background_wl.pid)
        # FIXME: hard coded
        CgroupCpuset.assign(self._bg_grp_name, set(range(self._cur_step, 32)))

    def __del__(self) -> None:
        if self._background_wl.is_running:
            CgroupCpuset.assign(self._bg_grp_name, set(self._prev_bg_affinity))
        else:
            CgroupCpuset.remove_group(self._bg_grp_name)

    def strengthen(self) -> 'SchedIsolator':
        self._cur_step += 1
        return self

    def weaken(self) -> 'SchedIsolator':
        self._cur_step -= 1
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step == 31

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step == 24

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        # FIXME: hard coded
        logger.info(f'affinity of background is {self._cur_step}-31')

        # FIXME: hard coded
        CgroupCpuset.assign(self._bg_grp_name, set(range(self._cur_step, 32)))

    def _monitoring_result(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()

        curr_diff = metric_diff.local_mem_util
        prev_diff = self._prev_metric_diff.local_mem_util
        diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        self._prev_metric_diff = metric_diff

        # FIXME: hard coded
        if not (24 < self._cur_step < 31) \
                or abs(diff_of_diff) <= SchedIsolator._THRESHOLD \
                or abs(curr_diff) <= SchedIsolator._THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            return NextStep.WEAKEN

        else:
            return NextStep.STRENGTHEN

# coding: UTF-8

import logging
from typing import Optional

from .base_isolator import Isolator
from .. import NextStep
from ...utils import CAT
from ...workload import Workload


class CacheIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._prev_step: Optional[int] = None
        self._cur_step: Optional[int] = None

        foreground_group = str(foreground_wl.pid)
        CAT.create_group(foreground_group)
        for tid in foreground_wl.all_child_tid():
            CAT.add_task(foreground_group, tid)

        background_group = str(background_wl.pid)
        CAT.create_group(background_group)
        for tid in background_wl.all_child_tid():
            CAT.add_task(background_group, tid)

    def strengthen(self) -> 'CacheIsolator':
        self._prev_step = self._cur_step

        if self._cur_step is None:
            self._cur_step = CAT.MAX // 2
        else:
            self._cur_step += 1

        return self

    def weaken(self) -> 'CacheIsolator':
        self._prev_step = self._cur_step

        if self._cur_step is not None:
            if self._prev_step is None:
                self._cur_step = None
            else:
                self._cur_step -= 1

        return self

    def _enforce(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)

        if self._cur_step is None:
            logger.info(f'turn off CAT')

            # FIXME: hard coded
            mask = CAT.gen_mask(0, CAT.MAX)
            CAT.assign(str(self._foreground_wl.pid), '1', mask)
            CAT.assign(str(self._background_wl.pid), '1', mask)

        else:
            logger.info(f'foreground : background = {self._cur_step} : {CAT.MAX - self._cur_step}')

            # FIXME: hard coded
            fg_mask = CAT.gen_mask(0, self._cur_step)
            CAT.assign(str(self._foreground_wl.pid), '1', fg_mask)

            # FIXME: hard coded
            bg_mask = CAT.gen_mask(self._cur_step)
            CAT.assign(str(self._background_wl.pid), '1', bg_mask)

    # TODO: consider turn off cache partitioning
    def monitoring_result(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()

        curr_diff = metric_diff.l3_hit_ratio
        prev_diff = self._prev_metric_diff.l3_hit_ratio
        diff_of_diff = curr_diff - prev_diff

        # TODO: remove
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f'diff of diff is {diff_of_diff}')
        logger.info(f'current diff: {curr_diff}, previous diff: {prev_diff}')

        self._prev_metric_diff = metric_diff

        if self._cur_step is not None \
                and not (CAT.MIN < self._cur_step < CAT.MAX) \
                or abs(diff_of_diff) <= CacheIsolator._THRESHOLD \
                or abs(curr_diff) <= CacheIsolator._THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            # FIXME: hard coded
            if self._cur_step is None or self._cur_step - CAT.STEP <= CAT.MIN:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

        else:
            # FIXME: hard coded
            if self._cur_step is None or CAT.MAX <= self._cur_step + CAT.STEP:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN

# coding: UTF-8

import logging
from typing import Optional, Dict, Set

from .base_isolator import Isolator
from .. import NextStep
from ...utils import ResCtrl
from ...utils import NumaTopology
from ...workload import Workload


class CacheIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._prev_step: Optional[int] = None
        self._cur_step: Optional[int] = None

        self._fg_grp_name = f'{foreground_wl.name}_{foreground_wl.pid}'
        self._bg_grp_name = f'{background_wl.name}_{background_wl.pid}'

        self._fg_resctrl = ResCtrl(self._fg_grp_name)
        self._bg_resctrl = ResCtrl(self._bg_grp_name)

    def __del__(self) -> None:
        logger = logging.getLogger(__name__)

        max_bits = ResCtrl.MAX_BITS
        max_mask = ResCtrl.gen_mask(0, max_bits)

        if self._foreground_wl.is_running:
            logger.debug(f'reset resctrl configuration of {self._foreground_wl}')
            # FIXME: The number of socket is two at most
            ResCtrl.assign_llc(self._fg_resctrl, max_mask, max_mask)

        if self._background_wl.is_running:
            logger.debug(f'reset resctrl configuration of {self._background_wl}')
            # FIXME: The number of socket is two at most
            ResCtrl.assign_llc(self._bg_resctrl, max_mask, max_mask)

    def strengthen(self) -> 'CacheIsolator':
        self._prev_step = self._cur_step

        if self._cur_step is None:
            self._cur_step = ResCtrl.MAX_BITS // 2
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

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step is not None and self._cur_step + ResCtrl.STEP >= ResCtrl.MAX_BITS

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step is None or self._cur_step - ResCtrl.STEP <= ResCtrl.MIN_BITS

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)

        bg_socket_id = self._background_wl.socket_id
        fg_socket_id = self._foreground_wl.socket_id

        if self._cur_step is None:
            logger.info('CAT off')

            # FIXME: The number of socket is two at most
            mask = ResCtrl.gen_mask(0, ResCtrl.MAX_BITS)
            if bg_socket_id == 0:
                ResCtrl.assign_llc(self._bg_resctrl, mask, '1')
            if bg_socket_id == 1:
                ResCtrl.assign_llc(self._bg_resctrl, '1', mask)
            if fg_socket_id == 0:
                ResCtrl.assign_llc(self._fg_resctrl, mask, '1')
            if fg_socket_id == 1:
                ResCtrl.assign_llc(self._fg_resctrl, '1', mask)

        else:
            logger.info(f'foreground : background = {self._cur_step} : {ResCtrl.MAX_BITS - self._cur_step}')

            # FIXME: The number of socket is two at most
            fg_mask = ResCtrl.gen_mask(0, self._cur_step)
            if fg_socket_id == 0:
                ResCtrl.assign_llc(self._fg_resctrl, fg_mask, '1')
            if fg_socket_id == 1:
                ResCtrl.assign_llc(self._fg_resctrl, '1', fg_mask)

            # FIXME: The number of socket is two at most
            bg_mask = ResCtrl.gen_mask(self._cur_step)
            if bg_socket_id == 0:
                ResCtrl.assign_llc(self._bg_resctrl, bg_mask, '1')
            if bg_socket_id == 1:
                ResCtrl.assign_llc(self._bg_resctrl, '1', bg_mask)


    def _first_decision(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()
        curr_diff = metric_diff.l3_hit_ratio

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        if curr_diff < 0:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN
        elif curr_diff <= CacheIsolator._FORCE_THRESHOLD:
            return NextStep.STOP
        else:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

    # TODO: consider turn off cache partitioning
    def _monitoring_result(self) -> NextStep:
        metric_diff = self._foreground_wl.calc_metric_diff()

        curr_diff = metric_diff.l3_hit_ratio
        prev_diff = self._prev_metric_diff.l3_hit_ratio
        diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        if self._cur_step is not None \
                and not (ResCtrl.MIN_BITS < self._cur_step < ResCtrl.MAX_BITS) \
                or abs(diff_of_diff) <= CacheIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= CacheIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

        else:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN

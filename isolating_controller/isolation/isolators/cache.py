# coding: UTF-8

import logging
from typing import Optional, Tuple

from .base_isolator import Isolator
from .. import NextStep
from ...utils import ResCtrl, numa_topology
from ...workload import Workload


class CacheIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._prev_step: Optional[int] = None
        self._cur_step: Optional[int] = None

        self._stored_config: Tuple[str, ...] = None

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

        if self._cur_step is None:
            logger.info('CAT off')
            self.reset()

        else:
            logger.info(f'foreground : background = {self._cur_step} : {ResCtrl.MAX_BITS - self._cur_step}')

            # FIXME: hard coded -> The number of socket is two at most
            masks = [ResCtrl.MIN_MASK, ResCtrl.MIN_MASK]
            masks[self._foreground_wl.cur_socket_id()] = ResCtrl.gen_mask(0, self._cur_step)
            self._foreground_wl.resctrl.assign_llc(*masks)

            # FIXME: hard coded -> The number of socket is two at most
            masks = [ResCtrl.MIN_MASK, ResCtrl.MIN_MASK]
            masks[self._background_wl.cur_socket_id()] = ResCtrl.gen_mask(self._cur_step)
            self._background_wl.resctrl.assign_llc(*masks)

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

        if self.is_min_level or self.is_max_level \
                or abs(diff_of_diff) <= CacheIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= CacheIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            return NextStep.WEAKEN

        else:
            return NextStep.STRENGTHEN

    def reset(self) -> None:
        masks = [ResCtrl.MIN_MASK] * (max(numa_topology.cur_online_nodes()) + 1)

        if self._background_wl.is_running:
            bg_masks = masks.copy()
            bg_masks[self._background_wl.cur_socket_id()] = ResCtrl.MAX_MASK
            self._background_wl.resctrl.assign_llc(*bg_masks)

        if self._foreground_wl.is_running:
            masks[self._foreground_wl.cur_socket_id()] = ResCtrl.MAX_MASK
            self._foreground_wl.resctrl.assign_llc(*masks)

    def store_cur_config(self) -> None:
        fg_resctrl = self._foreground_wl.resctrl
        fg_mask = fg_resctrl.get_llc_mask()
        bg_resctrl = self._background_wl.resctrl
        bg_mask = bg_resctrl.get_llc_mask()
        self._stored_config = (fg_mask, bg_mask)

    def load_cur_config(self):
        return self._stored_config

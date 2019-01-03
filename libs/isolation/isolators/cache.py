# coding: UTF-8

import logging
from typing import Optional, Tuple

from .base import Isolator
from ...metric_container.basic_metric import MetricDiff
from ...utils import ResCtrl, numa_topology
from ...workload import Workload


class CacheIsolator(Isolator):
    def __init__(self, foreground_wl: Workload, background_wls: Tuple[Workload, ...]) -> None:
        super().__init__(foreground_wl, background_wls)

        self._prev_step: Optional[int] = None
        self._cur_step: Optional[int] = None

        self._stored_config: Optional[Tuple[int, int]] = None

    @classmethod
    def _get_metric_type_from(cls, metric_diff: MetricDiff) -> float:
        return metric_diff.l3_hit_ratio

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
        return self._cur_step is None or self._cur_step - ResCtrl.STEP < ResCtrl.MIN_BITS

    def enforce(self) -> None:
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
            masks[self._any_running_bg.cur_socket_id()] = ResCtrl.gen_mask(self._cur_step)
            for bg in self._all_running_bgs:
                bg.resctrl.assign_llc(*masks)

    def reset(self) -> None:
        masks = [ResCtrl.MIN_MASK] * (max(numa_topology.cur_online_nodes()) + 1)

        for bg in self._all_running_bgs:
            bg_masks = masks.copy()
            bg_masks[bg.cur_socket_id()] = ResCtrl.MAX_MASK
            bg.resctrl.assign_llc(*bg_masks)

        if self._foreground_wl.is_running:
            masks[self._foreground_wl.cur_socket_id()] = ResCtrl.MAX_MASK
            self._foreground_wl.resctrl.assign_llc(*masks)

    def store_cur_config(self) -> None:
        self._stored_config = (self._prev_step, self._cur_step)

    def load_cur_config(self) -> None:
        super().load_cur_config()

        self._prev_step, self._cur_step = self._stored_config
        self._stored_config = None

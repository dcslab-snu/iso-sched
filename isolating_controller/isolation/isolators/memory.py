# coding: UTF-8

import logging
from typing import Optional

from .base import Isolator
from ...metric_container.basic_metric import MetricDiff
from ...utils import DVFS
from ...workload import Workload


class MemoryIsolator(Isolator):
    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded
        self._cur_step: int = DVFS.MAX
        self._stored_config: Optional[int] = None

    @classmethod
    def _get_metric_type_from(cls, metric_diff: MetricDiff) -> float:
        return metric_diff.local_mem_util_ps

    def strengthen(self) -> 'MemoryIsolator':
        self._cur_step -= DVFS.STEP
        return self

    def weaken(self) -> 'MemoryIsolator':
        self._cur_step += DVFS.STEP
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step - DVFS.STEP < DVFS.MIN

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        return DVFS.MAX < self._cur_step + DVFS.STEP

    def enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'frequency of bound_cores {self._background_wl.bound_cores} is {self._cur_step / 1_000_000}GHz')

        DVFS.set_freq(self._cur_step, self._background_wl.bound_cores)

    def reset(self) -> None:
        DVFS.set_freq(DVFS.MAX, self._background_wl.orig_bound_cores)

    def store_cur_config(self) -> None:
        self._stored_config = self._cur_step

    def load_cur_config(self) -> None:
        super().load_cur_config()

        self._cur_step = self._stored_config
        self._stored_config = None

# coding: UTF-8

import logging
from typing import Optional, Tuple

from .base import Isolator
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class SchedIsolator(Isolator):
    def __init__(self, foreground_wl: Workload, background_wls: Tuple[Workload, ...]) -> None:
        super().__init__(foreground_wl, background_wls)

        # FIXME: hard coded
        self._cur_step = background_wls[0].orig_bound_cores[0]

        self._stored_config: Optional[int] = None

    @classmethod
    def _get_metric_type_from(cls, metric_diff: MetricDiff) -> float:
        return metric_diff.local_mem_util_ps

    def strengthen(self) -> 'SchedIsolator':
        self._cur_step += 1
        return self

    def weaken(self) -> 'SchedIsolator':
        self._cur_step -= 1
        return self

    @property
    def is_max_level(self) -> bool:
        return self._cur_step == self._background_wls[0].orig_bound_cores[-1]

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step - 1 == self._foreground_wl.bound_cores[-1]

    def enforce(self) -> None:
        logger = logging.getLogger(__name__)
        # FIXME: hard coded
        logger.info(f'affinity of background is {self._cur_step}-{self._background_wls[0].orig_bound_cores[-1]}')

        # FIXME: hard coded
        for bg in self._all_running_bgs:
            bg.bound_cores = range(self._cur_step, bg.orig_bound_cores[-1] + 1)

    def reset(self) -> None:
        for bg in self._all_running_bgs:
            bg.bound_cores = bg.orig_bound_cores

    def store_cur_config(self) -> None:
        self._stored_config = self._cur_step

    def load_cur_config(self) -> None:
        super().load_cur_config()

        self._cur_step = self._stored_config
        self._stored_config = None

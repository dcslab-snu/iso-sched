# coding: UTF-8

import logging
from typing import Optional, Tuple

from .base import Isolator
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class AffinityIsolator(Isolator):
    def __init__(self, foreground_wl: Workload, background_wls: Tuple[Workload, ...]) -> None:
        super().__init__(foreground_wl, background_wls)

        self._cur_step: int = self._foreground_wl.orig_bound_cores[-1]

        self._stored_config: Optional[int] = None

    @classmethod
    def _get_metric_type_from(cls, metric_diff: MetricDiff) -> float:
        return metric_diff.instruction_ps

    def strengthen(self) -> 'AffinityIsolator':
        self._cur_step += 1
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step + 1 >= self._any_running_bg.bound_cores[0]

    @property
    def is_min_level(self) -> bool:
        return self._foreground_wl.orig_bound_cores == self._foreground_wl.bound_cores

    def weaken(self) -> 'AffinityIsolator':
        self._cur_step -= 1
        return self

    def enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'affinity of foreground is {self._foreground_wl.orig_bound_cores[0]}-{self._cur_step}')

        self._foreground_wl.bound_cores = range(self._foreground_wl.orig_bound_cores[0], self._cur_step + 1)

    def reset(self) -> None:
        if self._foreground_wl.is_running:
            self._foreground_wl.bound_cores = self._foreground_wl.orig_bound_cores

    def store_cur_config(self) -> None:
        self._stored_config = self._cur_step

    def load_cur_config(self) -> None:
        super().load_cur_config()

        self._cur_step = self._stored_config
        self._stored_config = None

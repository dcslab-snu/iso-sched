# coding: UTF-8

import logging

from .base_policy import IsolationPolicy
from ..isolators import CacheIsolator, IdleIsolator, MemoryIsolator, SchedIsolator
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class DiffPolicy(IsolationPolicy):
    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        super().__init__(fg_wl, bg_wl)

        self._is_llc_isolated = False
        self._is_mem_isolated = False
        self._is_sched_isolated = False

    @property
    def new_isolator_needed(self) -> bool:
        return isinstance(self._cur_isolator, IdleIsolator)

    def _clear_flags(self) -> None:
        self._is_llc_isolated = False
        self._is_mem_isolated = False
        self._is_sched_isolated = False

    def choose_next_isolator(self) -> None:
        logger = logging.getLogger(__name__)
        logger.debug('looking for new isolation...')

        metric_diff: MetricDiff = self._fg_wl.calc_metric_diff()
        logger.info(repr(metric_diff))

        l3_hit_ratio = abs(metric_diff.l3_hit_ratio)
        local_mem_util = abs(metric_diff.local_mem_util)

        if self._is_sched_isolated and self._is_mem_isolated and self._is_llc_isolated:
            self._clear_flags()
            logger.debug('****All isolators are applicable for now!****')

        if not self._is_llc_isolated and l3_hit_ratio > local_mem_util:
            self._cur_isolator = self._isolator_map[CacheIsolator]
            self._is_llc_isolated = True
            logger.info(f'Cache Isolation for {self._fg_wl} is started')

        elif not self._is_mem_isolated and l3_hit_ratio < local_mem_util:
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._is_mem_isolated = True
            logger.info(f'Memory Bandwidth Isolation for {self._fg_wl} is started')

        elif not self._is_sched_isolated and l3_hit_ratio < local_mem_util:
            self._cur_isolator = self._isolator_map[SchedIsolator]
            self._is_sched_isolated = True
            logger.info(f'Cpuset Isolation for {self._fg_wl} is started')

        else:
            logger.debug('A new Isolator has not been selected.')

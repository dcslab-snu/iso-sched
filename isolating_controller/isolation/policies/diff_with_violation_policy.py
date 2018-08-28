# coding: UTF-8

import logging
from typing import Optional

from .diff_policy import DiffPolicy
from ..isolators import CacheIsolator, IdleIsolator, MemoryIsolator, SchedIsolator
from ...workload import Workload


class DiffWViolationPolicy(DiffPolicy):
    VIOLATION_THRESHOLD = 3

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        super().__init__(fg_wl, bg_wl)

        self._violation_count: Optional[int] = None

    def _check_violation(self) -> bool:
        metric_diff = self._fg_wl.calc_metric_diff()
        l3_hit_ratio = abs(metric_diff.l3_hit_ratio)
        local_mem_util = abs(metric_diff.local_mem_util)

        if l3_hit_ratio > local_mem_util and not isinstance(self._cur_isolator, CacheIsolator):
            return True
        elif l3_hit_ratio < local_mem_util and \
                (not isinstance(self._cur_isolator, MemoryIsolator)
                 and not isinstance(self._cur_isolator, SchedIsolator)):
            return True
        else:
            return False

    @property
    def new_isolator_needed(self) -> bool:
        if isinstance(self._cur_isolator, IdleIsolator):
            return True

        logger = logging.getLogger(self.__class__.__name__)

        if self._check_violation():
            logger.info(f'violation is occurred. current isolator type : {self._cur_isolator.__class__.__name__}')

            if self._violation_count is None:
                self._violation_count = 1

            else:
                self._violation_count += 1

                if self._violation_count > DiffWViolationPolicy.VIOLATION_THRESHOLD:
                    logger.info('new isolator is required due to violation')
                    self._clear_flags()
                    self._violation_count = None
                    return True

        return False

    def choose_next_isolator(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)

        metric_diff = self._fg_wl.calc_metric_diff()
        # TODO: change level to debug
        logger.info(f'diff is {metric_diff}')

        l3_hit_ratio = abs(metric_diff.l3_hit_ratio)
        local_mem_util = abs(metric_diff.local_mem_util)
        fg_name = self._fg_wl.name
        fg_pid = self._fg_wl.pid

        if self._is_sched_isolated and self._is_mem_isolated and self._is_llc_isolated:
            self._clear_flags()

        if not self._is_llc_isolated and l3_hit_ratio > local_mem_util:
            self._cur_isolator.yield_isolation()
            self._cur_isolator = self._isolator_map[CacheIsolator]
            self._is_llc_isolated = True
            self._violation_count = None
            logger.info(f'Cache Isolation for workload {fg_name} (pid: {fg_pid}) is started')

        elif not self._is_mem_isolated and l3_hit_ratio < local_mem_util:
            self._cur_isolator.yield_isolation()
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._is_mem_isolated = True
            self._violation_count = None
            logger.info(f'Memory Bandwidth Isolation for workload {fg_name} (pid: {fg_pid}) is started')

        elif not self._is_sched_isolated and l3_hit_ratio < local_mem_util:
            self._cur_isolator.yield_isolation()
            self._cur_isolator = self._isolator_map[SchedIsolator]
            self._is_sched_isolated = True
            self._violation_count = None
            logger.info(f'Cpuset Isolation for workload {fg_name} (pid: {fg_pid}) is started')

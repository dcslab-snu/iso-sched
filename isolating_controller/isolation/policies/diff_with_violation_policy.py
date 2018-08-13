# coding: UTF-8

import logging
from typing import Optional

from .diff_policy import DiffPolicy
from ..isolators import CacheIsolator, MemoryIsolator, SchedIsolator
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

        if l3_hit_ratio > local_mem_util and not isinstance(self._isolator, CacheIsolator):
            return True
        elif l3_hit_ratio < local_mem_util and \
                (not isinstance(self._isolator, MemoryIsolator) and not isinstance(self._isolator, SchedIsolator)):
            return True

    @property
    def new_isolator_needed(self) -> bool:
        if self._isolator is None:
            return True

        logger = logging.getLogger(self.__class__.__name__)

        if self._check_violation():
            logger.info(f'violation is occurred. current isolator type : {self._isolator.__class__.__name__}')

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

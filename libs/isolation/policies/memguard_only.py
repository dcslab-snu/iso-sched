# coding: UTF-8

import logging

from typing import Dict, Type
from .base import IsolationPolicy, Isolator
from .. import ResourceType
from ..isolators import IdleIsolator, MemoryIsolator, SchedIsolator
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class MemguardOnlyPolicy(IsolationPolicy):
    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        super().__init__(fg_wl, bg_wl)

        self._is_mem_isolated = False
        self._isolator_map: Dict[Type[Isolator], Isolator] = dict((
            (MemoryIsolator, MemoryIsolator(self._fg_wl, self._bg_wl)),))

    @property
    def new_isolator_needed(self) -> bool:
        return isinstance(self._cur_isolator, IdleIsolator)

    def contentious_resource(self):
        metric_diff: MetricDiff = self._fg_wl.calc_metric_diff()
        return ResourceType.MEMORY, metric_diff.local_mem_util_ps

    def choose_next_isolator(self) -> bool:
        logger = logging.getLogger(__name__)
        logger.debug('looking for new isolation...')

        resource, mem_diff = self.contentious_resource()

        if mem_diff < 0 and resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._is_mem_isolated = True
            logger.info(f'Starting {self._cur_isolator.__class__.__name__}...')
            return True

        #elif resource is ResourceType.MEMORY:
        #    self._cur_isolator = self._isolator_map[SchedIsolator]
        #    self._is_mem_isolated = False
        #    logger.info(f'Starting {self._cur_isolator.__class__.__name__}...')
        #    return True

        else:
            logger.debug('A new Isolator has not been selected.')
            return False

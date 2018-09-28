# coding: UTF-8

import logging

from .base_policy import IsolationPolicy
from .. import ResourceType
from ..isolators import CacheIsolator, CoreIsolator, IdleIsolator, MemoryIsolator
from ...workload import Workload


class GreedyDiffPolicy(IsolationPolicy):
    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        super().__init__(fg_wl, bg_wl)

        self._is_mem_isolated = False

    @property
    def new_isolator_needed(self) -> bool:
        return isinstance(self._cur_isolator, IdleIsolator)

    def choose_next_isolator(self) -> bool:
        logger = logging.getLogger(__name__)
        logger.debug('looking for new isolation...')

        resource: ResourceType = self.contentious_resource()

        if resource is ResourceType.CPU:
            self._cur_isolator = self._isolator_map[CoreIsolator]
            self._cur_isolator._contentious_resource = ResourceType.CPU
            logger.info(f'Core Isolation for {self._fg_wl} is started to isolate {ResourceType.CPU.name}s')
            return True

        elif resource is ResourceType.CACHE:
            self._cur_isolator = self._isolator_map[CacheIsolator]
            logger.info(f'Cache Isolation for {self._fg_wl} is started to isolate {ResourceType.CACHE.name}s')
            return True

        elif not self._is_mem_isolated and resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._is_mem_isolated = True
            logger.info(f'Memory Bandwidth Isolation for {self._fg_wl} is started '
                        f'to isolate {ResourceType.MEMORY.name} BW')
            return True

        elif resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[CoreIsolator]
            self._cur_isolator._contentious_resource = ResourceType.MEMORY
            self._is_mem_isolated = False
            logger.info(f'Cpuset Isolation for {self._fg_wl} is started to isolate {ResourceType.MEMORY.name} BW')
            return True

        else:
            logger.debug('A new Isolator has not been selected.')
            return False

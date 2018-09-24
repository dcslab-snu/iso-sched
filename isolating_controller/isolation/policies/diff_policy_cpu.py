# coding: UTF-8

import logging

from .. import ResourceType
from .base_policy import IsolationPolicy
from ..isolators import CacheIsolator, IdleIsolator, MemoryIsolator, CoreIsolator
from ...workload import Workload


class DiffCPUPolicy(IsolationPolicy):
    def __init__(self, fg_wl: Workload, bg_wl: Workload, skt_id: int) -> None:
        super().__init__(fg_wl, bg_wl, skt_id)

        self._is_llc_isolated = False
        self._is_mem_isolated = False
        self._is_core_isolated = False

    @property
    def new_isolator_needed(self) -> bool:
        return isinstance(self._cur_isolator, IdleIsolator)

    def _clear_flags(self) -> None:
        self._is_llc_isolated = False
        self._is_mem_isolated = False
        self._is_core_isolated = False

    def choose_next_isolator(self) -> bool:
        logger = logging.getLogger(__name__)
        logger.debug('looking for new isolation...')

        resource: ResourceType = self.contentious_resource()

        if resource is ResourceType.CPU:
            self._cur_isolator = self._isolator_map[CoreIsolator]
            self._cur_isolator._contentious_resource = ResourceType.CPU
            logger.info(f'Core Isolation for {self._fg_wl} is started to isolate {ResourceType.CPU.name}s')
            return True

        elif not self._is_llc_isolated and resource is ResourceType.CACHE:
            self._cur_isolator = self._isolator_map[CacheIsolator]
            self._is_llc_isolated = True
            logger.info(f'Cache Isolation for {self._fg_wl} is started to isolate {ResourceType.CACHE.name}s')
            return True

        elif not self._is_mem_isolated and resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._is_mem_isolated = True
            logger.info(f'Memory Bandwidth Isolation for {self._fg_wl} is started '
                        f'to isolate {ResourceType.MEMORY.name} BW')
            return True

        elif not self._is_core_isolated and resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[CoreIsolator]
            self._is_core_isolated = True
            self._cur_isolator._contentious_resource = ResourceType.MEMORY
            logger.info(f'Core Isolation for {self._fg_wl} is started to isolate {ResourceType.MEMORY.name} BW ')
            return True

        else:
            logger.debug('A new Isolator has not been selected.')
            return False

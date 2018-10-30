# coding: UTF-8

import logging

from .base import IsolationPolicy
from .. import ResourceType
from ..isolators import CacheIsolator, IdleIsolator, MemoryIsolator, SchedIsolator
from ...workload import Workload


class ConservativePolicy(IsolationPolicy):
    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        super().__init__(fg_wl, bg_wl)

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

        if self._is_core_isolated and self._is_mem_isolated and self._is_llc_isolated:
            self._clear_flags()
            logger.debug('****All isolators are applicable for now!****')

        if not self._is_llc_isolated and resource is ResourceType.CACHE:
            self._cur_isolator = self._isolator_map[CacheIsolator]
            self._is_llc_isolated = True
            logger.info(f'Cache Isolation for {self._fg_wl} is started')
            return True

        elif not self._is_mem_isolated and resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._is_mem_isolated = True
            logger.info(f'Memory Bandwidth Isolation for {self._fg_wl} is started')
            return True

        elif not self._is_core_isolated and resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[SchedIsolator]
            self._is_core_isolated = True
            logger.info(f'Core Isolation for {self._fg_wl} is started')
            return True

        else:
            logger.debug('A new Isolator has not been selected.')
            return False

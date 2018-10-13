# coding: UTF-8

import logging

from isolating_controller.isolation.isolators.affinity import AffinityIsolator
from .base_policy import IsolationPolicy
from .. import ResourceType
from ..isolators import CacheIsolator, IdleIsolator, MemoryIsolator, SchedIsolator
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

        # if foreground is web server (CPU critical)
        if len(self._fg_wl.bound_cores) < self._fg_wl.number_of_threads:
            if AffinityIsolator in self._isolator_map and not self._isolator_map[AffinityIsolator].is_max_level:
                self._cur_isolator = self._isolator_map[AffinityIsolator]
                logger.info(f'Starting {self._cur_isolator.__class__.__name__}...')
                return True

        resource: ResourceType = self.contentious_resource()

        if resource is ResourceType.CACHE:
            self._cur_isolator = self._isolator_map[CacheIsolator]
            logger.info(f'Starting {self._cur_isolator.__class__.__name__}...')
            return True

        elif not self._is_mem_isolated and resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._is_mem_isolated = True
            logger.info(f'Starting {self._cur_isolator.__class__.__name__}...')
            return True

        elif resource is ResourceType.MEMORY:
            self._cur_isolator = self._isolator_map[SchedIsolator]
            self._is_mem_isolated = False
            logger.info(f'Starting {self._cur_isolator.__class__.__name__}...')
            return True

        else:
            logger.debug('A new Isolator has not been selected.')
            return False

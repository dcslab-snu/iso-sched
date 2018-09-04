# coding: UTF-8
import logging
from abc import ABCMeta, abstractmethod
from enum import IntEnum
from typing import Mapping, Type

from isolating_controller.metric_container.basic_metric import MetricDiff
from ..isolators import CacheIsolator, IdleIsolator, Isolator, MemoryIsolator, SchedIsolator
from ...workload import Workload


class ResourceType(IntEnum):
    CACHE = 0
    MEMORY = 1


class IsolationPolicy(metaclass=ABCMeta):
    _IDLE_ISOLATOR: IdleIsolator = IdleIsolator()

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl

        self._isolator_map: Mapping[Type[Isolator], Isolator] = dict()
        self._cur_isolator: Isolator = IsolationPolicy._IDLE_ISOLATOR

    def __hash__(self) -> int:
        return self._fg_wl.pid

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} <fg: {self._fg_wl}, bg: {self._bg_wl}>'

    def init_isolators(self) -> None:
        self._isolator_map = dict((
            (CacheIsolator, CacheIsolator(self._fg_wl, self._bg_wl)),
            (MemoryIsolator, MemoryIsolator(self._fg_wl, self._bg_wl)),
            (SchedIsolator, SchedIsolator(self._fg_wl, self._bg_wl))
        ))

    @property
    @abstractmethod
    def new_isolator_needed(self) -> bool:
        pass

    @abstractmethod
    def choose_next_isolator(self) -> bool:
        pass

    def contentious_resource(self) -> ResourceType:
        metric_diff: MetricDiff = self._fg_wl.calc_metric_diff()

        logger = logging.getLogger(__name__)
        logger.info(repr(metric_diff))

        if metric_diff.local_mem_util > 0 and metric_diff.l3_hit_ratio > 0:
            if metric_diff.l3_hit_ratio > metric_diff.local_mem_util:
                return ResourceType.CACHE
            else:
                return ResourceType.MEMORY

        elif metric_diff.local_mem_util < 0 < metric_diff.l3_hit_ratio:
            return ResourceType.MEMORY

        elif metric_diff.l3_hit_ratio < 0 < metric_diff.local_mem_util:
            return ResourceType.CACHE

        else:
            if metric_diff.l3_hit_ratio > metric_diff.local_mem_util:
                return ResourceType.MEMORY
            else:
                return ResourceType.CACHE

    @property
    def foreground_workload(self) -> Workload:
        return self._fg_wl

    @property
    def background_workload(self) -> Workload:
        return self._bg_wl

    @property
    def ended(self) -> bool:
        return not self._fg_wl.is_running or not self._bg_wl.is_running

    @property
    def cur_isolator(self) -> Isolator:
        return self._cur_isolator

    @property
    def name(self) -> str:
        return f'{self._fg_wl.name}({self._fg_wl.pid})'

    def set_idle_isolator(self) -> None:
        self._cur_isolator.yield_isolation()
        self._cur_isolator = IsolationPolicy._IDLE_ISOLATOR

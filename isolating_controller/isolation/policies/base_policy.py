# coding: UTF-8
import logging
from abc import ABCMeta, abstractmethod
from typing import Mapping, Type

from isolating_controller.metric_container.basic_metric import MetricDiff, BasicMetric
from ..isolators import CacheIsolator, IdleIsolator, Isolator, MemoryIsolator, CoreIsolator
from ...workload import Workload
from .. import ResourceType


class IsolationPolicy(metaclass=ABCMeta):
    _IDLE_ISOLATOR: IdleIsolator = IdleIsolator()
    # FIXME : _CPU_THRESHOLD needs test
    _CPU_THRESHOLD = 0.1

    def __init__(self, fg_wl: Workload, bg_wl: Workload, skt_id: int) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl
        self._skt_id = skt_id

        self._isolator_map: Mapping[Type[Isolator], Isolator] = dict()
        self._cur_isolator: Isolator = IsolationPolicy._IDLE_ISOLATOR

    def __hash__(self) -> int:
        return self._fg_wl.pid

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} <fg: {self._fg_wl}, bg: {self._bg_wl}>'

    # FIXME: If you use policy without CPUIso., then changing ResourceType.Unknown to ResourceType.Memory
    def init_isolators(self) -> None:
        self._isolator_map = dict((
            (CacheIsolator, CacheIsolator(self._fg_wl, self._bg_wl, ResourceType.CACHE)),
            (MemoryIsolator, MemoryIsolator(self._fg_wl, self._bg_wl, ResourceType.MEMORY)),
            (CoreIsolator, CoreIsolator(self._fg_wl, self._bg_wl, ResourceType.Unknown))
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
        cur_metric: BasicMetric = self._fg_wl.metrics[0]

        logger = logging.getLogger(__name__)
        logger.info(repr(metric_diff))
        logger.info(f'l3_int: {cur_metric.l3_intensity}, mem_int: {cur_metric.mem_intensity}')
        if abs(cur_metric.l3_intensity) < IsolationPolicy._CPU_THRESHOLD \
                and abs(cur_metric.mem_intensity) < IsolationPolicy._CPU_THRESHOLD:
                return ResourceType.CPU

        if metric_diff.local_mem_util_ps > 0 and metric_diff.l3_hit_ratio > 0:
            if metric_diff.l3_hit_ratio > metric_diff.local_mem_util_ps:
                return ResourceType.CACHE
            else:
                return ResourceType.MEMORY

        elif metric_diff.local_mem_util_ps < 0 < metric_diff.l3_hit_ratio:
            return ResourceType.MEMORY

        elif metric_diff.l3_hit_ratio < 0 < metric_diff.local_mem_util_ps:
            return ResourceType.CACHE

        else:
            if metric_diff.l3_hit_ratio > metric_diff.local_mem_util_ps:
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

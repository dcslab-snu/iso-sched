# coding: UTF-8
import logging
from abc import ABCMeta, abstractmethod
from typing import Dict, Type

from .. import ResourceType
from ..isolators import CacheIsolator, CoreIsolator, IdleIsolator, Isolator, MemoryIsolator
from ...metric_container.basic_metric import BasicMetric, MetricDiff
from ...workload import Workload


class IsolationPolicy(metaclass=ABCMeta):
    _IDLE_ISOLATOR: IdleIsolator = IdleIsolator()
    # FIXME : _CPU_THRESHOLD needs test
    _CPU_THRESHOLD = 0.1

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl

        self._isolator_map: Dict[Type[Isolator], Isolator] = dict()
        self._cur_isolator: Isolator = IsolationPolicy._IDLE_ISOLATOR

        self._aggr_inst_diff: float = None

    def __hash__(self) -> int:
        return id(self)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} <fg: {self._fg_wl}, bg: {self._bg_wl}>'

    def __del__(self) -> None:
        isolators = tuple(self._isolator_map.keys())
        for isolator in isolators:
            del self._isolator_map[isolator]

    def init_isolators(self) -> None:
        self._isolator_map = dict((
            (CacheIsolator, CacheIsolator(self._fg_wl, self._bg_wl)),
            (MemoryIsolator, MemoryIsolator(self._fg_wl, self._bg_wl)),
            (CoreIsolator, CoreIsolator(self._fg_wl, self._bg_wl)),
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
        logger.info(f'l3_int: {cur_metric.l3_intensity}, mem_int: {cur_metric.mem_intensity}, llc_util: {cur_metric.l3_util}')
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

    @foreground_workload.setter
    def foreground_workload(self, new_workload: Workload):
        self._fg_wl = new_workload
        for isolator in self._isolator_map.values():
            isolator.change_fg_wl(new_workload)
            isolator.enforce()

    @property
    def background_workload(self) -> Workload:
        return self._bg_wl

    @background_workload.setter
    def background_workload(self, new_workload: Workload):
        self._bg_wl = new_workload
        for isolator in self._isolator_map.values():
            isolator.change_bg_wl(new_workload)
            isolator.enforce()

    @property
    def ended(self) -> bool:
        return not self._fg_wl.is_running or not self._bg_wl.is_running

    @property
    def cur_isolator(self) -> Isolator:
        return self._cur_isolator

    @property
    def name(self) -> str:
        return f'{self._fg_wl.name}({self._fg_wl.pid})'

    @property
    def aggr_inst(self) -> float:
        return self._aggr_inst_diff

    @property
    def most_cont_workload(self) -> Workload:
        fg_wl = self.foreground_workload
        bg_wl = self.background_workload

        fg_inst_diff = fg_wl.inst_diff
        bg_inst_diff = bg_wl.inst_diff

        # FIXME: Below condition is likely to fail due to too little differences between fg and bg
        if fg_inst_diff < bg_inst_diff:
            return fg_wl
        else:
            return bg_wl

    @property
    def least_cont_workload(self) -> Workload:
        fg_wl = self.foreground_workload
        bg_wl = self.background_workload

        fg_ipc_diff = fg_wl.inst_diff
        bg_ipc_diff = bg_wl.inst_diff

        # FIXME: Below condition is likely to fail due to too little differences between fg and bg
        if fg_ipc_diff > bg_ipc_diff:
            return fg_wl
        else:
            return bg_wl

    @property
    def least_mem_bw_workload(self) -> Workload:
        fg_wl = self.foreground_workload
        bg_wl = self.background_workload

        fg_mem_bw = fg_wl.metrics[0].local_mem_ps()
        bg_mem_bw = bg_wl.metrics[0].local_mem_ps()

        if fg_mem_bw > bg_mem_bw:
            return bg_wl
        else:
            return fg_wl

    # FIXME: replace to property
    def update_aggr_instr(self) -> None:
        fg_diff = self._fg_wl.calc_metric_diff()
        bg_diff = self._bg_wl.calc_metric_diff()
        self._fg_wl._ipc_diff = fg_diff.instruction_ps
        self._bg_wl._ipc_diff = bg_diff.instruction_ps
        self._aggr_inst_diff = fg_diff.instruction_ps + bg_diff.instruction_ps

    def set_idle_isolator(self) -> None:
        self._cur_isolator.yield_isolation()
        self._cur_isolator = IsolationPolicy._IDLE_ISOLATOR

    def reset(self) -> None:
        for isolator in self._isolator_map.values():
            isolator.reset()

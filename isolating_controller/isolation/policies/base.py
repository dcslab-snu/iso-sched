# coding: UTF-8

import logging
from abc import ABCMeta, abstractmethod
from typing import ClassVar, Dict, Tuple, Type

from .. import ResourceType
from ..isolators import CacheIsolator, IdleIsolator, Isolator, MemoryIsolator, SchedIsolator
from ..isolators.affinity import AffinityIsolator
from ...metric_container.basic_metric import BasicMetric, MetricDiff
from ...workload import Workload


class IsolationPolicy(metaclass=ABCMeta):
    _IDLE_ISOLATOR: ClassVar[IdleIsolator] = IdleIsolator()
    _VERIFY_THRESHOLD: ClassVar[int] = 3

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl

        self._isolator_map: Dict[Type[Isolator], Isolator] = dict((
            (CacheIsolator, CacheIsolator(self._fg_wl, self._bg_wl)),
            (AffinityIsolator, AffinityIsolator(self._fg_wl, self._bg_wl)),
            (SchedIsolator, SchedIsolator(self._fg_wl, self._bg_wl)),
            (MemoryIsolator, MemoryIsolator(self._fg_wl, self._bg_wl)),
        ))
        self._cur_isolator: Isolator = IsolationPolicy._IDLE_ISOLATOR

        self._aggr_inst_diff: float = None

        self._in_solorun_profile: bool = False
        self._cached_fg_num_threads: int = fg_wl.number_of_threads
        self._solorun_verify_violation_count: int = 0

    def __hash__(self) -> int:
        return id(self)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} <fg: {self._fg_wl}, bg: {self._bg_wl}>'

    def __del__(self) -> None:
        isolators = tuple(self._isolator_map.keys())
        for isolator in isolators:
            del self._isolator_map[isolator]

    @property
    @abstractmethod
    def new_isolator_needed(self) -> bool:
        pass

    @abstractmethod
    def choose_next_isolator(self) -> bool:
        pass

    def contentious_resource(self) -> ResourceType:
        return self.contentious_resources()[0][0]

    def contentious_resources(self) -> Tuple[Tuple[ResourceType, float], ...]:
        metric_diff: MetricDiff = self._fg_wl.calc_metric_diff()

        logger = logging.getLogger(__name__)
        logger.info(f'foreground : {metric_diff}')
        logger.info(f'background : {self._bg_wl.calc_metric_diff()}')

        resources = ((ResourceType.CACHE, metric_diff.l3_hit_ratio),
                     (ResourceType.MEMORY, metric_diff.local_mem_util_ps))

        if all(v > 0 for m, v in resources):
            return tuple(sorted(resources, key=lambda x: x[1], reverse=True))

        else:
            return tuple(sorted(resources, key=lambda x: x[1]))

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

    def set_idle_isolator(self) -> None:
        self._cur_isolator.yield_isolation()
        self._cur_isolator = IsolationPolicy._IDLE_ISOLATOR

    def reset(self) -> None:
        for isolator in self._isolator_map.values():
            isolator.reset()

    # Solorun profiling related

    @property
    def in_solorun_profiling(self) -> bool:
        return self._in_solorun_profile

    def start_solorun_profiling(self) -> None:
        """ profile solorun status of a workload """
        if self._in_solorun_profile:
            raise ValueError('Stop the ongoing solorun profiling first!')

        self._in_solorun_profile = True
        self._cached_fg_num_threads = self._fg_wl.number_of_threads
        self._solorun_verify_violation_count = 0

        # suspend all workloads and their perf agents
        self._bg_wl.pause()

        self._fg_wl.metrics.clear()

        # store current configuration
        for isolator in self._isolator_map.values():
            isolator.store_cur_config()
            isolator.reset()

    def stop_solorun_profiling(self) -> None:
        if not self._in_solorun_profile:
            raise ValueError('Start solorun profiling first!')

        logger = logging.getLogger(__name__)
        logger.debug(f'number of collected solorun data: {len(self._fg_wl.metrics)}')
        self._fg_wl.avg_solorun_data = BasicMetric.calc_avg(self._fg_wl.metrics)
        logger.debug(f'calculated average solorun data: {self._fg_wl.avg_solorun_data}')

        logger.debug('Enforcing restored configuration...')
        # restore stored configuration
        for isolator in self._isolator_map.values():
            isolator.load_cur_config()
            isolator.enforce()

        self._fg_wl.metrics.clear()

        self._bg_wl.resume()

        self._in_solorun_profile = False

    def profile_needed(self) -> bool:
        """
        This function checks if the profiling procedure should be called
        :return: Decision whether to initiate online solorun profiling
        """
        logger = logging.getLogger(__name__)

        if self._fg_wl.avg_solorun_data is None:
            logger.debug('initialize solorun data')
            return True

        if not self._fg_wl.calc_metric_diff().verify():
            self._solorun_verify_violation_count += 1

            if self._solorun_verify_violation_count == self._VERIFY_THRESHOLD:
                logger.debug(f'fail to verify solorun data. {{{self._fg_wl.calc_metric_diff()}}}')
                return True

        cur_num_threads = self._fg_wl.number_of_threads
        if cur_num_threads is not 0 and self._cached_fg_num_threads != cur_num_threads:
            logger.debug(f'number of threads. cached: {self._cached_fg_num_threads}, current : {cur_num_threads}')
            return True

        return False

    # Swapper related

    @property
    def safe_to_swap(self) -> bool:
        return not self._in_solorun_profile and len(self._fg_wl.metrics) > 0

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

        fg_mem_bw = fg_wl.metrics[0].local_mem_ps
        bg_mem_bw = bg_wl.metrics[0].local_mem_ps

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

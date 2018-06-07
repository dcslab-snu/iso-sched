# coding: UTF-8

import cpuinfo
import logging
import psutil
from typing import Deque, Optional, Tuple

from isolation.isolator import CacheIsolator, Isolator, MemoryIsolator, SchedIsolator
from metric_container.basic_metric import BasicMetric, MetricDiff
from solorun_data.datas import data_map

L3_SIZE = int(cpuinfo.get_cpu_info()['l3_cache_size'].split()[0]) * 1024


class Workload:
    """
    Workload class
    This class abstracts the process and contains the related metrics to represent its characteristics
    ControlThread schedules the groups of `Workload' instances to enforce their scheduling decisions
    """

    def __init__(self, name: str, pid: int, perf_pid: int, corun_metrics: Deque[BasicMetric], perf_interval: int):
        self._name = name
        self._pid = pid
        self._corun_metrics = corun_metrics
        self._perf_pid = perf_pid

        self._proc_info = psutil.Process(pid)

    def __str__(self):
        return 'Workload (pid: {}, perf_pid: {})'.format(self._pid, self._perf_pid)

    def __repr__(self):
        return self.__str__()

    @property
    def name(self) -> str:
        return self._name

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def corun_metrics(self):
        return self._corun_metrics

    @property
    def cpuset(self) -> Tuple[int, ...]:
        return tuple(self._proc_info.cpu_affinity())

    # TODO: remove
    @property
    def num_threads(self) -> int:
        return self._proc_info.num_threads()

    @property
    def perf_pid(self) -> int:
        return self._perf_pid

    @property
    def is_running(self) -> bool:
        return self._proc_info.is_running()


class BackgroundWorkload(Workload):
    pass


class ForegroundWorkload(Workload):
    """
    Workload class
    This class abstracts the process and contains the related metrics to represent its characteristics
    ControlThread schedules the groups of `Workload' instances to enforce their scheduling decisions
    """

    def __init__(self, name: str, pid: int, perf_pid: int, corun_metrics: Deque[BasicMetric],
                 perf_interval: int, background_wl: Optional[BackgroundWorkload] = None):
        super().__init__(name, pid, perf_pid, corun_metrics, perf_interval)

        self._isolator = None  # type: Optional[Isolator]
        self._background_wl = background_wl

        self._is_llc_isolated = False
        self._is_mem_isolated = False
        self._is_sched_isolated = False
        self._is_isolation_done = False

        self._last_isolation = None

    @property
    def background_workload(self) -> BackgroundWorkload:
        return self._background_wl

    @background_workload.setter
    def background_workload(self, workload: BackgroundWorkload):
        self._background_wl = workload

    @property
    def isolator(self) -> Optional[Isolator]:
        return self._isolator

    @isolator.setter
    def isolator(self, isolator: Isolator):
        self._isolator = isolator

        if isinstance(isolator, CacheIsolator):
            self._is_llc_isolated = True
            self._last_isolation = CacheIsolator

        elif isinstance(isolator, MemoryIsolator):
            self._is_mem_isolated = True
            self._last_isolation = MemoryIsolator

        elif isinstance(isolator, SchedIsolator):
            self._is_sched_isolated = True
            self._last_isolation = SchedIsolator

        elif isinstance(isolator, Isolator):
            # TODO
            pass

    def calc_metric_diff(self) -> MetricDiff:
        logger = logging.getLogger(self.__class__.__name__)

        solorun_data = data_map[self.name]
        curr_metric = self._corun_metrics.pop()  # type: BasicMetric

        return MetricDiff(solorun_data, curr_metric)

    @property
    def is_llc_isolated(self):
        return self._is_llc_isolated

    @property
    def is_mem_isolated(self):
        return self._is_mem_isolated

    @property
    def is_sched_isolated(self):
        return self._is_sched_isolated

    @property
    def is_isolation_done(self):
        return self._is_isolation_done

    def clear_flags(self):
        self._is_llc_isolated = True
        self._is_mem_isolated = True
        self._is_sched_isolated = True

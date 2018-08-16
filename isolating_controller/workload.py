# coding: UTF-8

from itertools import chain
from typing import Deque, Tuple

import cpuinfo
import psutil

from .metric_container.basic_metric import BasicMetric, MetricDiff
from .solorun_data.datas import data_map

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

    def calc_metric_diff(self) -> MetricDiff:
        solorun_data = data_map[self.name]
        curr_metric: BasicMetric = self._corun_metrics[0]

        return MetricDiff(curr_metric, solorun_data)

    def all_child_tid(self) -> Tuple[int, ...]:
        try:
            return tuple(chain(
                    (t.id for t in self._proc_info.threads()),
                    *((t.id for t in proc.threads()) for proc in self._proc_info.children(recursive=True))
            ))
        except psutil.NoSuchProcess:
            return tuple()

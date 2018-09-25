# coding: UTF-8

from collections import deque
from itertools import chain
from typing import Deque, Tuple, Set

import cpuinfo
import psutil

from .utils.numa_topology import NumaTopology
from .metric_container.basic_metric import BasicMetric, MetricDiff
from .solorun_data.datas import data_map


L3_SIZE = int(cpuinfo.get_cpu_info()['l3_cache_size'].split()[0]) * 1024


class Workload:
    """
    Workload class
    This class abstracts the process and contains the related metrics to represent its characteristics
    ControlThread schedules the groups of `Workload' instances to enforce their scheduling decisions
    """

    def __init__(self, name: str, wl_type: str, pid: int, perf_pid: int, perf_interval: int) -> None:
        self._name = name
        self._wl_type = wl_type
        self._pid = pid
        self._metrics: Deque[BasicMetric] = deque()
        self._perf_pid = perf_pid
        self._perf_interval = perf_interval

        self._proc_info = psutil.Process(pid)
        self._socket_id: int = None
        self._ipc_diff: float = None

    def __repr__(self) -> str:
        return f'{self._name} (pid: {self._pid})'

    @property
    def name(self) -> str:
        return self._name

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def wl_type(self) -> str:
        return self._wl_type

    @property
    def metrics(self) -> Deque[BasicMetric]:
        return self._metrics

    @property
    def socket_id(self) -> int:
        self._socket_id = self.get_socket_id()
        return self._socket_id

    @property
    def cpuset(self) -> Tuple[int, ...]:
        return tuple(self._proc_info.cpu_affinity())

    @property
    def perf_pid(self) -> int:
        return self._perf_pid

    @property
    def perf_interval(self):
        return self._perf_interval

    @property
    def is_running(self) -> bool:
        return self._proc_info.is_running()

    @property
    def ipc_diff(self) -> float:
        return self._ipc_diff

    def calc_metric_diff(self) -> MetricDiff:
        solorun_data = data_map[self.name]
        curr_metric: BasicMetric = self._metrics[0]

        return MetricDiff(curr_metric, solorun_data)

    def all_child_tid(self) -> Tuple[int, ...]:
        try:
            return tuple(chain(
                    (t.id for t in self._proc_info.threads()),
                    *((t.id for t in proc.threads()) for proc in self._proc_info.children(recursive=True))
            ))
        except psutil.NoSuchProcess:
            return tuple()
        
    def get_socket_id(self) -> int:
        cpuset: Set[int] = self.cpuset
        cpu_topo, _ = NumaTopology.get_numa_info()

        # FIXME: Hardcode for assumption (one workload to one socket)
        for socket_id, skt_cpus in cpu_topo.items():
            #print(f'cpuset: {cpuset}, socket_id: {socket_id}, skt_cpus: {skt_cpus}')
            for cpu_id in cpuset:
                if cpu_id in skt_cpus:
                    ret = socket_id
                    self._socket_id = ret
                    return ret

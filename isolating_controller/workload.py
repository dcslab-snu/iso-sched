# coding: UTF-8

from collections import deque
from itertools import chain
from typing import Deque, Iterable, Set, Tuple

import psutil

from .metric_container.basic_metric import BasicMetric, MetricDiff
from .solorun_data.datas import data_map
from .utils import ResCtrl, numa_topology
from .utils.cgroup import Cpu, CpuSet


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
        self._ipc_diff: float = None

        self._cgroup_cpuset = CpuSet(self.group_name)
        self._cgroup_cpu = Cpu(self.group_name)
        self._resctrl = ResCtrl(self.group_name)

        self._orig_bound_cores: Tuple[int, ...] = tuple(self._cgroup_cpuset.read_cpus())
        self._orig_bound_mems: Set[int] = self._cgroup_cpuset.read_mems()

    def __repr__(self) -> str:
        return f'{self._name} (pid: {self._pid})'

    def __hash__(self) -> int:
        return self._pid

    @property
    def cgroup_cpuset(self) -> CpuSet:
        return self._cgroup_cpuset

    @property
    def cgroup_cpu(self) -> Cpu:
        return self._cgroup_cpu

    @property
    def resctrl(self) -> ResCtrl:
        return self._resctrl

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
    def bound_cores(self) -> Tuple[int, ...]:
        return tuple(self._cgroup_cpuset.read_cpus())

    @bound_cores.setter
    def bound_cores(self, core_ids: Iterable[int]):
        self._cgroup_cpuset.assign_cpus(core_ids)

    @property
    def orig_bound_cores(self) -> Tuple[int, ...]:
        return self._orig_bound_cores

    @orig_bound_cores.setter
    def orig_bound_cores(self, orig_bound_cores: Tuple[int, ...]) -> None:
        self._orig_bound_cores = orig_bound_cores

    @property
    def bound_mems(self) -> Tuple[int, ...]:
        return tuple(self._cgroup_cpuset.read_mems())

    @bound_mems.setter
    def bound_mems(self, affinity: Iterable[int]):
        self._cgroup_cpuset.assign_mems(affinity)

    @property
    def orig_bound_mems(self) -> Set[int]:
        return self._orig_bound_mems

    @orig_bound_mems.setter
    def orig_bound_mems(self, orig_bound_mems: Set[int]) -> None:
        self._orig_bound_mems = orig_bound_mems

    @property
    def perf_interval(self):
        return self._perf_interval

    @property
    def is_running(self) -> bool:
        return self._proc_info.is_running()

    @property
    def ipc_diff(self) -> float:
        return self._ipc_diff

    @property
    def group_name(self) -> str:
        return f'{self.name}_{self.pid}'

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

    def cur_socket_id(self) -> int:
        sockets = frozenset(numa_topology.core_to_node[core_id] for core_id in self.bound_cores)

        # FIXME: hard coded
        if len(sockets) is not 1:
            raise NotImplementedError('Workload spans multiple sockets.')
        else:
            return next(iter(sockets))

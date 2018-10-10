# coding: UTF-8

from statistics import mean
from typing import Iterable

from cpuinfo import cpuinfo

LLC_SIZE = int(cpuinfo.get_cpu_info()['l3_cache_size'].split()[0]) * 1024


class BasicMetric:
    def __init__(self, l2miss, l3miss, inst, cycles, stall_cycles, wall_cycles, intra_coh,
                 inter_coh, llc_size, local_mem, remote_mem, interval):
        self._l2miss = l2miss
        self._l3miss = l3miss
        self._instructions = inst
        self._cycles = cycles
        self._stall_cycles = stall_cycles
        self._wall_cycles = wall_cycles
        self._intra_coh = intra_coh
        self._inter_coh = inter_coh
        self._llc_size = llc_size
        self._local_mem = local_mem
        self._remote_mem = remote_mem
        self._interval = interval

    @classmethod
    def calc_avg(cls, metrics: Iterable['BasicMetric']) -> 'BasicMetric':
        return BasicMetric(
                mean(metric._l2miss for metric in metrics),
                mean(metric._l3miss for metric in metrics),
                mean(metric._instructions for metric in metrics),
                mean(metric._cycles for metric in metrics),
                mean(metric._stall_cycles for metric in metrics),
                mean(metric._wall_cycles for metric in metrics),
                mean(metric._intra_coh for metric in metrics),
                mean(metric._inter_coh for metric in metrics),
                mean(metric._llc_size for metric in metrics),
                mean(metric._local_mem for metric in metrics),
                mean(metric._remote_mem for metric in metrics),
                mean(metric._interval for metric in metrics),
        )

    @property
    def l2miss(self):
        return self._l2miss

    @property
    def l3miss(self):
        return self._l3miss

    @property
    def instruction(self):
        return self._instructions

    @property
    def instruction_ps(self):
        return self._instructions * (1000 / self._interval)

    @property
    def wall_cycles(self):
        return self._wall_cycles

    @property
    def cycles(self):
        return self._cycles

    @property
    def stall_cycle(self):
        return self._stall_cycles

    @property
    def intra_coh(self):
        return self._intra_coh

    @property
    def inter_coh(self):
        return self._inter_coh

    @property
    def llc_size(self):
        return self._llc_size

    @property
    def local_mem(self) -> float:
        return self._local_mem

    @property
    def local_mem_ps(self) -> float:
        return self._local_mem * (1000 / self._interval)

    @property
    def remote_mem(self):
        return self._remote_mem

    @property
    def remote_mem_ps(self) -> float:
        return self._remote_mem * (1000 / self._interval)

    @property
    def ipc(self) -> float:
        return self._instructions / self._cycles

    @property
    def intra_coh_ratio(self) -> float:
        return self._intra_coh / self._l2miss

    @property
    def inter_coh_ratio(self) -> float:
        return self._inter_coh / self._l2miss

    @property
    def coh_ratio(self) -> float:
        return (self._inter_coh + self._intra_coh) / self._l2miss

    @property
    def l3miss_ratio(self) -> float:
        return self._l3miss / self._l2miss

    @property
    def l3hit_ratio(self) -> float:
        return 1 - self._l3miss / self._l2miss

    @property
    def l3_util(self) -> float:
        return self._llc_size / LLC_SIZE

    @property
    def l3_intensity(self) -> float:
        return self.l3_util * self.l3hit_ratio

    @property
    def mem_intensity(self) -> float:
        return self.l3_util * self.l3miss_ratio

    def __repr__(self) -> str:
        return ', '.join(map(str, (
            self._l2miss, self._l3miss, self._instructions, self._cycles, self._stall_cycles, self._wall_cycles,
            self._intra_coh, self._inter_coh, self._llc_size, self._local_mem, self._remote_mem, self._interval)))


class MetricDiff:
    def __init__(self, curr: BasicMetric, prev: BasicMetric) -> None:
        self._l3_hit_ratio = curr.l3hit_ratio - prev.l3hit_ratio
        self._local_mem_ps = curr.local_mem_ps / prev.local_mem_ps - 1
        self._remote_mem_ps = curr.remote_mem_ps / prev.remote_mem_ps - 1
        self._instruction_ps = curr.instruction_ps / prev.instruction_ps - 1

    @property
    def l3_hit_ratio(self) -> float:
        return self._l3_hit_ratio

    @property
    def local_mem_util_ps(self) -> float:
        return self._local_mem_ps

    @property
    def remote_mem_ps(self) -> float:
        return self._remote_mem_ps

    @property
    def instruction_ps(self) -> float:
        return self._instruction_ps

    def __repr__(self) -> str:
        return f'L3 hit ratio diff: {self._l3_hit_ratio:>6.03f}, ' \
               f'Local Memory access diff: {self._local_mem_ps:>6.03f}, ' \
               f'Instructions per sec. diff: {self._instruction_ps:>6.03f}'

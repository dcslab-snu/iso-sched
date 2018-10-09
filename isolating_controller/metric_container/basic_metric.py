# coding: UTF-8

from __future__ import division

from time import localtime, strftime

from cpuinfo import cpuinfo
from typing import Type

LLC_SIZE = int(cpuinfo.get_cpu_info()['l3_cache_size'].split()[0]) * 1024


class BasicMetric:
    def __init__(self, l2miss=0, l3miss=0, inst=0, cycles=0, stall_cycles=0, wall_cycles=0, intra_coh=0,
                 inter_coh=0, llc_size=0, local_mem=0, remote_mem=0, interval: int=50):
        self._l2miss = l2miss
        self._l3miss = l3miss
        self._instructions = inst
        self._wall_cycles = wall_cycles
        self._cycles = cycles
        self._stall_cycles = stall_cycles
        self._intra_coh = intra_coh
        self._inter_coh = inter_coh
        self._llc_size = llc_size
        self._local_mem = local_mem
        self._remote_mem = remote_mem
        self._interval = interval
        self._req_date = strftime("%I:%M:%S", localtime())

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

    def local_mem_ps(self) -> float:
        return self._local_mem * (1000 / self._interval)

    @property
    def remote_mem(self):
        return self._remote_mem

    def remote_mem_ps(self) -> float:
        return self._remote_mem * (1000 / self._interval)

    @property
    def req_date(self):
        return self._req_date

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
    def llc_util(self) -> float:
        return self._llc_size / LLC_SIZE

    @property
    def l3_intensity(self) -> float:
        return self.llc_util * self.l3hit_ratio

    @property
    def mem_intensity(self) -> float:
        return self.llc_util * self.l3miss_ratio

    @property
    def l3_util(self) -> float:
        return self.llc_util

    def __repr__(self) -> str:
        return ', '.join(map(str, (
            self._l2miss, self._l3miss, self._instructions, self._cycles, self._stall_cycles, self._wall_cycles,
            self._intra_coh, self._inter_coh, self._llc_size, self._local_mem, self._remote_mem,
            self._interval, self._req_date)))

    def __iadd__(self, others):
        self._l2miss = self.l2miss + others.l2miss
        self._l3miss = self.l3miss + others.l3miss
        self._instructions = self.instruction + others.instruction
        self._cycles = self._cycles + others.cycles
        self._stall_cycles = self.stall_cycle + others.stall_cycle
        self._wall_cycles = self.wall_cycles + others.wall_cycles
        self._intra_coh = self.intra_coh + others.intra_coh
        self._inter_coh = self.inter_coh + others.inter_coh
        self._llc_size = self.llc_size + others.llc_size
        self._local_mem = self.local_mem + others.local_mem
        self._remote_mem = self.remote_mem + others.remote_mem
        return self

    def __truediv__(self, other: int):
        self._l2miss /= other
        self._l3miss /= other
        self._instructions /= other
        self._cycles /= other
        self._stall_cycles /= other
        self._wall_cycles /= other
        self._intra_coh /= other
        self._inter_coh /= other
        self._llc_size /= other
        self._local_mem /= other
        self._remote_mem /= other
        return self


class MetricDiff:
    def __init__(self, curr: BasicMetric, prev: BasicMetric) -> None:
        self._l3_hit_ratio = curr.l3hit_ratio - prev.l3hit_ratio
        self._local_mem_ps = curr.local_mem_ps() / prev.local_mem_ps() - 1
        self._remote_mem_ps = curr.remote_mem_ps() / prev.remote_mem_ps() - 1
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

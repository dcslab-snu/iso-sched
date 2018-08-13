# coding: UTF-8

from time import localtime, strftime


class BasicMetric:
    def __init__(self, l2miss, l3miss_load, l3miss, inst, cycles, stall_cycles, tsc_rate,
                 intra_coh, inter_coh, llc_size, local_mem, remote_mem, req_num=None):
        self._l2miss = l2miss
        self._l3miss_load = l3miss_load
        self._l3miss = l3miss
        self._instructions = inst
        self._tsc_rate = tsc_rate
        self._cycles = cycles
        self._stall_cycles = stall_cycles
        self._intra_coh = intra_coh
        self._inter_coh = inter_coh
        self._llc_size = llc_size
        self._local_mem = local_mem
        self._remote_mem = remote_mem
        self._req_number = req_num
        self._req_date = strftime("%I:%M:%S", localtime())

    @property
    def l2miss(self):
        return self._l2miss

    @property
    def l3miss_load(self):
        return self._l3miss_load

    @property
    def l3miss(self):
        return self._l3miss

    @property
    def instruction(self):
        return self._instructions

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
    def remote_mem(self):
        return self._remote_mem

    @property
    def req_num(self):
        return self._req_number

    @property
    def req_date(self):
        return self._req_date

    @property
    def ipc(self):
        return self._instructions / self._cycles

    @property
    def intra_coh_ratio(self):
        return self._intra_coh / self._l2miss

    @property
    def inter_coh_ratio(self):
        return self._inter_coh / self._l2miss

    @property
    def coh_ratio(self):
        return (self._inter_coh + self._intra_coh) / self._l2miss

    @property
    def l3miss_ratio(self):
        return self._l3miss / self._l2miss

    @property
    def l3hit_ratio(self) -> float:
        return 1 - self._l3miss / self._l2miss

    @property
    def l3_intensity(self):
        l3_hit_ratio = 1 - self.l3miss_ratio
        return self._llc_size * l3_hit_ratio

    def __str__(self):
        return ', '.join(map(str, (
            self._l2miss, self._l3miss_load, self._l3miss, self._instructions, self._cycles, self._stall_cycles,
            self._intra_coh, self._inter_coh, self._llc_size, self._req_number, self._req_date)))

    def __repr__(self):
        return self.__str__()


class MetricDiff:
    # FIXME: hard coded
    _MAX_MEM_BANDWIDTH = 68 * 1024 * 1024 * 1024

    def __init__(self, curr: BasicMetric, prev: BasicMetric) -> None:
        self._l3_hit_ratio = curr.l3hit_ratio - prev.l3hit_ratio
        # FIXME: hard coded
        self._local_mem = curr.local_mem * 5 / self._MAX_MEM_BANDWIDTH - prev.local_mem * 5 / self._MAX_MEM_BANDWIDTH
        self._remote_mem = curr.remote_mem * 5 / self._MAX_MEM_BANDWIDTH - prev.remote_mem * 5 / self._MAX_MEM_BANDWIDTH

    @property
    def l3_hit_ratio(self):
        return self._l3_hit_ratio

    @property
    def local_mem_util(self):
        return self._local_mem

    @property
    def remote_mem(self):
        return self._remote_mem

    def __repr__(self) -> str:
        return f'L3 hit ratio: {self._l3_hit_ratio}, Local Memory access: {self._local_mem}'

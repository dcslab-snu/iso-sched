# coding: UTF-8

import subprocess
from pathlib import Path
from typing import Iterable, Dict
from itertools import chain
from isolating_controller.utils.cgroup import Cgroup


class DVFS:
    MIN = int(Path('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq').read_text())
    STEP = 100000
    MAX = int(Path('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq').read_text())

    def __init__(self, group_name, cpu_affinity):
        self._group_name: str = group_name
        self._cur_cgroup = Cgroup(self._group_name, 'cpuset,cpu')
        self._cpufreq: Dict[int, int] = dict()

        # FIXME: hard coded to max freq.
        self.set_freq_cgroup(DVFS.MAX)

    def set_freq_cgroup(self, target_freq: int):
        cur_grp_cpuset = self._cur_cgroup._get_cpu_affinity_from_group()
        DVFS.set_freq(target_freq, chain(cur_grp_cpuset))

    @property
    def cpufreq(self):
        return self._cpufreq

    def save_freq(self, freq: int):
        cpuset = self._cpufreq.keys()
        for cpu_id in cpuset:
            self._cpufreq[cpu_id] = freq

    @staticmethod
    def set_freq(freq: int, cores: Iterable[int]) -> None:
        for core in cores:
            subprocess.run(args=('sudo', 'tee', f'/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_max_freq'),
                           check=True, input=f'{freq}\n', encoding='ASCII', stdout=subprocess.DEVNULL)

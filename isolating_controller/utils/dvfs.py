# coding: UTF-8

import subprocess
from pathlib import Path
from typing import Iterable

from isolating_controller.utils.cgroup import CpuSet


class DVFS:
    MIN = int(Path('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq').read_text())
    STEP = 100000
    MAX = int(Path('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq').read_text())

    def __init__(self, group_name):
        self._group_name: str = group_name
        self._cur_cgroup = CpuSet(self._group_name)

    def set_freq_cgroup(self, target_freq: int):
        """
        Set the frequencies to current cgroup cpusets
        :param target_freq: freq. to set to cgroup cpuset
        :return:
        """
        DVFS.set_freq(target_freq, self._cur_cgroup.read_cpus())

    @staticmethod
    def set_freq(freq: int, cores: Iterable[int]) -> None:
        """
        Set the freq. to the specified cores
        :param freq: freq. to set
        :param cores:
        :return:
        """
        for core in cores:
            subprocess.run(args=('sudo', 'tee', f'/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_max_freq'),
                           check=True, input=f'{freq}\n', encoding='ASCII', stdout=subprocess.DEVNULL)

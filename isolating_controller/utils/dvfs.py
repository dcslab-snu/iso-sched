# coding: UTF-8

import subprocess
from typing import Iterable


class DVFS:
    MIN = int()
    STEP = 100000
    MAX = int()

    @staticmethod
    def set_freq(freq: int, cores: Iterable[int]):
        for core in cores:
            subprocess.run(args=('sudo', 'tee', f'/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_max_freq'),
                           check=True, input=f'{freq}\n', encoding='ASCII', stdout=subprocess.DEVNULL)


with open('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq') as fp:
    DVFS.MAX = int(fp.readline())

with open('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq') as fp:
    DVFS.MIN = int(fp.readline())

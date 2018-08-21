# coding: UTF-8

import subprocess
from pathlib import Path
from typing import Iterable


class DVFS:
    MIN = int(Path('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_min_freq').read_text())
    STEP = 100000
    MAX = int(Path('/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq').read_text())

    @staticmethod
    def set_freq(freq: int, cores: Iterable[int]) -> None:
        for core in cores:
            subprocess.run(args=('sudo', 'tee', f'/sys/devices/system/cpu/cpu{core}/cpufreq/scaling_max_freq'),
                           check=True, input=f'{freq}\n', encoding='ASCII', stdout=subprocess.DEVNULL)

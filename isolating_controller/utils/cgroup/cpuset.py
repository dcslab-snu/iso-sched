# coding: UTF-8


import subprocess
from typing import Iterable, Set

from .base import BaseCgroup


class CpuSet(BaseCgroup):
    CONTROLLER = 'cpuset'

    def assign_cpus(self, core_set: Iterable[int]) -> None:
        core_ids = ','.join(map(str, core_set))
        subprocess.check_call(args=('cgset', '-r', f'cpuset.cpus={core_ids}', self._group_name))

    def assign_mems(self, socket_set: Set[int]) -> None:
        mem_ids = ','.join(map(str, socket_set))
        subprocess.check_call(args=('cgset', '-r', f'cpuset.mems={mem_ids}', self._group_name))

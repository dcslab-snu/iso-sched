# coding: UTF-8


import subprocess
from typing import Iterable, Set

from .base import BaseCgroup
from ..hyphen import convert_to_set


class CpuSet(BaseCgroup):
    CONTROLLER = 'cpuset'

    def assign_cpus(self, core_set: Iterable[int]) -> None:
        core_ids = ','.join(map(str, core_set))
        subprocess.check_call(args=('cgset', '-r', f'cpuset.cpus={core_ids}', self._group_name))

    def assign_mems(self, socket_set: Iterable[int]) -> None:
        mem_ids = ','.join(map(str, socket_set))
        subprocess.check_call(args=('cgset', '-r', f'cpuset.mems={mem_ids}', self._group_name))

    def set_memory_migrate(self, flag: bool) -> None:
        subprocess.check_call(args=('cgset', '-r', f'cpuset.memory_migrate={int(flag)}', self._group_name))

    def read_cpus(self) -> Set[int]:
        cpus = subprocess.check_output(args=('cgget', '-nvr', 'bound_cores.cpus', self._group_name), encoding='ASCII')
        return convert_to_set(cpus)

    def read_mems(self) -> Set[int]:
        cpus = subprocess.check_output(args=('cgget', '-nvr', 'bound_cores.mems', self._group_name), encoding='ASCII')
        return convert_to_set(cpus)

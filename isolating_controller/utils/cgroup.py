# coding: UTF-8


import getpass
import grp
import os
import subprocess
from typing import Iterable, Optional, Set

from .hyphen import convert_to_set


# TODO: delete
class Cgroup:
    CPUSET_MOUNT_POINT = '/sys/fs/cgroup/cpuset'
    CPU_MOUNT_POINT = '/sys/fs/cgroup/cpu'

    def __init__(self, group_name: str, controllers: str) -> None:
        self._group_name: str = group_name
        self._controllers: str = controllers
        self._group_path: str = f'{controllers}:{group_name}'

    def create_group(self) -> None:
        uname: str = getpass.getuser()
        gid: int = os.getegid()
        gname: str = grp.getgrgid(gid).gr_name

        subprocess.check_call(args=(
            'sudo', 'cgcreate', '-a', f'{uname}:{gname}', '-d', '700', '-f',
            '600', '-t', f'{uname}:{gname}', '-s', '600', '-g', self._group_path))

    def assign_cpus(self, core_set: Iterable[int]) -> None:
        core_ids = ','.join(map(str, core_set))
        subprocess.check_call(args=('cgset', '-r', f'cpuset.cpus={core_ids}', self._group_name))

    def assign_mems(self, socket_set: Set[int]) -> None:
        mem_ids = ','.join(map(str, socket_set))
        subprocess.check_call(args=('cgset', '-r', f'cpuset.mems={mem_ids}', self._group_name))

    def _get_cpu_affinity_from_group(self) -> Set[int]:
        with open(f'{Cgroup.CPUSET_MOUNT_POINT}/{self._group_name}/cpuset.cpus', "r") as fp:
            line: str = fp.readline()
            core_set: Set[int] = convert_to_set(line)
        return core_set

    def limit_cpu_quota(self, limit_percentage: float, period: Optional[int] = None) -> None:
        if period is None:
            with open(f'{Cgroup.CPU_MOUNT_POINT}/cpu.cfs_period_us', "r") as fp:
                line: str = fp.readline()
                period = int(line)

        cpu_cores = self._get_cpu_affinity_from_group()
        quota = int(period * limit_percentage / 100 * len(cpu_cores))
        subprocess.check_call(args=('cgset', '-r', f'cpu.cfs_quota_us={quota}', self._group_name))

        subprocess.check_call(args=('cgset', '-r', f'cpu.cfs_period_us={period}', self._group_name))

    def add_tasks(self, pids: Iterable[int]) -> None:
        subprocess.check_call(args=('cgclassify', '-g', self._group_path, '--sticky', *map(str, pids)))

    def delete(self) -> None:
        subprocess.check_call(args=('sudo', 'cgdelete', '-r', '-g', self._group_path))

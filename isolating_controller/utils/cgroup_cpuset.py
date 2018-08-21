# coding: UTF-8

import subprocess
from typing import Set

import psutil


class CgroupCpuset:
    MOUNT_POINT = '/sys/fs/cgroup/cpuset'

    @staticmethod
    def create_group(name: str) -> None:
        subprocess.check_call(args=('sudo', 'mkdir', '-p', f'{CgroupCpuset.MOUNT_POINT}/{name}'))

    @staticmethod
    def add_task(name: str, pid: int) -> None:
        p = psutil.Process(pid)

        for thread in p.threads():
            subprocess.run(args=('sudo', 'tee', '-a', f'{CgroupCpuset.MOUNT_POINT}/{name}/tasks'),
                           input=f'{thread.id}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

        for child in p.children(True):
            for thread in child.threads():
                subprocess.run(args=('sudo', 'tee', '-a', f'{CgroupCpuset.MOUNT_POINT}/{name}/tasks'),
                               input=f'{thread.id}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    @staticmethod
    def remove_group(name: str) -> None:
        subprocess.check_call(args=('sudo', 'rmdir', f'/sys/fs/cgroup/cpuset/{name}'))

    @staticmethod
    def assign(group_name: str, core_set: Set[int]) -> None:
        subprocess.run(args=('sudo', 'tee', f'/sys/fs/cgroup/cpuset/{group_name}/cpuset.cpus'),
                       input=','.join(map(str, core_set)), check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    @staticmethod
    def convert_to_set(hyphen_str: str) -> Set[int]:
        ret = set()

        for elem in hyphen_str.split(','):
            group = tuple(map(int, elem.split('-')))

            if len(group) is 1:
                ret.add(group[0])
            elif len(group) is 2:
                ret.update(range(group[0], group[1] + 1))

        return ret

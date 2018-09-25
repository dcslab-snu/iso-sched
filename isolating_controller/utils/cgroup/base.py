# coding: UTF-8

import getpass
import grp
import os
import subprocess
from abc import ABCMeta
from typing import Iterable


class BaseCgroup(metaclass=ABCMeta):
    MOUNT_POINT = '/sys/fs/cgroup'
    CONTROLLER = str()

    def __init__(self, group_name: str) -> None:
        self._group_name: str = group_name
        self._group_path: str = f'{self.CONTROLLER}:{group_name}'

    def create_group(self) -> None:
        uname: str = getpass.getuser()
        gid: int = os.getegid()
        gname: str = grp.getgrgid(gid).gr_name

        subprocess.check_call(args=(
            'sudo', 'cgcreate', '-a', f'{uname}:{gname}', '-d', '755', '-f',
            '644', '-t', f'{uname}:{gname}', '-s', '644', '-g', self._group_path))

    def add_tasks(self, pids: Iterable[int]) -> None:
        subprocess.check_call(args=('cgclassify', '-g', self._group_path, '--sticky', *map(str, pids)))

    def delete(self) -> None:
        subprocess.check_call(args=('sudo', 'cgdelete', '-r', '-g', self._group_path))

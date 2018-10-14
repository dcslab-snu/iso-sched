# coding: UTF-8


import subprocess
from typing import ClassVar

from .base import BaseCgroup


class Cpu(BaseCgroup):
    CONTROLLER: ClassVar[str] = 'cpu'

    def limit_cpu_quota(self, quota: int, period: int) -> None:
        subprocess.check_call(args=('cgset', '-r', f'cpu.cfs_quota_us={quota}', self._group_name))
        subprocess.check_call(args=('cgset', '-r', f'cpu.cfs_period_us={period}', self._group_name))

# coding: UTF-8

import os
import psutil
import subprocess
from typing import Iterable, Optional


class CAT:
    MOUNT_POINT = '/sys/fs/resctrl'

    MIN = int()
    STEP = 1
    MAX = int()

    @staticmethod
    def create_group(name: str) -> None:
        subprocess.check_call(args=('sudo', 'mkdir', '-p', f'{CAT.MOUNT_POINT}/{name}'))

    @staticmethod
    def add_task(name: str, pid: int) -> None:
        p = psutil.Process(pid)
        for child in p.children(True):
            subprocess.run(args=('sudo', 'tee', '-a', f'{CAT.MOUNT_POINT}/{name}/tasks'),
                           input=f'{child.pid}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    @staticmethod
    def remove_group(name: str) -> None:
        subprocess.check_call(args=('sudo', 'rmdir', f'{CAT.MOUNT_POINT}/{name}'))

    @staticmethod
    def assign(name: str, *masks: Iterable[str]) -> None:
        masks = (f'{i}={mask}' for i, mask in enumerate(masks))
        mask = ';'.join(masks)
        subprocess.run(args=('sudo', 'tee', f'{CAT.MOUNT_POINT}/{name}/schemata'),
                       input=f'L3:{mask}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    @staticmethod
    def len_of_mask(mask: str) -> int:
        cnt = 0
        num = int(mask, 16)

        while num is not 0:
            cnt += 1
            num >>= 1

        return cnt

    @staticmethod
    def gen_mask(start: int, end: Optional[int] = None) -> str:
        if end is None or end > CAT.MAX:
            end = CAT.MAX

        if start < 0:
            raise ValueError('start must be greater than 0')

        return format(((1 << (end - start)) - 1) << (CAT.MAX - end), 'x')


if not os.path.ismount(CAT.MOUNT_POINT):
    subprocess.check_call(args=('sudo', 'mount', '-t', 'resctrl', 'resctrl', CAT.MOUNT_POINT))

with open(f'{CAT.MOUNT_POINT}/info/L3/min_cbm_bits') as fp:
    CAT.MIN = int(fp.readline())

with open(f'{CAT.MOUNT_POINT}/info/L3/cbm_mask') as fp:
    CAT.MAX = CAT.len_of_mask(fp.readline())

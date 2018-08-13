# coding: UTF-8

import os
import subprocess
from pathlib import Path
from typing import Iterable, Optional


def len_of_mask(mask: str) -> int:
    cnt = 0
    num = int(mask, 16)
    while num is not 0:
        cnt += 1
        num >>= 1
    return cnt


class CAT:
    MOUNT_POINT = Path('/sys/fs/resctrl')

    MIN = int((MOUNT_POINT / 'info' / 'L3' / 'min_cbm_bits').read_text())
    STEP = 1
    MAX = len_of_mask((MOUNT_POINT / 'info' / 'L3' / 'cbm_mask').read_text())

    @staticmethod
    def create_group(name: str) -> None:
        subprocess.check_call(args=('sudo', 'mkdir', '-p', str(CAT.MOUNT_POINT / name)))

    @staticmethod
    def add_task(name: str, pid: int) -> None:
        subprocess.run(args=('sudo', 'tee', '-a', str(CAT.MOUNT_POINT / name / 'tasks')),
                       input=f'{pid}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    @staticmethod
    def remove_group(name: str) -> None:
        subprocess.check_call(args=('sudo', 'rmdir', str(CAT.MOUNT_POINT / name)))

    @staticmethod
    def assign(name: str, *masks: Iterable[str]) -> None:
        masks = (f'{i}={mask}' for i, mask in enumerate(masks))
        mask = ';'.join(masks)
        subprocess.run(args=('sudo', 'tee', str(CAT.MOUNT_POINT / name / 'schemata')),
                       input=f'L3:{mask}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    @staticmethod
    def gen_mask(start: int, end: Optional[int] = None) -> str:
        if end is None or end > CAT.MAX:
            end = CAT.MAX

        if start < 0:
            raise ValueError('start must be greater than 0')

        return format(((1 << (end - start)) - 1) << (CAT.MAX - end), 'x')


if not os.path.ismount(str(CAT.MOUNT_POINT)):
    subprocess.check_call(args=('sudo', 'mount', '-t', 'resctrl', 'resctrl', str(CAT.MOUNT_POINT)))

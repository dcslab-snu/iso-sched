# coding: UTF-8

import subprocess
from pathlib import Path


def len_of_mask(mask: str) -> int:
    cnt = 0
    num = int(mask, 16)
    while num is not 0:
        cnt += 1
        num >>= 1
    return cnt


def bits_to_mask(bits: int) -> str:
    return f'{bits:x}'


class ResCtrl:
    MOUNT_POINT: Path = Path('/sys/fs/resctrl')
    MAX_MASK: str = Path('/sys/fs/resctrl/info/L3/cbm_mask').read_text(encoding='ASCII').strip()
    MAX_BITS: int = len_of_mask((MOUNT_POINT / 'info' / 'L3' / 'cbm_mask').read_text())
    MIN_BITS: int = int((MOUNT_POINT / 'info' / 'L3' / 'min_cbm_bits').read_text())
    MIN_MASK: str = bits_to_mask(MIN_BITS)
    STEP = 1

    def __init__(self, group_name: str) -> None:
        self._group_name: str = group_name
        self._group_path: Path = ResCtrl.MOUNT_POINT / f'{group_name}'

    @property
    def group_name(self):
        return self._group_name

    @group_name.setter
    def group_name(self, new_name):
        self._group_name = new_name
        self._group_path: Path = ResCtrl.MOUNT_POINT / new_name

    def add_task(self, pid: int) -> None:
        subprocess.run(args=('sudo', 'tee', str(self._group_path / 'tasks')),
                       input=f'{pid}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    def assign_llc(self, *masks: str) -> None:
        masks = (f'{i}={mask}' for i, mask in enumerate(masks))
        mask = ';'.join(masks)
        subprocess.run(args=('sudo', 'tee', str(ResCtrl.MOUNT_POINT / self._group_name / 'schemata')),
                       input=f'L3:{mask}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    @staticmethod
    def gen_mask(start: int, end: int = None) -> str:
        if end is None or end > ResCtrl.MAX_BITS:
            end = ResCtrl.MAX_BITS

        if start < 0:
            raise ValueError('start must be greater than 0')

        return format(((1 << (end - start)) - 1) << (ResCtrl.MAX_BITS - end), 'x')

    def remove_group(self) -> None:
        subprocess.check_call(args=('sudo', 'rmdir', str(ResCtrl.MOUNT_POINT / self._group_name)))

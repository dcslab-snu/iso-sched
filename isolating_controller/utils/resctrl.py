# coding: UTF-8

import re
import subprocess
from pathlib import Path
from typing import List, Pattern, Tuple


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
    _read_regex: Pattern = re.compile(r'L3:((\d+=[0-9a-fA-F]+;?)*)', re.MULTILINE)

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
        # subprocess.check_call('ls -ll /sys/fs/resctrl/', shell=True)
        subprocess.run(args=('sudo', 'tee', str(self._group_path / 'schemata')),
                       input=f'L3:{mask}\n', check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    def read_assigned_llc(self) -> Tuple[int, ...]:
        schemata = self._group_path / 'schemata'
        if not schemata.is_file():
            raise ProcessLookupError()

        with schemata.open() as fp:
            content: str = fp.read().strip()

        l3_schemata = ResCtrl._read_regex.search(content).group(1)

        # example: [('0', '00fff'), ('1', 'fff00')]
        pairs: List[Tuple[str, str]] = sorted(tuple(pair.split('=')) for pair in l3_schemata.split(';'))
        return tuple(len_of_mask(mask) for socket, mask in pairs)

    @staticmethod
    def gen_mask(start: int, end: int = None) -> str:
        if end is None or end > ResCtrl.MAX_BITS:
            end = ResCtrl.MAX_BITS

        if start < 0:
            raise ValueError('start must be greater than 0')

        return format(((1 << (end - start)) - 1) << (ResCtrl.MAX_BITS - end), 'x')

    def remove_group(self) -> None:
        subprocess.check_call(args=('sudo', 'rmdir', str(self._group_path)))

    def get_llc_mask(self) -> List[str]:
        """
        :return: `socket_masks` which is the elements of list in hex_str
        """
        proc = subprocess.Popen(['cat', f'{ResCtrl.MOUNT_POINT}/{self._group_name}/schemata'],
                                stdout=subprocess.PIPE)
        line = proc.communicate()[0].decode().lstrip()
        striped_schema_line = line.lstrip('L3:').rstrip('\n').split(';')
        socket_masks = list()
        for i, item in enumerate(striped_schema_line):
            mask = item.lstrip(f'{i}=')
            socket_masks.append(mask)
        return socket_masks

    @staticmethod
    def get_llc_bits_from_mask(input_list: List[str]) -> List[int]:
        """
        :param input_list: Assuming the elements of list is hex_str such as "0xfffff"
        :return:
        """
        output_list = list()
        for mask in input_list:
            hex_str = mask
            hex_int = int(hex_str, 16)
            bin_tmp = bin(hex_int)
            llc_bits = len(bin_tmp.lstrip('0b'))
            output_list.append(llc_bits)
        return output_list

    def read_llc_bits(self) -> int:
        socket_masks = self.get_llc_mask()
        llc_bits_list = ResCtrl.get_llc_bits_from_mask(socket_masks)
        ret_llc_bits = 0
        for llc_bits in llc_bits_list:
            ret_llc_bits += llc_bits
        return ret_llc_bits

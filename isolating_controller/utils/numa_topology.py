# coding: UTF-8

from pathlib import Path
from typing import Dict, Set, Tuple

import aiofiles

from .hyphen import convert_to_set


class NumaTopology:
    BASE_PATH: Path = Path('/sys/devices/system/node')

    @staticmethod
    async def get_node_topo() -> Set[int]:
        online_path: Path = NumaTopology.BASE_PATH / 'online'

        async with aiofiles.open(online_path) as fp:
            line: str = await fp.readline()
            node_list = convert_to_set(line)

        return node_list

    @staticmethod
    async def get_cpu_topo(node_list: Set[int]) -> Dict[int, Set[int]]:
        cpu_topo: Dict[int, Set[int]] = dict()

        for num in node_list:
            cpulist_path: Path = NumaTopology.BASE_PATH / f'node{num}/cpulist'

            async with aiofiles.open(cpulist_path) as fp:
                line: str = await fp.readline()
                cpu_topo[num] = convert_to_set(line)

        return cpu_topo

    @staticmethod
    async def get_mem_topo() -> Set[int]:
        has_memory_path = NumaTopology.BASE_PATH / 'has_memory'

        async with aiofiles.open(has_memory_path) as fp:
            line: str = await fp.readline()
            mem_topo = convert_to_set(line)

            # TODO: get_mem_topo can be enhanced by using real numa memory access latency

        return mem_topo

    @staticmethod
    async def get_numa_info() -> Tuple[Dict[int, Set[int]], Set[int]]:
        node_list = await NumaTopology.get_node_topo()
        cpu_topo = await NumaTopology.get_cpu_topo(node_list)
        mem_topo = await NumaTopology.get_mem_topo()
        return cpu_topo, mem_topo

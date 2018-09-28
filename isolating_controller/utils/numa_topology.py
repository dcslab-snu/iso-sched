# coding: UTF-8

from pathlib import Path
from typing import Dict, Mapping, Set

from .hyphen import convert_to_set

_BASE_PATH: Path = Path('/sys/devices/system/node')


def get_mem_topo() -> Set[int]:
    has_memory_path = _BASE_PATH / 'has_memory'

    with has_memory_path.open() as fp:
        line: str = fp.readline()
        mem_topo = convert_to_set(line)

        # TODO: get_mem_topo can be enhanced by using real numa memory access latency

    return mem_topo


def cur_online_nodes() -> Set[int]:
    online_path: Path = _BASE_PATH / 'online'

    with online_path.open() as fp:
        line: str = fp.readline()
        node_list = convert_to_set(line)

    return node_list


def core_belongs_to(socket_id: int) -> Set[int]:
    cpulist_path: Path = _BASE_PATH / f'node{socket_id}/cpulist'

    with cpulist_path.open() as fp:
        line: str = fp.readline()
        return convert_to_set(line)


def _node_to_core() -> Dict[int, Set[int]]:
    node_list = cur_online_nodes()
    return dict((socket_id, core_belongs_to(socket_id)) for socket_id in node_list)


def _core_to_node() -> Dict[int, int]:
    ret_dict: Dict[int, int] = dict()
    node_list = cur_online_nodes()

    for socket_id in node_list:
        for core_id in core_belongs_to(socket_id):
            ret_dict[core_id] = socket_id

    return ret_dict


node_to_core: Mapping[int, Set[int]] = _node_to_core()  # key: socket id, value: corresponding core ids
core_to_node: Mapping[int, int] = _core_to_node()  # key: core id, value: corresponding socket id

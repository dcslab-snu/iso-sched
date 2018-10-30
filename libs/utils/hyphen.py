# coding: UTF-8

from typing import Iterable, Set


def convert_to_set(hyphen_str: str) -> Set[int]:
    ret = set()

    for elem in hyphen_str.split(','):
        group = tuple(map(int, elem.split('-')))

        if len(group) is 1:
            ret.add(group[0])
        elif len(group) is 2:
            ret.update(range(group[0], group[1] + 1))

    return ret


def convert_to_hyphen(core_ids: Iterable[int]) -> str:
    # TODO
    return ','.join(map(str, set(core_ids)))

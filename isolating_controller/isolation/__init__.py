# coding: UTF-8

from enum import IntEnum


class NextStep(IntEnum):
    STRENGTHEN = 1
    WEAKEN = 2
    STOP = 3
    IDLE = 4


class ResourceType(IntEnum):
    CPU = 0
    CACHE = 1
    MEMORY = 2
    Unknown = 3

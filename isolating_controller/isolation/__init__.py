# coding: UTF-8

from enum import IntEnum


class NextStep(IntEnum):
    STRENGTHEN = 1
    WEAKEN = 2
    STOP = 3
    IDLE = 4

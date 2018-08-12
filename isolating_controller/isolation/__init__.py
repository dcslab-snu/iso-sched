# coding: UTF-8

from enum import IntEnum


class IsolationPhase(IntEnum):
    ENFORCING = 1
    MONITORING = 2
    IDLE = 3


class IsolationResult(IntEnum):
    INCREASE = 1
    DECREASE = 2
    STOP = 3

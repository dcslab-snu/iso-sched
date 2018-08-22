# coding: UTF-8

from .base_isolator import Isolator
from .. import NextStep


class IdleIsolator(Isolator):
    def __init__(self) -> None:
        pass

    def enforce(self) -> None:
        pass

    def _monitoring_result(self) -> NextStep:
        return NextStep.IDLE

    def strengthen(self) -> 'IdleIsolator':
        return self

    def weaken(self) -> 'IdleIsolator':
        return self

    @property
    def is_max_level(self) -> bool:
        return True

    @property
    def is_min_level(self) -> bool:
        return False

    def _enforce(self) -> None:
        pass

# coding: UTF-8

from .base_isolator import Isolator
from .. import NextStep


class IdleIsolator(Isolator):
    def __init__(self) -> None:
        pass

    def strengthen(self) -> 'Isolator':
        pass

    @property
    def is_max_level(self) -> bool:
        return True

    @property
    def is_min_level(self) -> bool:
        return False

    def weaken(self) -> 'Isolator':
        pass

    def _enforce(self) -> None:
        pass

    def _try_scheduled(self) -> NextStep:
        return NextStep.IDLE

    def monitoring_result(self) -> NextStep:
        return self._monitoring_result()

    def _monitoring_result(self) -> NextStep:
        return NextStep.IDLE

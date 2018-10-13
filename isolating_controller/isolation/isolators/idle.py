# coding: UTF-8

from .base import Isolator
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

    def enforce(self) -> None:
        pass

    def _first_decision(self, _) -> NextStep:
        self._fg_next_step = NextStep.IDLE
        self._bg_next_step = NextStep.IDLE
        return NextStep.IDLE

    def decide_next_step(self) -> NextStep:
        return self._monitoring_result()

    def _monitoring_result(self, **kwargs) -> NextStep:
        self._fg_next_step = NextStep.IDLE
        self._bg_next_step = NextStep.IDLE
        return NextStep.IDLE

    def reset(self) -> None:
        pass

    def store_cur_config(self) -> None:
        pass

    def load_cur_config(self) -> None:
        pass

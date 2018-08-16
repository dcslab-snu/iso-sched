# coding: UTF-8

from .base_isolator import Isolator
from .. import NextStep


class IdleIsolator(Isolator):
    def __init__(self) -> None:
        pass

    def enforce(self) -> None:
        pass

    def monitoring_result(self) -> NextStep:
        pass

    def increase(self) -> 'IdleIsolator':
        return self

    def decrease(self) -> 'IdleIsolator':
        return self

    def _enforce(self) -> None:
        pass

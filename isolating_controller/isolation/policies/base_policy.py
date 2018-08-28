# coding: UTF-8

from abc import ABCMeta, abstractmethod
from typing import Mapping, Type

from ..isolators import CacheIsolator, IdleIsolator, Isolator, MemoryIsolator, SchedIsolator
from ...workload import Workload


class IsolationPolicy(metaclass=ABCMeta):
    _IDLE_ISOLATOR: IdleIsolator = IdleIsolator()

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl

        self._isolator_map: Mapping[Type[Isolator], Isolator] = dict()
        self._cur_isolator: Isolator = IsolationPolicy._IDLE_ISOLATOR

    def __hash__(self) -> int:
        return self._fg_wl.pid

    def init_isolators(self) -> None:
        self._isolator_map = dict((
            (CacheIsolator, CacheIsolator(self._fg_wl, self._bg_wl)),
            (MemoryIsolator, MemoryIsolator(self._fg_wl, self._bg_wl)),
            (SchedIsolator, SchedIsolator(self._fg_wl, self._bg_wl))
        ))

    @property
    @abstractmethod
    def new_isolator_needed(self) -> bool:
        pass

    @abstractmethod
    def choose_next_isolator(self) -> None:
        pass

    @property
    def foreground_workload(self) -> Workload:
        return self._fg_wl

    @property
    def background_workload(self) -> Workload:
        return self._bg_wl

    @property
    def ended(self) -> bool:
        return not self._fg_wl.is_running or not self._bg_wl.is_running

    @property
    def cur_isolator(self) -> Isolator:
        return self._cur_isolator

    @property
    def name(self) -> str:
        return f'{self._fg_wl.name}({self._fg_wl.pid})'

    def set_idle_isolator(self) -> None:
        self._cur_isolator.yield_isolation()
        self._cur_isolator = IsolationPolicy._IDLE_ISOLATOR

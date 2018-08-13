# coding: UTF-8

from abc import ABCMeta, abstractmethod
from typing import Mapping, Type

from .. import IsolationPhase
from ..isolators import CacheIsolator, IdleIsolator, Isolator, MemoryIsolator, SchedIsolator
from ...workload import Workload


class IsolationPolicy(metaclass=ABCMeta):
    IDLE_ISOLATOR = IdleIsolator()

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl

        self._iteration_num: int = 0
        self._isolator_map: Mapping[Type[Isolator], Isolator] = dict((
            (IdleIsolator, IdleIsolator()),
            (CacheIsolator, CacheIsolator(fg_wl, bg_wl)),
            (MemoryIsolator, MemoryIsolator(fg_wl, bg_wl)),
            (SchedIsolator, SchedIsolator(fg_wl, bg_wl))
        ))
        self._cur_isolator: Isolator = self._isolator_map[IdleIsolator]

    @property
    @abstractmethod
    def new_isolator_needed(self) -> bool:
        pass

    @abstractmethod
    def choose_next_isolator(self) -> None:
        pass

    def isolate(self) -> None:
        self._isolate()
        self._iteration_num += 1

    @abstractmethod
    def _isolate(self) -> None:
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
    def iteration_num(self) -> int:
        return self._iteration_num

    @property
    def name(self) -> str:
        return f'{self._fg_wl.name}({self._fg_wl.pid})'

    @property
    def current_phase(self) -> IsolationPhase:
        return self._cur_isolator.next_phase

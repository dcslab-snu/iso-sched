# coding: UTF-8

from abc import ABCMeta, abstractmethod
from typing import Optional

from ..isolators import IdleIsolator, Isolator
from ...workload import Workload


class IsolationPolicy(metaclass=ABCMeta):
    IDLE_ISOLATOR = IdleIsolator()

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl

        self._isolator: Optional[Isolator] = None

    @property
    @abstractmethod
    def new_isolator_needed(self) -> bool:
        pass

    @abstractmethod
    def choose_next_isolator(self) -> None:
        pass

    @abstractmethod
    def isolate(self) -> None:
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
    def isolator(self):
        return self._isolator

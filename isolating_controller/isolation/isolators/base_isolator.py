# coding: UTF-8

from abc import ABCMeta, abstractmethod
from typing import Optional

from .. import NextStep
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class Isolator(metaclass=ABCMeta):
    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        self._prev_metric_diff: Optional[MetricDiff] = None

        self._foreground_wl = foreground_wl
        self._background_wl = background_wl

    @abstractmethod
    def strengthen(self) -> 'Isolator':
        pass

    @abstractmethod
    def weaken(self) -> 'Isolator':
        pass

    @abstractmethod
    def _enforce(self) -> None:
        pass

    def enforce(self) -> None:
        self._prev_metric_diff: MetricDiff = self._foreground_wl.calc_metric_diff()

        self._enforce()

    @abstractmethod
    def monitoring_result(self) -> NextStep:
        pass

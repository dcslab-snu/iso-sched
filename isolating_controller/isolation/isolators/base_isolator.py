# coding: UTF-8

from abc import ABCMeta, abstractmethod

from .. import NextStep
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class Isolator(metaclass=ABCMeta):
    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        self._prev_metric_diff: MetricDiff = foreground_wl.calc_metric_diff()

        self._foreground_wl = foreground_wl
        self._background_wl = background_wl

        self._is_fist_decision: bool = True

    @abstractmethod
    def strengthen(self) -> 'Isolator':
        pass

    @property
    @abstractmethod
    def is_max_level(self) -> bool:
        pass

    @property
    @abstractmethod
    def is_min_level(self) -> bool:
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

    def yield_isolation(self) -> None:
        self._is_fist_decision = True

    @abstractmethod
    def _first_decision(self) -> NextStep:
        pass

    @abstractmethod
    def _monitoring_result(self) -> NextStep:
        pass

    def decide_next_step(self) -> NextStep:
        if self._is_fist_decision:
            self._is_fist_decision = False
            return self._first_decision()

        else:
            return self._monitoring_result()

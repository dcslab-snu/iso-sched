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

        self._force_strengthen: bool = True

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
        self._force_strengthen = True

    @abstractmethod
    def _try_scheduled(self) -> NextStep:
        pass

    @abstractmethod
    def _monitoring_result(self) -> NextStep:
        pass

    def monitoring_result(self) -> NextStep:
        if self._force_strengthen:
            self._force_strengthen = False
            return self._try_scheduled()

        else:
            return self._monitoring_result()

# coding: UTF-8

from abc import ABCMeta, abstractmethod

from .. import IsolationPhase, IsolationResult
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class Isolator(metaclass=ABCMeta):
    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        self._prev_metric_diff: MetricDiff = None

        self._next_phase = IsolationPhase.ENFORCING
        self._foreground_wl = foreground_wl
        self._background_wl = background_wl

    @abstractmethod
    def increase(self) -> 'Isolator':
        pass

    @abstractmethod
    def decrease(self) -> 'Isolator':
        pass

    @abstractmethod
    def _enforce(self) -> None:
        pass

    def enforce(self) -> None:
        self._prev_metric_diff: MetricDiff = self._foreground_wl.calc_metric_diff()

        self._next_phase = IsolationPhase.MONITORING

        self._enforce()

    @abstractmethod
    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        pass

    def monitoring_result(self) -> IsolationResult:
        self._next_phase = IsolationPhase.ENFORCING

        metric_diff = self._foreground_wl.calc_metric_diff()

        ret = self._monitoring_result(metric_diff)

        self._prev_metric_diff = metric_diff

        return ret

    @property
    def next_phase(self) -> IsolationPhase:
        return self._next_phase

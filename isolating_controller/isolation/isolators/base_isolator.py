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

        self._fg_next_step = NextStep.IDLE
        self._bg_next_step = NextStep.IDLE

        self._is_first_decision: bool = True

    def __del__(self):
        self.reset()

    @abstractmethod
    def strengthen(self) -> 'Isolator':
        """
        Adjust the isolation parameter to allocate more resources to the foreground workload.
        (Does not actually isolate)

        :return: current isolator object for method chaining
        :rtype: Isolator
        """
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
        """
        Adjust the isolation parameter to allocate less resources to the foreground workload.
        (Does not actually isolate)

        :return: current isolator object for method chaining
        :rtype: Isolator
        """
        pass

    @abstractmethod
    def _enforce(self) -> None:
        pass

    def enforce(self) -> None:
        """Actually applies the isolation parameter that set on the current object"""
        self._prev_metric_diff: MetricDiff = self._foreground_wl.calc_metric_diff()

        self._enforce()

    def yield_isolation(self) -> None:
        """
        Declare to stop the configuration search for the current isolator.
        Must be called when the current isolator yields the initiative.
        """
        self._is_first_decision = True

    @abstractmethod
    def _first_decision(self) -> NextStep:
        pass

    @abstractmethod
    def _monitoring_result(self) -> NextStep:
        pass

    def decide_next_step(self) -> NextStep:
        if self._is_first_decision:
            self._is_first_decision = False
            return self._first_decision()

        else:
            return self._monitoring_result()

    @abstractmethod
    def reset(self) -> None:
        """Restore to initial configuration"""
        pass

    def change_fg_wl(self, new_workload: Workload) -> None:
        self._foreground_wl = new_workload
        self._prev_metric_diff = new_workload.calc_metric_diff()

    def change_bg_wl(self, new_workload: Workload) -> None:
        self._background_wl = new_workload

    @abstractmethod
    def store_cur_config(self) -> None:
        """Store the current configuration"""
        pass

    @abstractmethod
    def load_cur_config(self) -> None:
        """Load the current configuration"""
        pass

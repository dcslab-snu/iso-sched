# coding: UTF-8

import logging
from abc import ABCMeta, abstractmethod
from typing import Any, ClassVar, Iterable, Optional, Tuple

from .. import NextStep
from ...metric_container.basic_metric import MetricDiff
from ...workload import Workload


class Isolator(metaclass=ABCMeta):
    _DOD_THRESHOLD: ClassVar[float] = 0.005
    _FORCE_THRESHOLD: ClassVar[float] = 0.05

    def __init__(self, foreground_wl: Workload, background_wls: Tuple[Workload, ...]) -> None:
        self._prev_metric_diff: MetricDiff = None

        self._foreground_wl = foreground_wl
        self._background_wls = background_wls

        self._fg_next_step = NextStep.IDLE
        self._bg_next_step = NextStep.IDLE

        self._is_first_decision: bool = True

        self._stored_config: Optional[Any] = None

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
    def enforce(self) -> None:
        """Actually applies the isolation parameter that set on the current object"""
        pass

    def yield_isolation(self) -> None:
        """
        Declare to stop the configuration search for the current isolator.
        Must be called when the current isolator yields the initiative.
        """
        self._is_first_decision = True

    def _first_decision(self, cur_metric_diff: MetricDiff) -> NextStep:
        curr_diff = self._get_metric_type_from(cur_metric_diff)

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        if curr_diff < 0:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN
        elif curr_diff <= self._FORCE_THRESHOLD:
            return NextStep.STOP
        else:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

    def _monitoring_result(self, prev_metric_diff: MetricDiff, cur_metric_diff: MetricDiff) -> NextStep:
        curr_diff = self._get_metric_type_from(cur_metric_diff)
        prev_diff = self._get_metric_type_from(prev_metric_diff)
        diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        if abs(diff_of_diff) <= self._DOD_THRESHOLD \
                or abs(curr_diff) <= self._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

        else:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN

    @classmethod
    @abstractmethod
    def _get_metric_type_from(cls, metric_diff: MetricDiff) -> float:
        pass

    def decide_next_step(self) -> NextStep:
        curr_metric_diff = self._foreground_wl.calc_metric_diff()

        if self._is_first_decision:
            self._is_first_decision = False
            next_step = self._first_decision(curr_metric_diff)

        else:
            next_step = self._monitoring_result(self._prev_metric_diff, curr_metric_diff)

        self._prev_metric_diff = curr_metric_diff

        return next_step

    @abstractmethod
    def reset(self) -> None:
        """Restore to initial configuration"""
        pass

    def change_fg_wl(self, new_workload: Workload) -> None:
        self._foreground_wl = new_workload
        self._prev_metric_diff = new_workload.calc_metric_diff()

    def change_bg_wl(self, new_workloads: Tuple[Workload, ...]) -> None:
        self._background_wls = new_workloads

    @abstractmethod
    def store_cur_config(self) -> None:
        """Store the current configuration"""
        pass

    def load_cur_config(self) -> None:
        """Load the current configuration"""
        if self._stored_config is None:
            raise ValueError('Store configuration first!')

    @property
    def _all_running_bgs(self) -> Iterable[Workload]:
        for bg in self._background_wls:
            if bg.is_running:
                yield bg

    @property
    def _any_running_bg(self) -> Workload:
        for bg in self._all_running_bgs:
            return bg

        raise ProcessLookupError('All BG is ended')

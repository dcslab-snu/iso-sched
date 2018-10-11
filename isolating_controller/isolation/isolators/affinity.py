# coding: UTF-8

import logging
from typing import Optional

from isolating_controller.workload import Workload
from .base_isolator import Isolator
from .. import NextStep
from ...metric_container.basic_metric import MetricDiff


class AffinityIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        self._cur_step: int = self._foreground_wl.orig_bound_cores[-1]

        self._stored_config: Optional[int] = None

    def strengthen(self) -> 'AffinityIsolator':
        self._cur_step += 1
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step + 1 == self._background_wl.bound_cores[0]

    @property
    def is_min_level(self) -> bool:
        return self._foreground_wl.orig_bound_cores == self._foreground_wl.bound_cores

    def weaken(self) -> 'AffinityIsolator':
        self._cur_step -= 1
        return self

    def enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'affinity of foreground is {self._foreground_wl.orig_bound_cores[0]}-{self._cur_step}')

        self._foreground_wl.bound_cores = range(self._foreground_wl.orig_bound_cores[0], self._cur_step + 1)

    def _first_decision(self, metric_diff: MetricDiff) -> NextStep:
        curr_diff = metric_diff.instruction_ps

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        if curr_diff < 0:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN
        elif curr_diff <= AffinityIsolator._FORCE_THRESHOLD:
            return NextStep.STOP
        else:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

    def _monitoring_result(self, prev_metric_diff: MetricDiff, cur_metric_diff: MetricDiff) -> NextStep:
        curr_diff = cur_metric_diff.instruction_ps
        prev_diff = prev_metric_diff.instruction_ps
        diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        if self.is_min_level or self.is_max_level \
                or abs(diff_of_diff) <= AffinityIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= AffinityIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            return NextStep.WEAKEN

        else:
            return NextStep.STRENGTHEN

    def reset(self) -> None:
        if self._foreground_wl.is_running:
            self._foreground_wl.bound_cores = self._foreground_wl.orig_bound_cores

    def store_cur_config(self) -> None:
        self._stored_config = self._cur_step

    def load_cur_config(self) -> None:
        super().load_cur_config()

        self._cur_step = self._stored_config
        self._stored_config = None

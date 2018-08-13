# coding: UTF-8

import logging

from .base_policy import IsolationPolicy
from .. import IsolationPhase, IsolationResult
from ..isolators import CacheIsolator, IdleIsolator, MemoryIsolator, SchedIsolator
from ...workload import Workload


class DiffPolicy(IsolationPolicy):
    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        super().__init__(fg_wl, bg_wl)

        self._is_llc_isolated = False
        self._is_mem_isolated = False
        self._is_sched_isolated = False

    @property
    def new_isolator_needed(self) -> bool:
        return isinstance(self._cur_isolator, IdleIsolator)

    def _clear_flags(self) -> None:
        self._is_llc_isolated = False
        self._is_mem_isolated = False
        self._is_sched_isolated = False

    def choose_next_isolator(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)

        metric_diff = self._fg_wl.calc_metric_diff()
        # TODO: change level to debug
        logger.info(f'diff is {metric_diff}')

        l3_hit_ratio = abs(metric_diff.l3_hit_ratio)
        local_mem_util = abs(metric_diff.local_mem_util)
        fg_name = self._fg_wl.name
        fg_pid = self._fg_wl.pid

        if self._is_sched_isolated and self._is_mem_isolated and self._is_llc_isolated:
            self._clear_flags()

        if not self._is_llc_isolated and l3_hit_ratio > local_mem_util:
            self._cur_isolator = self._isolator_map[CacheIsolator]
            self._is_llc_isolated = True
            logger.info(f'Cache Isolation for workload {fg_name} (pid: {fg_pid}) is started')

        elif not self._is_mem_isolated and l3_hit_ratio < local_mem_util:
            self._cur_isolator = self._isolator_map[MemoryIsolator]
            self._cur_isolator.increase()
            self._cur_isolator._next_phase = IsolationPhase.ENFORCING
            self._is_mem_isolated = True
            logger.info(f'Memory Bandwidth Isolation for workload {fg_name} (pid: {fg_pid}) is started')

        elif not self._is_sched_isolated and l3_hit_ratio < local_mem_util:
            self._cur_isolator = self._isolator_map[SchedIsolator]
            self._cur_isolator.increase()
            self._cur_isolator._next_phase = IsolationPhase.ENFORCING
            self._is_sched_isolated = True
            logger.info(f'Cpuset Isolation for workload {fg_name} (pid: {fg_pid}) is started')

    def _isolate(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)

        if self._cur_isolator.next_phase is IsolationPhase.ENFORCING:
            self._cur_isolator.enforce()

        elif self._cur_isolator.next_phase is IsolationPhase.MONITORING:
            result = self._cur_isolator.monitoring_result()

            logger.info(f'Monitoring Result : {result.name}')

            if result is IsolationResult.INCREASE:
                self._cur_isolator.increase()
            elif result is IsolationResult.DECREASE:
                self._cur_isolator.decrease()
            elif result is IsolationResult.STOP:
                self._cur_isolator = DiffPolicy.IDLE_ISOLATOR
            else:
                raise NotImplementedError(f'unknown isolation result : {result}')

        elif self._cur_isolator.next_phase is IsolationPhase.IDLE:
            pass

        else:
            raise NotImplementedError(f'unknown isolation phase : {self._cur_isolator.next_phase}')

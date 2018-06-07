# coding: UTF-8

import logging
from abc import ABC, abstractmethod
from enum import IntEnum

from isolation.cpu import CgroupCpuset
from isolation.last_level_cache import CAT
from isolation.memory import DVFS
from metric_container.basic_metric import MetricDiff


class IsolationPhase(IntEnum):
    ENFORCING = 1
    MONITORING = 2


class IsolationResult(IntEnum):
    INCREASE = 1
    DECREASE = 2
    STOP = 3


class Isolator(ABC):
    def __init__(self, foreground_wl: 'ForegroundWorkload') -> None:
        self._prev_metric_diff = None  # type: MetricDiff

        self._next_phase = IsolationPhase.ENFORCING
        self._foreground_wl = foreground_wl
        self._background_wl = foreground_wl.background_workload

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
        self._prev_metric_diff = self._foreground_wl.calc_metric_diff()  # type: MetricDiff

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
    def next_phase(self):
        return self._next_phase


class CacheIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: 'ForegroundWorkload') -> None:
        super().__init__(foreground_wl)

        self._cur_step = CAT.MAX // 2 + CAT.STEP
        self._acceleration = CAT.STEP

        CAT.create_group(str(foreground_wl.pid))
        CAT.add_task(str(foreground_wl.pid), foreground_wl.pid)
        CAT.create_group(str(self._background_wl.pid))
        CAT.add_task(str(self._background_wl.pid), self._background_wl.pid)

    def increase(self) -> 'CacheIsolator':
        self._cur_step += 1
        self._acceleration *= 2
        return self

    def decrease(self) -> 'CacheIsolator':
        self._cur_step -= 1
        self._acceleration //= 2
        # TODO: suggest `self._acceleration = CAT.STEP`
        return self

    def _enforce(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f'current step : {self._cur_step}, accel: {self._acceleration}')
        logger.info(f'foreground : background = {self._cur_step} : {CAT.MAX - self._cur_step}')

        fg_mask = CAT.gen_mask(0, self._cur_step)
        CAT.assign(str(self._foreground_wl.pid), fg_mask, '1')

        bg_mask = CAT.gen_mask(self._cur_step)
        CAT.assign(str(self._background_wl.pid), bg_mask, '1')

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        logger = logging.getLogger(self.__class__.__name__)

        curr = metric_diff.l3_hit_ratio
        prev = self._prev_metric_diff.l3_hit_ratio
        diff = curr - prev

        # TODO: remove
        logger.info(f'monitoring diff is {diff}')
        logger.info(f'current diff: {curr}, prev diff: {prev}')

        if abs(diff) <= CacheIsolator._THRESHOLD or not (CAT.MIN <= self._cur_step <= CAT.MAX):
            # FIXME
            # CAT.remove_group(str(self._foreground_wl.pid))
            # CAT.remove_group(str(self._foreground_wl.background_workload.pid))

            return IsolationResult.STOP
        elif diff > 0:
            return IsolationResult.DECREASE
        else:
            return IsolationResult.INCREASE


class MemoryIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: 'ForegroundWorkload') -> None:
        super().__init__(foreground_wl)

        # FIXME: hard coding
        self._cur_step = DVFS.MAX - DVFS.STEP

    def increase(self) -> 'MemoryIsolator':
        self._cur_step += DVFS.STEP
        return self

    def decrease(self) -> 'MemoryIsolator':
        self._cur_step -= DVFS.STEP
        return self

    def _enforce(self) -> None:
        # FIXME: hard coding
        DVFS.set_freq(self._cur_step, range(8, 16))

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        logger = logging.getLogger(self.__class__.__name__)

        curr = metric_diff.local_mem_util
        prev = self._prev_metric_diff.local_mem_util
        diff = curr - prev

        # TODO: remove
        logger.info(f'monitoring diff is {diff}')
        logger.info(f'current diff: {curr}, prev diff: {prev}')

        if not (DVFS.MIN <= self._cur_step <= DVFS.MAX) or abs(diff) <= MemoryIsolator._THRESHOLD:
            return IsolationResult.STOP
        elif diff > 0:
            return IsolationResult.INCREASE
        else:
            return IsolationResult.DECREASE


class SchedIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: 'ForegroundWorkload') -> None:
        super().__init__(foreground_wl)

        # FIXME: hard coding
        self._cur_step = 9

        CgroupCpuset.create_group(str(self._background_wl.pid))
        CgroupCpuset.add_task(str(self._background_wl.pid), self._background_wl.pid)

    def increase(self) -> 'Isolator':
        self._cur_step -= 1
        return self

    def decrease(self) -> 'Isolator':
        self._cur_step += 1
        return self

    def _enforce(self) -> None:
        # FIXME: hard coding
        CgroupCpuset.assign(str(self._background_wl.pid), set(range(self._cur_step, 16)))

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        logger = logging.getLogger(self.__class__.__name__)

        curr = metric_diff.local_mem_util
        prev = self._prev_metric_diff.local_mem_util
        diff = curr - prev

        # TODO: remove
        logger.info(f'monitoring diff is {diff}')
        logger.info(f'current diff: {curr}, prev diff: {prev}')

        if not (8 <= self._cur_step <= 15) or abs(diff) <= SchedIsolator._THRESHOLD:
            return IsolationResult.STOP
        elif diff > 0:
            return IsolationResult.INCREASE
        else:
            return IsolationResult.DECREASE

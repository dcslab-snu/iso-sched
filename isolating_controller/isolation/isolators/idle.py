# coding: UTF-8

from .base_isolator import Isolator
from .. import IsolationPhase, IsolationResult
from ...metric_container.basic_metric import MetricDiff


class IdleIsolator(Isolator):
    def __init__(self) -> None:
        pass

    def enforce(self) -> None:
        raise NotImplementedError(f'{self.__class__.__name__} can not be enforced')

    def monitoring_result(self) -> IsolationResult:
        raise NotImplementedError(f'{self.__class__.__name__} does not have monitoring result')

    @property
    def next_phase(self) -> IsolationPhase:
        return IsolationPhase.IDLE

    def increase(self) -> 'IdleIsolator':
        raise NotImplementedError(f'{self.__class__.__name__} can not be increased')

    def decrease(self) -> 'IdleIsolator':
        raise NotImplementedError(f'{self.__class__.__name__} can not be decreased')

    def _enforce(self) -> None:
        raise NotImplementedError(f'{self.__class__.__name__} can not be enforced')

    def _monitoring_result(self, metric_diff: MetricDiff) -> IsolationResult:
        raise NotImplementedError(f'{self.__class__.__name__} does not have monitoring result')

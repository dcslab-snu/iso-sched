# coding: UTF-8

import logging

from typing import Dict, Set

from .base_isolator import Isolator
from .. import NextStep
from ...workload import Workload
from ..policies import IsolationPolicy

class SwapIsolator(Isolator):
    _THRESHOLD = 0.005

    def __init__(self, foreground_wl: Workload, background_wl: Workload,
                 isolation_groups: Dict[IsolationPolicy, int]) -> None:
        super().__init__(foreground_wl, background_wl)

        self._all_groups = isolation_groups
        self._swap_candidates: Set[Workload] = None

    def __del__(self):
        logger = logging.getLogger(__name__)
        if self._foreground_wl.is_running:
            logger.debug(f'reset swap configuration of {self._foreground_wl}')

        if self._background_wl.is_running:
            logger.debug(f'reset swap configuration of {self._background_wl}')


    def strengthen(self) -> 'Isolator':
        """
        Choosing which contentious workloads to swap out to other socket
        :return:
        """
        # FIXME: hard coded (two sockets)
        ## Estimating the socket contention
        ##

        return

    @property
    def is_max_level(self) -> bool:
        """
        Searching configuration space to the max level
        e.g., There is no searchable candidate to strengthen the degree of isolation
        :return:
        """
        # FIXME:

        return False

    @property
    def is_min_level(self) -> bool:
        """
        Searching configuration space to the min level
        e.g., There is no searchable candidate to weaken the degree of isolation
        :return:
        """
        # FIXME:

        return False

    def weaken(self) -> 'Isolator':
        """
        Choosing which contentious workloads to swap in from other socket
        :return:
        """
        # FIXME: hard coded (two sockets)
        pass

    def _enforce(self) -> None:
        """
        Enforcing the pre-configured swap isolation
        :return:
        """
        pass

#    def enforce(self) -> None:
#        self._prev_metric_diff: MetricDiff = self._foreground_wl.calc_metric_diff()
#
#        self._enforce()

    def _first_decision(self) -> NextStep:
        pass

    def _monitoring_result(self) -> NextStep:
        pass

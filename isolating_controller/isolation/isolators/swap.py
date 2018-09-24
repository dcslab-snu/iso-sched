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
        super().__init__(foreground_wl, background_wl, None)

        self._all_groups = isolation_groups
        self._swap_candidates: Set[Workload] = None
        self._most_contentious_group = None
        self._most_contentious_workload = None

    def __del__(self):
        logger = logging.getLogger(__name__)
        if self._foreground_wl.is_running:
            logger.debug(f'reset swap configuration of {self._foreground_wl}')

        if self._background_wl.is_running:
            logger.debug(f'reset swap configuration of {self._background_wl}')


    def strengthen(self) -> 'SwapIsolator':
        """
        Choosing which contentious workloads to swap out to other socket
        :return:
        """
        # FIXME: hard coded (two sockets)
        ## 1.Estimating and selecting the most contentious workloads from the socket of cur_group
        ## 2.

        return self

    @property
    def is_max_level(self) -> bool:
        """
        Searching configuration space to the max level
        e.g., There is no searchable candidate to strengthen the degree of isolation
        :return:
        """
        # FIXME: hard coded
        return self._swap_candidates == None


    @property
    def is_min_level(self) -> bool:
        """
        Searching configuration space to the min level
        e.g., There is no searchable candidate to weaken the degree of isolation
        :return:
        """
        # FIXME: hard coded
        return self._swap_candidates == None


    def weaken(self) -> 'SwapIsolator':
        """
        Choosing which contentious workloads to swap in from other socket
        :return:
        """
        # FIXME: hard coded (two sockets)
        ## 1.Estimating and selecting the most contentious workloads from the socket of other_group
        return self

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
        """
        How to choose the first candidate?
        :return:
        """
        pass

    def _monitoring_result(self) -> NextStep:
        """
        If the effect of swapping is getting worse, then rollback??
        :return:
        """
        pass

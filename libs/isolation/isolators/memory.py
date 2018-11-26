# coding: UTF-8

import logging
from typing import Optional, Dict, List

from .base import Isolator
from ...metric_container.basic_metric import MetricDiff
from ...utils.memguard import Memguard
from ...workload import Workload


class MemoryIsolator(Isolator):
    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded
        # bw_weight is initialized to 100 at first (This functions same as cur_step)
        self._cur_wl_pids: List[str] = [self._foreground_wl.pid, self._background_wl.pid]
        self._cur_grp_names: List[str] = [self._foreground_wl.group_name, self._background_wl.group_name]
        self._cur_bw_weight: Dict[str, int] = dict()
        for grp_name in self._cur_grp_names:
            self._cur_bw_weight[grp_name] = 100

        self._cur_total_bw_weight = sum(self._cur_bw_weight.values())
        self._cur_weight_step = int(self._cur_total_bw_weight * 0.1)
        self._stored_config: Optional[int] = None
        self._memguard = Memguard(self._cur_grp_names, self._cur_bw_weight)

    @classmethod
    def _get_metric_type_from(cls, metric_diff: MetricDiff) -> float:
        return metric_diff.local_mem_util_ps

    def strengthen(self) -> 'MemoryIsolator':
        self._cur_bw_weight[self._foreground_wl.group_name] += self._cur_weight_step
        self._cur_bw_weight[self._background_wl.group_name] -= self._cur_weight_step
        return self

    def weaken(self) -> 'MemoryIsolator':
        self._cur_bw_weight[self._foreground_wl.group_name] -= self._cur_weight_step
        self._cur_bw_weight[self._background_wl.group_name] += self._cur_weight_step
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return (self._cur_bw_weight[self._foreground_wl.group_name] + self._cur_weight_step > Memguard.MAX_WEIGHT) or \
            (self._cur_bw_weight[self._background_wl.group_name] + self._cur_weight_step > Memguard.MAX_WEIGHT)

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        return (self._cur_bw_weight[self._foreground_wl.group_name] - self._cur_weight_step < Memguard.MIN_WEIGHT) or \
           (self._cur_bw_weight[self._background_wl.group_name] - self._cur_weight_step < Memguard.MIN_WEIGHT)

    def enforce(self) -> None:
        logger = logging.getLogger(__name__)
        fg_grp_name = self._foreground_wl.group_name
        bg_grp_name = self._background_wl.group_name
        logger.info(f'BW weight of {fg_grp_name} : {self._cur_bw_weight[self._foreground_wl.group_name]}')
        logger.info(f'BW weight of {bg_grp_name} : {self._cur_bw_weight[self._background_wl.group_name]}')

        self._memguard.update_bw_list_for_all_workloads()
        updated_bw_list = self._memguard.bw_list
        logger.info(f'Memguard\'s updated_bw_list : {updated_bw_list}')
        # FIXME: Currently no enforcing
        self._memguard.assign_bandwidth(None)

    def reset(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'reset called in MemoryIsolator!')
        # FIXME: Currently no enforcing
        #for grp_name in self._cur_grp_names:
        #    self._memguard.update_bw_weight(grp_name, 100)
        #self._memguard.update_bw_list_for_all_workloads()
        self._memguard.assign_bandwidth(['4250']*16)    # To guarantee the unfinished workload runs alone

    def store_cur_config(self) -> None:
        self._stored_config = self._cur_bw_weight

    def load_cur_config(self) -> None:
        super().load_cur_config()
        self._cur_bw_weight = self._stored_config
        self._stored_config = None

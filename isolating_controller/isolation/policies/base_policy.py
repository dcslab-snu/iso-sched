# coding: UTF-8

import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Type

from .. import ResourceType
from ..isolators import CacheIsolator, CoreIsolator, IdleIsolator, Isolator, MemoryIsolator
from ...metric_container.basic_metric import BasicMetric, MetricDiff
from ...workload import Workload


class IsolationPolicy(metaclass=ABCMeta):
    _IDLE_ISOLATOR: IdleIsolator = IdleIsolator()
    # FIXME : _CPU_THRESHOLD needs test
    _CPU_THRESHOLD = 0.1

    def __init__(self, fg_wl: Workload, bg_wl: Workload) -> None:
        self._fg_wl = fg_wl
        self._bg_wl = bg_wl

        self._isolator_map: Dict[Type[Isolator], Isolator] = dict()
        self._cur_isolator: Isolator = IsolationPolicy._IDLE_ISOLATOR

        self._aggr_inst_diff: float = None
        self._isolator_configs: Dict[Type[Isolator], Any] = dict()
        self._profile_stop_cond: int = None  # the count to stop solorun profiling condition
        self._thread_changed: bool = False
        self._fg_runs_alone: bool = False

    def __hash__(self) -> int:
        return id(self)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} <fg: {self._fg_wl}, bg: {self._bg_wl}>'

    def __del__(self) -> None:
        isolators = tuple(self._isolator_map.keys())
        for isolator in isolators:
            del self._isolator_map[isolator]

    def init_isolators(self) -> None:
        self._isolator_map = dict((
            (CacheIsolator, CacheIsolator(self._fg_wl, self._bg_wl)),
            (MemoryIsolator, MemoryIsolator(self._fg_wl, self._bg_wl)),
            (CoreIsolator, CoreIsolator(self._fg_wl, self._bg_wl)),
        ))

    @property
    @abstractmethod
    def new_isolator_needed(self) -> bool:
        pass

    @abstractmethod
    def choose_next_isolator(self) -> bool:
        pass

    def contentious_resource(self) -> ResourceType:
        metric_diff: MetricDiff = self._fg_wl.calc_metric_diff()
        cur_metric: BasicMetric = self._fg_wl.metrics[0]

        logger = logging.getLogger(__name__)
        logger.info(repr(metric_diff))
        logger.info(f'l3_int: {cur_metric.l3_intensity:>7.04f}, '
                    f'mem_int: {cur_metric.mem_intensity:>7.04f}, '
                    f'llc_util: {cur_metric.l3_util:>7.04f}')
        if abs(cur_metric.l3_intensity) < IsolationPolicy._CPU_THRESHOLD \
                and abs(cur_metric.mem_intensity) < IsolationPolicy._CPU_THRESHOLD:
            return ResourceType.CPU

        if metric_diff.local_mem_util_ps > 0 and metric_diff.l3_hit_ratio > 0:
            if metric_diff.l3_hit_ratio > metric_diff.local_mem_util_ps:
                return ResourceType.CACHE
            else:
                return ResourceType.MEMORY

        elif metric_diff.local_mem_util_ps < 0 < metric_diff.l3_hit_ratio:
            return ResourceType.MEMORY

        elif metric_diff.l3_hit_ratio < 0 < metric_diff.local_mem_util_ps:
            return ResourceType.CACHE

        else:
            if metric_diff.l3_hit_ratio > metric_diff.local_mem_util_ps:
                return ResourceType.MEMORY
            else:
                return ResourceType.CACHE

    @property
    def foreground_workload(self) -> Workload:
        return self._fg_wl

    @foreground_workload.setter
    def foreground_workload(self, new_workload: Workload):
        self._fg_wl = new_workload
        for isolator in self._isolator_map.values():
            isolator.change_fg_wl(new_workload)
            isolator.enforce()

    @property
    def background_workload(self) -> Workload:
        return self._bg_wl

    @background_workload.setter
    def background_workload(self, new_workload: Workload):
        self._bg_wl = new_workload
        for isolator in self._isolator_map.values():
            isolator.change_bg_wl(new_workload)
            isolator.enforce()

    @property
    def ended(self) -> bool:
        return not self._fg_wl.is_running or not self._bg_wl.is_running

    @property
    def cur_isolator(self) -> Isolator:
        return self._cur_isolator

    @property
    def name(self) -> str:
        return f'{self._fg_wl.name}({self._fg_wl.pid})'

    @property
    def aggr_inst(self) -> float:
        return self._aggr_inst_diff

    @property
    def most_cont_workload(self) -> Workload:
        fg_wl = self.foreground_workload
        bg_wl = self.background_workload

        fg_inst_diff = fg_wl.inst_diff
        bg_inst_diff = bg_wl.inst_diff

        # FIXME: Below condition is likely to fail due to too little differences between fg and bg
        if fg_inst_diff < bg_inst_diff:
            return fg_wl
        else:
            return bg_wl

    @property
    def least_cont_workload(self) -> Workload:
        fg_wl = self.foreground_workload
        bg_wl = self.background_workload

        fg_ipc_diff = fg_wl.inst_diff
        bg_ipc_diff = bg_wl.inst_diff

        # FIXME: Below condition is likely to fail due to too little differences between fg and bg
        if fg_ipc_diff > bg_ipc_diff:
            return fg_wl
        else:
            return bg_wl

    @property
    def least_mem_bw_workload(self) -> Workload:
        fg_wl = self.foreground_workload
        bg_wl = self.background_workload

        fg_mem_bw = fg_wl.metrics[0].local_mem_ps()
        bg_mem_bw = bg_wl.metrics[0].local_mem_ps()

        if fg_mem_bw > bg_mem_bw:
            return bg_wl
        else:
            return fg_wl

    # FIXME: replace to property
    def update_aggr_instr(self) -> None:
        fg_diff = self._fg_wl.calc_metric_diff()
        bg_diff = self._bg_wl.calc_metric_diff()
        self._fg_wl._ipc_diff = fg_diff.instruction_ps
        self._bg_wl._ipc_diff = bg_diff.instruction_ps
        self._aggr_inst_diff = fg_diff.instruction_ps + bg_diff.instruction_ps

    def set_idle_isolator(self) -> None:
        self._cur_isolator.yield_isolation()
        self._cur_isolator = IsolationPolicy._IDLE_ISOLATOR

    def reset(self) -> None:
        for isolator in self._isolator_map.values():
            isolator.reset()

    def store_cur_configs(self) -> None:
        for isotype, isolator in self._isolator_map.items():
            isolator.store_cur_config()
            self._isolator_configs[isotype] = isolator.load_cur_config()

    def reset_stored_configs(self) -> None:
        """
        Reset stored configs
        """
        logger = logging.getLogger(__name__)
        # Cpuset (Cpuset)
        cpuset_config = self._isolator_configs[CoreIsolator]
        (fg_cpuset, bg_cpuset) = cpuset_config
        self._fg_wl.cgroup_cpuset.assign_cpus(fg_cpuset)
        self._bg_wl.cgroup_cpuset.assign_cpus(bg_cpuset)

        # DVFS (Dict(cpuid, freq))
        dvfs_config = self._isolator_configs[MemoryIsolator]
        (fg_dvfs_config, bg_dvfs_config) = dvfs_config
        fg_cpuset = fg_dvfs_config.keys()
        fg_cpufreq = fg_dvfs_config.values()
        fg_dvfs = self._fg_wl.dvfs
        for fg_cpu in fg_cpuset:
            freq = fg_cpufreq[fg_cpu]
            fg_dvfs.set_freq(freq, fg_cpu)

        bg_cpuset = bg_dvfs_config.keys()
        bg_cpufreq = bg_dvfs_config.values()
        bg_dvfs = self._bg_wl.dvfs
        for bg_cpu in bg_cpuset:
            freq = bg_cpufreq[bg_cpu]
            bg_dvfs.set_freq(freq, bg_cpu)

        # ResCtrl (Mask)
        resctrl_config = self._isolator_configs[CacheIsolator]
        (fg_mask, bg_mask) = resctrl_config
        logger.debug(f'fg_mask: {fg_mask}, bg_mask: {bg_mask}')
        logger.debug(f'fg_path: {self._fg_wl.resctrl.MOUNT_POINT/self._fg_wl.group_name}')
        self._fg_wl.resctrl.assign_llc(*fg_mask)
        self._bg_wl.resctrl.assign_llc(*bg_mask)

    def profile_solorun(self) -> None:
        """
        profile solorun status of a workload
        :return:
        """
        # suspend all workloads and their perf agents
        all_fg_wls = list()
        all_bg_wls = list()
        fg_wl = self.foreground_workload
        bg_wl = self.background_workload
        fg_wl.pause()
        fg_wl.pause_perf()
        bg_wl.pause()
        bg_wl.pause_perf()
        all_fg_wls.append(fg_wl)
        all_bg_wls.append(bg_wl)

        # run FG workloads alone
        for fg_wl in all_fg_wls:
            fg_wl.solorun_data_queue.clear()  # clear the prev. solorun data
            fg_wl.profile_solorun = True
            self.fg_runs_alone = True
            fg_wl.resume()
            fg_wl.resume_perf()

    def _update_all_workloads_num_threads(self):
        """
        update the workloads' number of threads (cur_num_threads -> prev_num_threads)
        :return:
        """
        bg_wl = self.background_workload
        fg_wl = self.foreground_workload
        bg_wl.update_num_threads()
        fg_wl.update_num_threads()

    def profile_needed(self, profile_interval, schedule_interval, count: int) -> bool:
        """
        This function checks if the profiling procedure should be called

        profile_freq : the frequencies of online profiling
        :param profile_interval: the frequency of attempting profiling solorun
        :param schedule_interval: the frequency of scheduling (isolation)
        :param count: This counts the number of entering the run func. loop
        :return: Decision whether to initiate online solorun profiling
        """
        logger = logging.getLogger(__name__)
        profile_freq = int(profile_interval / schedule_interval)
        fg_wl = self.foreground_workload
        logger.debug(f'count: {count}, profile_freq: {profile_freq}, '
                     f'fg_wl.is_num_threads_changed(): {fg_wl.is_num_threads_changed()}')

        if fg_wl.is_num_threads_changed():
            fg_wl.thread_changed_before = True

        if count % profile_freq != 0 or not fg_wl.thread_changed_before:
            self._update_all_workloads_num_threads()
            return False
        else:
            self._update_all_workloads_num_threads()
            fg_wl.thread_changed_before = False
            return True

    @property
    def profile_stop_cond(self) -> int:
        return self._profile_stop_cond

    @profile_stop_cond.setter
    def profile_stop_cond(self, new_count: int) -> None:
        self._profile_stop_cond = new_count

    def all_workload_pause(self):
        self._fg_wl.pause()
        self._fg_wl.pause_perf()
        self._bg_wl.pause()
        self._bg_wl.pause_perf()

    def all_workload_resume(self):
        self._fg_wl.resume()
        self._fg_wl.resume_perf()
        self._bg_wl.resume()
        self._bg_wl.resume_perf()
        self.fg_runs_alone = False

    @property
    def fg_runs_alone(self) -> bool:
        return self._fg_runs_alone

    @fg_runs_alone.setter
    def fg_runs_alone(self, new_val) -> None:
        self._fg_runs_alone = new_val

# coding: UTF-8


import subprocess

from pathlib import Path
from typing import ClassVar, Set, Dict, List
from libs.utils.cgroup import CpuSet
from . import numa_topology


class Memguard:
    MOUNT_POINT: ClassVar[Path] = Path('/sys/kerenl/debug/memguard')
    TOTAL_BW = 68000    # 68000 MB/s == 68 GB/s
    MIN_WEIGHT = 20
    MAX_WEIGHT = 180

    def __init__(self, group_names: List[str], bw_weights: Dict[str, int]):
        self._group_names: List[str] = group_names
        self._all_cgroup = [CpuSet(group_name) for group_name in self._group_names]
        self._maxbw = 1200  # TODO: It is not used currently

        # Get the info of nodes
        self._online_nodes = numa_topology.cur_online_nodes()
        self._online_cores: Dict[int, Set[int]] = dict()    # key:val = node_num:cpu_affinity
        for node in self._online_nodes:
            node_online_cores = numa_topology.core_belongs_to(node)
            self._online_cores[node] = node_online_cores

        self._bw_weight_dict: Dict[str, int] = bw_weights       # key:val = grp_name:weight
        self._bw_dict: Dict[str, int] = dict()                  # key:val = grp_name:membw(MB)
        # FIXME: hard coded initial per_core MEM. BW as 1000MB/s and set `ncpus` to `node 0's ncpus`
        self._bw_list: List[str] = ['1000'] * len(self._online_cores[0])  # idx = core_id, val = membw of core_id

    @property
    def maxbw(self):
        return self._maxbw

    @property
    def bw_list(self):
        return self._bw_list

    def update_bw_weight(self, grp_name: str, bw_weight: int) -> None:
        self._bw_weight_dict[grp_name] = bw_weight

    def assign_bandwidth(self) -> None:
        mb_bandwidth = 'mb '
        bw_str = " ".join(self._bw_list)

        input_bandwidths = mb_bandwidth + bw_str
        subprocess.run(args=('sudo', 'echo', str(self.MOUNT_POINT/'limit')),
                       input=input_bandwidths, check=True, encoding='ASCII', stdout=subprocess.DEVNULL)

    def calc_bandwidth_of_cores(self, grp_name: str, bw_weight: int, total_bw_weight: int) -> int:
        """
        Return the each core's bandwidth to enforce the share of workload
        :param grp_name: group name which cores belong to
        :param bw_weight: It contains the share of each workload (key:val = pid:bw_share), bw_share : [0,100]
        :param total_bw_weight: The value of total bw weight
        :return:
        """
        bw_share = float(bw_weight/total_bw_weight)
        wl_bw = float(self.TOTAL_BW * bw_share)
        wl_cpus: Set[int] = CpuSet(grp_name).read_cpus()
        wl_ncpus = len(wl_cpus)
        membw_of_each_cpu = int(wl_bw/wl_ncpus)
        return membw_of_each_cpu

    def calc_total_bw_weight(self):
        total_bw_weight = 0
        for pid, bw_weight in self._bw_weight_dict.items():
            total_bw_weight += bw_weight
        return total_bw_weight

    def update_bw_list_for_all_workloads(self) -> None:
        """
        This function updates the Mem. BW of all cores before assigning bandwidth
        :return:
        """
        total_bw_weight = self.calc_total_bw_weight()
        for grp_name, bw_weight in self._bw_weight_dict.items():
            membw = self.calc_bandwidth_of_cores(grp_name, bw_weight, total_bw_weight)
            # Making BW_List for a group
            wl_cpus: Set[int] = CpuSet(grp_name).read_cpus()
            for cpu_id in wl_cpus:
                self._bw_list[cpu_id] = str(membw)

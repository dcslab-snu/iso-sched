# coding: UTF-8



from libs.utils.cgroup import CpuSet

class Memguard:

    def __init__(self, group_name):
        self._group_name: str = group_name
        self._cur_cgroup = CpuSet(self._group_name)

        return
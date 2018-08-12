#!/usr/bin/env python3
# coding: UTF-8

import argparse
import functools
import json
import logging
import sys
import time
from collections import deque
from threading import Thread
from typing import Deque, Dict, List, Set, Tuple, Type

import pika
import psutil
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from isolating_controller.isolation.policies import DiffPolicy, IsolationPolicy
from isolating_controller.metric_container.basic_metric import BasicMetric
from isolating_controller.workload import Workload

MIN_PYTHON = (3, 6)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class PendingQueue:
    def __init__(self, policy_type: Type[IsolationPolicy]) -> None:
        self._policy_type: Type[IsolationPolicy] = policy_type

        self._bg_q: Dict[Tuple[int, ...], Workload] = dict()
        self._fg_q: Dict[Tuple[int, ...], Workload] = dict()
        self._pending_list: List[IsolationPolicy] = list()

    def add_bg(self, workload: Workload) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f'{workload.name} (pid: {workload.pid}) is ready for active as Background')

        # FIXME: hard coded
        other_cpuset = tuple(map(lambda x: x - 8, workload.cpuset))

        if other_cpuset in self._fg_q:
            new_group = self._policy_type(self._fg_q[other_cpuset], workload)
            self._pending_list.append(new_group)
            del self._fg_q[other_cpuset]

        else:
            self._bg_q[workload.cpuset] = workload

    def add_fg(self, workload: Workload) -> None:
        logger = logging.getLogger(self.__class__.__name__)
        logger.info(f'{workload.name} (pid: {workload.pid}) is ready for active as Foreground')

        # FIXME: hard coded
        other_cpuset = tuple(map(lambda x: x + 8, workload.cpuset))

        if other_cpuset in self._bg_q:
            new_group = self._policy_type(self._bg_q[other_cpuset], workload)
            self._pending_list.append(new_group)
            del self._bg_q[other_cpuset]

        else:
            self._fg_q[workload.cpuset] = workload

    @property
    def pending(self):
        return self._pending_list


class MainController(metaclass=Singleton):
    def __init__(self, metric_buf_size: int):
        self._corun_metric_dict: Dict[int, Deque] = dict()

        self._pending_wl = PendingQueue(DiffPolicy)

        self._metric_buf_size = metric_buf_size

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._channel = self._connection.channel()

        self._creation_queue = 'workload_creation'

        self._control_thread = ControlThread(self)

    @property
    def metric_buf_size(self) -> int:
        return self._metric_buf_size

    @property
    def pending_workloads(self) -> PendingQueue:
        return self._pending_wl

    @property
    def corun_metric_dict(self) -> Dict[int, Deque]:
        return self._corun_metric_dict

    def delete_workload(self, pid: int):
        del self._corun_metric_dict[pid]

    def _cbk_wl_creation(self, ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes):
        logger = logging.getLogger(self.__class__.__name__)

        ch.basic_ack(method.delivery_tag)

        arr = body.decode().strip().split(',')
        logger.debug(f'{arr} is received from workload_creation queue')

        if len(arr) != 4:
            return

        wl_name, pid, perf_pid, perf_interval = arr
        pid = int(pid)
        perf_pid = int(perf_pid)
        perf_interval = int(perf_interval)

        if not psutil.pid_exists(pid):
            return

        corun_q = deque()
        self._corun_metric_dict[pid] = corun_q

        workload = Workload(wl_name, pid, perf_pid, corun_q, perf_interval)

        # FIXME: hard coded
        if wl_name == 'SP':
            self._pending_wl.add_bg(workload)
        else:
            self._pending_wl.add_fg(workload)

        logger.info(f'{wl_name} (pid: {pid}) is created')

        wl_queue_name = '{}({})'.format(wl_name, pid)
        ch.queue_declare(wl_queue_name)
        ch.basic_consume(functools.partial(self._cbk_wl_monitor, pid), wl_queue_name)

    def _cbk_wl_monitor(self, pid: int, ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes):
        metric = json.loads(body.decode())
        ch.basic_ack(method.delivery_tag)

        logger = logging.getLogger(self.__class__.__name__)

        item = BasicMetric(metric['l2miss'],
                           metric['r1b2'],
                           metric['l3miss'],
                           metric['instructions'],
                           metric['cycles'],
                           metric['stall_cycles'],
                           metric['wall-cycles'],
                           metric['intra_coh'],
                           metric['inter_coh'],
                           metric['llc_occupancy'],
                           metric['local_mem'],
                           metric['remote_mem'],
                           metric['req_num'])

        logger.debug(f'{metric} is given from ')

        if pid not in self._corun_metric_dict:
            return

        if len(self._corun_metric_dict[pid]) == self._metric_buf_size:
            self._corun_metric_dict[pid].pop()

        self._corun_metric_dict[pid].appendleft(item)

    def run(self):
        logger = logging.getLogger(self.__class__.__name__)

        self._control_thread.start()

        self._channel.queue_declare(self._creation_queue)
        self._channel.basic_consume(self._cbk_wl_creation, self._creation_queue)

        try:
            logger.info('starting consuming thread')
            self._channel.start_consuming()

        except KeyboardInterrupt:
            self._channel.close()
            self._connection.close()


class ControlThread(Thread):
    def __init__(self, parent: MainController):
        Thread.__init__(self)
        self.daemon = True

        # FIXME
        self.parent: MainController = parent

        self._interval = 2  #: Scheduling interval (2 sec.)

        self._isolation_groups: Set[IsolationPolicy] = set()

    def _isolate_workloads(self):
        for group in self._isolation_groups:
            if group.new_isolator_needed:
                group.choose_next_isolator()
                continue

            group.isolate()

    def _register_pending_workloads(self):
        """
        This function detects and registers the spawned workloads(threads),
        also deletes the finished workloads(threads) from the dict.
        """
        logger = logging.getLogger(self.__class__.__name__)

        ended = tuple(filter(lambda g: g.ended, self._isolation_groups))

        for group in ended:
            fg_workload = group.foreground_workload
            logger.info(f'workload {fg_workload.name} (pid: {fg_workload.pid}) is ended')
            # TODO: deallocate isolation groups

            # remove from containers
            self._isolation_groups.remove(group)
            self.parent.delete_workload(group.foreground_workload.pid)
            self.parent.delete_workload(group.background_workload.pid)

        # set pending workloads as active
        while len(self.parent.pending_workloads.pending):
            new_wl: IsolationPolicy = self.parent.pending_workloads.pending.pop()
            logger.info(f'{new_wl} is created')
            new_wl.choose_next_isolator()
            self._isolation_groups.add(new_wl)

    def run(self):
        logger = logging.getLogger(self.__class__.__name__)

        logger.info('starting isolation loop')

        while True:
            self._register_pending_workloads()
            self._isolate_workloads()

            time.sleep(self._interval)


def main():
    parser = argparse.ArgumentParser(description='Run workloads that given by parameter.')
    parser.add_argument('-b', '--metric-buf-size', dest='buf_size', default='50', type=int,
                        help='metric buffer size per thread. (default : 50)')

    args = parser.parse_args()

    controller = MainController(args.buf_size)
    controller.run()


if __name__ == '__main__':
    if sys.version_info < MIN_PYTHON:
        sys.exit('Python {}.{} or later is required.\n'.format(*MIN_PYTHON))

    main()

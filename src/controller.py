#!/usr/bin/env python3
# coding: UTF-8

import sys
import time

import argparse
import functools
import json
import logging
import pika
import psutil
from collections import deque
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from threading import Thread
from typing import Deque, Dict, List, Set, Tuple

from isolation.isolator import CacheIsolator, IsolationPhase, IsolationResult, MemoryIsolator, SchedIsolator
from metric_container.basic_metric import BasicMetric
from workload import BackgroundWorkload, ForegroundWorkload, Workload

MIN_PYTHON = (3, 5)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class PendingQueue:
    def __init__(self) -> None:
        super().__init__()

        self._bg_q = dict()  # type: Dict[Tuple[int, ...], BackgroundWorkload]
        self._fg_q = dict()  # type: Dict[Tuple[int, ...], ForegroundWorkload]

        self._pending_list = list()  # type: List[ForegroundWorkload]

    def add(self, workload: Workload):
        logger = logging.getLogger(self.__class__.__name__)

        if isinstance(workload, BackgroundWorkload):
            cpuset = workload.cpuset

            logger.info(f'{workload.name} (pid: {workload.pid}) is ready for active as Background')

            # if cpuset in self._fg_q:
            #     fg_wl = self._fg_q[cpuset]
            #     fg_wl.background_workload = workload
            #
            #     del self._fg_q[cpuset]
            #
            #     self._pending_list.append(fg_wl)
            # else:
            #     self._bg_q[cpuset] = workload

            if len(self._fg_q):
                fg_wl = tuple(self._fg_q.values())[0]

                fg_wl.background_workload = workload
                self._pending_list.append(fg_wl)
            else:
                self._bg_q[cpuset] = workload

        elif isinstance(workload, ForegroundWorkload):
            cpuset = workload.cpuset

            logger.info(f'{workload.name} (pid: {workload.pid}) is ready for active as Foreground')

            # if cpuset in self._bg_q:
            #     bg_wl = self._bg_q[cpuset]
            #     del self._bg_q[cpuset]
            #
            #     workload.background_workload = bg_wl
            #
            #     self._pending_list.append(workload)
            # else:
            #     self._fg_q[cpuset] = workload

            if len(self._bg_q):
                bg_wl = tuple(self._bg_q.values())[0]

                workload.background_workload = bg_wl
                self._pending_list.append(workload)
            else:
                self._fg_q[cpuset] = workload

        else:
            # FIXME
            pass

    @property
    def pending(self):
        return self._pending_list


class MainController(metaclass=Singleton):
    def __init__(self, metric_buf_size: int):
        self._corun_metric_dict = dict()  # type: Dict[int, Deque]

        self._pending_wl = PendingQueue()

        self._metric_buf_size = metric_buf_size

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._channel = self._connection.channel()

        self._creation_queue = 'workload_creation'

        self._control_thread = ControlThread(self)

    @property
    def metric_buf_size(self) -> int:
        """:rtype: int"""
        return self._metric_buf_size

    @property
    def pending_workloads(self) -> PendingQueue:
        """:rtype: list[Workload]"""
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

        wl_name, pid, perf_pid, perf_interval = arr  # type: str, str, str, str
        pid = int(pid)  # type: int
        perf_pid = int(perf_pid)  # type: int
        perf_interval = int(perf_interval)  # type: int

        if not psutil.pid_exists(pid):
            return

        corun_q = deque()
        self._corun_metric_dict[pid] = corun_q

        if wl_name == 'sp':
            workload = BackgroundWorkload(wl_name, pid, perf_pid, corun_q, perf_interval)
        else:
            workload = ForegroundWorkload(wl_name, pid, perf_pid, corun_q, perf_interval)

        logger.info(f'{wl_name} (pid: {pid}) is created')

        self._pending_wl.add(workload)

        wl_queue_name = '{}({})'.format(wl_name, pid)
        ch.queue_declare(wl_queue_name)
        ch.basic_consume(functools.partial(self._cbk_wl_monitor, pid), wl_queue_name)

    def _cbk_wl_monitor(self, pid: int, ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes):
        metric = json.loads(body.decode())
        ch.basic_ack(method.delivery_tag)

        item = BasicMetric(metric['l2miss'],
                           metric['l3miss_load'],
                           metric['l3miss'],
                           metric['instructions'],
                           metric['cycles'],
                           metric['stall_cycles'],
                           metric['tsc_rate'],
                           metric['intra_coh'],
                           metric['inter_coh'],
                           metric['llc_size'],
                           metric['local_mem'],
                           metric['remote_mem'],
                           metric['req_num'])

        logger = logging.getLogger(self.__class__.__name__)
        logger.debug(str(item))

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
    def __init__(self, parent):
        Thread.__init__(self)
        self.daemon = True

        # FIXME
        self.parent = parent  # type: MainController

        self._interval = 2  #: Scheduling interval (2 sec.)

        self.active_workloads = set()  # type: Set[ForegroundWorkload]

    def _isolate_workloads(self):
        logger = logging.getLogger(self.__class__.__name__)

        self._register_pending_workloads()

        for workload in self.active_workloads:
            isolator = workload.isolator

            if isolator is None:
                if workload.is_mem_isolated and workload.is_llc_isolated and workload.is_sched_isolated:
                    workload.clear_flags()
                self._set_isolator(workload)
                continue

            logger.info(f'isolate workload {workload.name} (pid: {workload.pid})')

            metric_diff = workload.calc_metric_diff()
            l3_hit_ratio = abs(metric_diff.l3_hit_ratio)
            local_mem_util = abs(metric_diff.local_mem_util)

            if l3_hit_ratio > local_mem_util and not isinstance(isolator, CacheIsolator):
                logger.warning('Violation! not cache')
            elif l3_hit_ratio < local_mem_util and \
                    (not isinstance(isolator, MemoryIsolator) or not isinstance(isolator, SchedIsolator)):
                logger.warning('Violation! not memory')

            if isolator.next_phase is IsolationPhase.ENFORCING:
                isolator.enforce()
            elif isolator.next_phase is IsolationPhase.MONITORING:
                result = isolator.monitoring_result()

                logger.info(f'Monitoring Result : {result.name}')

                if result is IsolationResult.INCREASE:
                    isolator.increase()
                elif result is IsolationResult.DECREASE:
                    isolator.decrease()
                elif result is IsolationResult.STOP:
                    workload.isolator = None
                else:
                    # TODO
                    pass
            else:
                # TODO
                pass

    def _register_pending_workloads(self):
        """
        This function detects and registers the spawned workloads(threads),
        also deletes the finished workloads(threads) from the dict.
        """
        logger = logging.getLogger(self.__class__.__name__)

        ended = tuple(workload for workload in self.active_workloads if not workload.is_running)

        for workload in ended:
            logger.info(f'workload {workload.name} (pid: {workload.pid}) is ended')
            # TODO: deallocate isolation groups

            # remove from containers
            self.active_workloads.remove(workload)
            self.parent.delete_workload(workload.pid)

        # set pending workloads as active
        while len(self.parent.pending_workloads.pending):
            new_wl = self.parent.pending_workloads.pending.pop()

            self._set_isolator(new_wl)

            self.active_workloads.add(new_wl)

    def _set_isolator(self, workload: ForegroundWorkload) -> ForegroundWorkload:
        logger = logging.getLogger(self.__class__.__name__)
        metric_diff = workload.calc_metric_diff()

        # TODO: change level to debug
        logger.info(f'scanning diff is {metric_diff}')

        l3_hit_ratio = abs(metric_diff.l3_hit_ratio)
        local_mem_util = abs(metric_diff.local_mem_util)

        if not workload.is_llc_isolated and l3_hit_ratio > local_mem_util:
            workload.isolator = CacheIsolator(workload)

            logger.info(f'Cache Isolation for workload {workload.name} (pid: {workload.pid}) is started')
        elif not workload.is_mem_isolated and l3_hit_ratio < local_mem_util:
            workload.isolator = MemoryIsolator(workload)

            logger.info(f'Memory Bandwidth Isolation for workload {workload.name} (pid: {workload.pid}) is started')
        elif not workload.is_sched_isolated and l3_hit_ratio < local_mem_util:
            workload.isolator = SchedIsolator(workload)

            logger.info(f'Cpuset Isolation for workload {workload.name} (pid: {workload.pid}) is started')

    def run(self):
        logger = logging.getLogger(self.__class__.__name__)

        logger.info('starting isolation loop')

        time.sleep(4)

        while True:
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

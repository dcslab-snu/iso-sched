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
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from threading import Thread
from typing import Dict

from isolating_controller.isolation import NextStep
from isolating_controller.isolation.policies import DiffPolicy, IsolationPolicy
from isolating_controller.metric_container.basic_metric import BasicMetric
from isolating_controller.workload import Workload
from pending_queue import PendingQueue

MIN_PYTHON = (3, 6)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MainController(metaclass=Singleton):
    def __init__(self, metric_buf_size: int) -> None:
        self._pending_wl = PendingQueue(DiffPolicy)

        self._metric_buf_size = metric_buf_size

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._channel = self._connection.channel()

        self._creation_queue = 'workload_creation'

        self._control_thread = ControlThread(self._pending_wl)

    def _cbk_wl_creation(self, ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
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

        workload = Workload(wl_name, pid, perf_pid, perf_interval)

        # FIXME: hard coded
        if wl_name == 'SP':
            self._pending_wl.add_bg(workload)
        else:
            self._pending_wl.add_fg(workload)

        logger.info(f'{wl_name} (pid: {pid}) is created')

        wl_queue_name = '{}({})'.format(wl_name, pid)
        ch.queue_declare(wl_queue_name)
        ch.basic_consume(functools.partial(self._cbk_wl_monitor, workload), wl_queue_name)

    def _cbk_wl_monitor(self, workload: Workload,
                        ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
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

        metric_que = workload.corun_metrics

        if len(metric_que) == self._metric_buf_size:
            metric_que.pop()

        metric_que.appendleft(item)

    def run(self) -> None:
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
    def __init__(self, pending_queue: PendingQueue) -> None:
        Thread.__init__(self)
        self.daemon = True

        self._pending_queue: PendingQueue = pending_queue

        self._interval: int = 2  #: Scheduling interval (2 sec.)

        self._isolation_groups: Dict[IsolationPolicy, int] = dict()

    def _isolate_workloads(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)

        for group, iteration_num in self._isolation_groups.items():
            try:
                logger.info('')
                logger.info(f'***************isolation of {group.name} #{iteration_num}***************')

                if group.new_isolator_needed:
                    group.choose_next_isolator()
                    group.cur_isolator.enforce()
                    continue

                cur_isolator = group.cur_isolator

                monitoring = cur_isolator.monitoring_result()
                logger.info(f'Monitoring Result : {monitoring.name}')

                if monitoring is NextStep.STRENGTHEN:
                    cur_isolator.strengthen()
                elif monitoring is NextStep.WEAKEN:
                    cur_isolator.weaken()
                elif monitoring is NextStep.STOP:
                    group.set_idle_isolator()
                else:
                    raise NotImplementedError(f'unknown isolation result : {monitoring}')

                cur_isolator.enforce()

            except psutil.NoSuchProcess:
                pass

            finally:
                self._isolation_groups[group] += 1

    def _register_pending_workloads(self) -> None:
        """
        This function detects and registers the spawned workloads(threads).
        """
        logger = logging.getLogger(self.__class__.__name__)

        # set pending workloads as active
        while len(self._pending_queue):
            pending_group: IsolationPolicy = self._pending_queue.pop()
            logger.info(f'{pending_group} is created')

            self._isolation_groups[pending_group] = 0

    def _remove_ended_groups(self) -> None:
        """
        deletes the finished workloads(threads) from the dict.
        """
        logger = logging.getLogger(self.__class__.__name__)

        ended = tuple(filter(lambda g: g.ended, self._isolation_groups))

        for group in ended:
            if group.foreground_workload.is_running:
                ended_workload = group.background_workload
            else:
                ended_workload = group.foreground_workload
            logger.info(f'workload {ended_workload.name} (pid: {ended_workload.pid}) is ended')

            # remove from containers
            del self._isolation_groups[group]

    def run(self) -> None:
        logger = logging.getLogger(self.__class__.__name__)

        logger.info('starting isolation loop')

        while True:
            self._remove_ended_groups()
            self._register_pending_workloads()

            time.sleep(self._interval)

            self._isolate_workloads()


def main() -> None:
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

#!/usr/bin/env python3
# coding: UTF-8

import argparse
import datetime
import functools
import json
import logging
import os
import subprocess
import sys
import time
from threading import Thread
from typing import Dict

import pika
import psutil
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

import isolating_controller
from isolating_controller.isolation import NextStep
from isolating_controller.isolation.isolators import Isolator
from isolating_controller.isolation.policies import GreedyDiffWViolationPolicy, IsolationPolicy
from isolating_controller.metric_container.basic_metric import BasicMetric
from isolating_controller.workload import Workload
from pending_queue import PendingQueue
from swap_iso import SwapIsolator

MIN_PYTHON = (3, 6)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MainController(metaclass=Singleton):
    def __init__(self, metric_buf_size: int) -> None:
        self._metric_buf_size = metric_buf_size

        self._rmq_host = 'localhost'
        self._rmq_creation_queue = 'workload_creation'

        self._pending_wl = PendingQueue(GreedyDiffWViolationPolicy)
        self._control_thread = ControlThread(self._pending_wl)

    def _cbk_wl_creation(self, ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
        ch.basic_ack(method.delivery_tag)

        arr = body.decode().strip().split(',')

        logger = logging.getLogger('monitoring.workload_creation')
        logger.debug(f'{arr} is received from workload_creation queue')

        if len(arr) != 5:
            return

        wl_identifier, wl_type, pid, perf_pid, perf_interval = arr
        pid = int(pid)
        perf_pid = int(perf_pid)
        perf_interval = int(perf_interval)
        item = wl_identifier.split('_')
        wl_name = item[0]

        if not psutil.pid_exists(pid):
            return

        workload = Workload(wl_name, wl_type, pid, perf_pid, perf_interval)
        if wl_type == 'bg':
            logger.info(f'{workload} is background process')
        else:
            logger.info(f'{workload} is foreground process')

        self._pending_wl.add(workload)

        wl_queue_name = '{}({})'.format(wl_name, pid)
        ch.queue_declare(wl_queue_name)
        ch.basic_consume(functools.partial(self._cbk_wl_monitor, workload), wl_queue_name)

    def _cbk_wl_monitor(self, workload: Workload,
                        ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
        metric = json.loads(body.decode())
        ch.basic_ack(method.delivery_tag)

        item = BasicMetric(metric['l2miss'],
                           metric['l3miss'],
                           metric['instructions'],
                           metric['cycles'],
                           metric['stall_cycles'],
                           metric['wall_cycles'],
                           metric['intra_coh'],
                           metric['inter_coh'],
                           metric['llc_size'],
                           metric['local_mem'],
                           metric['remote_mem'],
                           workload.perf_interval)

        logger = logging.getLogger(f'monitoring.metric.{workload}')
        logger.debug(f'{metric} is given from ')

        metric_que = None
        if not workload.profile_solorun:
            logger.debug('Metric_queue : workload.metrics')
            # TODO: Do we need clear()?
            metric_que = workload.metrics
        elif workload.profile_solorun:
            logger.debug('Metric_queue : workload.profile_solorun')
            # init the solorun_data_queue
            # workload.solorun_data_queue.clear()
            # suspend ALL BGs in the same socket
            metric_que = workload.solorun_data_queue

        if len(metric_que) == self._metric_buf_size:
            metric_que.pop()

        metric_que.appendleft(item)

    def run(self) -> None:
        logger = logging.getLogger('monitoring')

        self._control_thread.start()

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._rmq_host))
        channel = connection.channel()

        channel.queue_declare(self._rmq_creation_queue)
        channel.basic_consume(self._cbk_wl_creation, self._rmq_creation_queue)

        try:
            logger.debug('starting consuming thread')
            channel.start_consuming()

        except KeyboardInterrupt:
            channel.close()
            connection.close()


class ControlThread(Thread):
    def __init__(self, pending_queue: PendingQueue) -> None:
        super().__init__(daemon=True)

        self._pending_queue: PendingQueue = pending_queue

        self._interval: float = 0.2  # scheduling interval (sec)
        self._count: int = 0  # scheduling counts
        self._profile_interval: float = 1.0  # check interval for phase change (sec)
        self._solorun_interval: float = 2.0  # the FG's solorun profiling interval (sec)
        self._isolation_groups: Dict[IsolationPolicy, int] = dict()
        # Swapper init
        self._swapper: SwapIsolator = SwapIsolator(self._isolation_groups)

    def _isolate_workloads(self) -> None:
        logger = logging.getLogger(__name__)

        self._swapper.try_swap()

        for group, iteration_num in self._isolation_groups.items():
            logger.info('')
            logger.info(f'***************isolation of {group.name} #{iteration_num}***************')

            try:
                if group.profile_needed(self._profile_interval, self._interval, self._count):
                    logger.debug('store_cur_configs')
                    group.store_cur_configs()
                    group.profile_stop_cond = self._count + int(self._solorun_interval / self._interval)
                    logger.debug('reset_to_initial_configs')
                    group.reset()
                    logger.debug(f'profile_solorun ({self._count} ~ {group.profile_stop_cond})')
                    group.profile_solorun()

                    logger.info('skipping isolation because of solorun profiling...')

                elif group.foreground_workload.profile_solorun and self._count > group.profile_stop_cond:
                    logger.debug('all_workload_pause')
                    group.all_workload_pause()

                    logger.debug('fg.profile_solorun = False')
                    group.foreground_workload.profile_solorun = False
                    logger.debug('calc_and_update fg._avg_solorun_data')
                    group.foreground_workload.calc_avg_solorun()
                    logger.debug(f'fg_wl.avg_solorun_data: {group.foreground_workload.avg_solorun_data}')
                    logger.debug('reset_stored_configs')
                    group.reset_stored_configs()

                    logger.debug('all_workload_resume')
                    group.all_workload_resume()

                    logger.info('skipping isolation... because corun data isn\'t collected yet')

                if group.new_isolator_needed:
                    group.choose_next_isolator()

                cur_isolator: Isolator = group.cur_isolator

                decided_next_step: NextStep = cur_isolator.decide_next_step()

                if group.fg_runs_alone is True:
                    decided_next_step = NextStep.IDLE

                logger.info(f'Monitoring Result : {decided_next_step.name}')

                if decided_next_step is NextStep.STRENGTHEN:
                    cur_isolator.strengthen()
                elif decided_next_step is NextStep.WEAKEN:
                    cur_isolator.weaken()
                elif decided_next_step is NextStep.STOP:
                    group.set_idle_isolator()
                    continue
                elif decided_next_step is NextStep.IDLE:
                    continue
                else:
                    raise NotImplementedError(f'unknown isolation result : {decided_next_step}')

                cur_isolator.enforce()

            except (psutil.NoSuchProcess, subprocess.CalledProcessError, ProcessLookupError):
                pass

            finally:
                self._isolation_groups[group] += 1
                self._count += 1

    def _register_pending_workloads(self) -> None:
        """
        This function detects and registers the spawned workloads(threads).
        """
        logger = logging.getLogger(__name__)

        # set pending workloads as active
        while len(self._pending_queue):
            pending_group: IsolationPolicy = self._pending_queue.pop()
            logger.info(f'{pending_group} is created')

            self._isolation_groups[pending_group] = 0
            pending_group.init_isolators()

    def _remove_ended_groups(self) -> None:
        """
        deletes the finished workloads(threads) from the dict.
        """
        logger = logging.getLogger(__name__)

        ended = tuple(filter(lambda g: g.ended, self._isolation_groups))

        for group in ended:
            if group.foreground_workload.is_running:
                ended_workload = group.background_workload
            else:
                ended_workload = group.foreground_workload
            logger.info(f'{group} of {ended_workload.name} is ended')

            # remove from containers
            group.reset()
            del self._isolation_groups[group]

    def _profile_solorun(self) -> None:
        """
        profile solorun status of a workload
        :return:
        """
        all_fg_wls = list()
        all_bg_wls = list()
        # suspend all workloads and their perf agents
        for group in self._isolation_groups:
            fg_wl = group.foreground_workload
            bg_wl = group.background_workload
            fg_wl.pause()
            bg_wl.pause()
            all_fg_wls.append(fg_wl)
            all_bg_wls.append(bg_wl)

        # run FG workloads alone
        for fg_wl in all_fg_wls:
            fg_wl.profile_solorun = True
            fg_wl.resume()

        # four seconds for monitoring solo-run
        time.sleep(self._solorun_interval)

        # disable solorun mode
        for fg_wl in all_fg_wls:
            fg_wl.profile_solorun = False

        # resume BG workloads
        for bg_wl in all_bg_wls:
            bg_wl.resume()

    def _update_all_workloads_num_threads(self):
        """
        update the workloads' number of threads (cur_num_threads -> prev_num_threads)
        :return:
        """
        for group in self._isolation_groups:
            bg_wl = group.background_workload
            fg_wl = group.foreground_workload
            bg_wl.update_num_threads()
            fg_wl.update_num_threads()

    def _profile_needed(self, count: int) -> bool:
        """
        This function checks if the profiling procedure should be called

        profile_freq : the frequencies of online profiling
        :param count: This counts the number of entering the run func. loop
        :return: Decision whether to initiate online solorun profiling
        """

        profile_freq = int(self._profile_interval / self._interval)
        for group in self._isolation_groups:
            fg_wl = group.foreground_workload
            if count % profile_freq != 0 and fg_wl.is_num_threads_changed():
                self._update_all_workloads_num_threads()
                return False
            else:
                self._update_all_workloads_num_threads()
                return True

    def run(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info('starting isolation loop')
        # count = 0
        while True:
            self._remove_ended_groups()
            self._register_pending_workloads()

            time.sleep(self._interval)
            # count += 1
            # if self._profile_needed(count):
            #    self._profile_solorun()
            self._isolate_workloads()


def main() -> None:
    parser = argparse.ArgumentParser(description='Run workloads that given by parameter.')
    parser.add_argument('-b', '--metric-buf-size', dest='buf_size', default='50', type=int,
                        help='metric buffer size per thread. (default : 50)')

    os.makedirs('logs', exist_ok=True)

    args = parser.parse_args()

    formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(f'logs/debug_{datetime.datetime.now().isoformat()}.log')
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    controller_logger = logging.getLogger(__name__)
    controller_logger.setLevel(logging.INFO)
    controller_logger.addHandler(stream_handler)
    controller_logger.addHandler(file_handler)

    module_logger = logging.getLogger(isolating_controller.__name__)
    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(stream_handler)
    module_logger.addHandler(file_handler)

    monitoring_logger = logging.getLogger('monitoring')
    monitoring_logger.setLevel(logging.DEBUG)
    monitoring_logger.addHandler(stream_handler)
    monitoring_logger.addHandler(file_handler)

    controller = MainController(args.buf_size)
    controller.run()


if __name__ == '__main__':
    if sys.version_info < MIN_PYTHON:
        sys.exit('Python {}.{} or later is required.\n'.format(*MIN_PYTHON))

    main()

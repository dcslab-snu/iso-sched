#!/usr/bin/env python3
# coding: UTF-8

import argparse
import datetime
import logging
import os
import subprocess
import sys
import time
from typing import Dict, Optional

import psutil

import libs
from libs.isolation import NextStep
from libs.isolation.isolators import Isolator
from libs.isolation.policies import AggressiveWViolationPolicy, IsolationPolicy
from libs.isolation.swapper import SwapIsolator
from pending_queue import PendingQueue
from polling_thread import PollingThread

MIN_PYTHON = (3, 6)


class Controller:
    def __init__(self, metric_buf_size: int, swap_off: bool) -> None:
        self._pending_queue: PendingQueue = PendingQueue(AggressiveWViolationPolicy)

        self._interval: float = 0.2  # scheduling interval (sec)
        self._profile_interval: float = 1.0  # check interval for phase change (sec)
        self._solorun_interval: float = 2.0  # the FG's solorun profiling interval (sec)
        self._solorun_count: Dict[IsolationPolicy, Optional[int]] = dict()

        self._isolation_groups: Dict[IsolationPolicy, int] = dict()

        self._polling_thread = PollingThread(metric_buf_size, self._pending_queue)
        self._swap_off: bool = swap_off

        # Swapper init
        self._swapper: SwapIsolator = SwapIsolator(self._isolation_groups)

    def _isolate_workloads(self) -> None:
        logger = logging.getLogger(__name__)

        for group, iteration_num in self._isolation_groups.items():
            logger.info('')
            logger.info(f'***************isolation of {group.name} #{iteration_num}***************')

            try:
                if group.in_solorun_profiling:
                    if iteration_num - self._solorun_count[group] >= int(self._solorun_interval / self._interval):
                        logger.info('Stopping solorun profiling...')

                        group.stop_solorun_profiling()
                        del self._solorun_count[group]

                        logger.info('skipping isolation... because corun data isn\'t collected yet')
                    else:
                        logger.info('skipping isolation because of solorun profiling...')

                    continue

                # TODO: first expression can lead low reactivity
                elif iteration_num % int(self._profile_interval / self._interval) == 0 and group.profile_needed():
                    logger.info('Starting solorun profiling...')
                    group.start_solorun_profiling()
                    self._solorun_count[group] = iteration_num
                    group.set_idle_isolator()
                    logger.info('skipping isolation because of solorun profiling...')
                    continue

                if group.new_isolator_needed:
                    group.choose_next_isolator()

                cur_isolator: Isolator = group.cur_isolator

                decided_next_step: NextStep = cur_isolator.decide_next_step()
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

        if not self._swap_off and len(tuple(g for g in self._isolation_groups if g.safe_to_swap)) >= 2:
            if self._swapper.swap_is_needed():
                self._swapper.do_swap()

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

    def _remove_ended_groups(self) -> None:
        """
        deletes the finished workloads(threads) from the dict.
        """
        logger = logging.getLogger(__name__)

        ended = tuple(filter(lambda g: g.ended, self._isolation_groups))

        for group in ended:
            if group.foreground_workload.is_running:
                logger.info(f'{group} of backgrounds are ended')
            else:
                logger.info(f'{group} of {group.foreground_workload.name} is ended')

            # remove from containers
            group.reset()
            del self._isolation_groups[group]
            if group.in_solorun_profiling:
                for bg in filter(lambda w: w.is_running, group.background_workloads):
                    bg.resume()
                del self._solorun_count[group]

    def run(self) -> None:
        self._polling_thread.start()

        logger = logging.getLogger(__name__)
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

    parser.add_argument('--swap-off', action='store_true', help='turn off swapper')

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

    module_logger = logging.getLogger(libs.__name__)
    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(stream_handler)
    module_logger.addHandler(file_handler)

    monitoring_logger = logging.getLogger('monitoring')
    monitoring_logger.setLevel(logging.INFO)
    monitoring_logger.addHandler(stream_handler)
    monitoring_logger.addHandler(file_handler)

    controller = Controller(args.buf_size, args.swap_off)
    controller.run()


if __name__ == '__main__':
    if sys.version_info < MIN_PYTHON:
        sys.exit('Python {}.{} or later is required.\n'.format(*MIN_PYTHON))

    main()

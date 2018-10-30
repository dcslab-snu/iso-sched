# coding: UTF-8

import functools
import json
import logging
from threading import Thread

import pika
import psutil
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from libs.metric_container.basic_metric import BasicMetric
from libs.workload import Workload
from pending_queue import PendingQueue


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class PollingThread(Thread, metaclass=Singleton):
    def __init__(self, metric_buf_size: int, pending_queue: PendingQueue) -> None:
        super().__init__(daemon=True)
        self._metric_buf_size = metric_buf_size

        self._rmq_host = 'localhost'
        self._rmq_creation_queue = 'workload_creation'

        self._pending_wl = pending_queue

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

        metric_que = workload.metrics

        if len(metric_que) == self._metric_buf_size:
            metric_que.pop()

        metric_que.appendleft(item)

    def run(self) -> None:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._rmq_host))
        channel = connection.channel()

        channel.queue_declare(self._rmq_creation_queue)
        channel.basic_consume(self._cbk_wl_creation, self._rmq_creation_queue)

        try:
            logger = logging.getLogger('monitoring')
            logger.debug('starting consuming thread')
            channel.start_consuming()

        except KeyboardInterrupt:
            channel.close()
            connection.close()

#!/usr/bin/env python
# coding=UTF-8

from __future__ import division, print_function

import multiprocessing
import os
import sys
import time
from signal import SIGCONT, SIGSTOP
from threading import Timer

import psutil

dead_status = (psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE)


class ProcessArbitrator:
    def __init__(self, pids, time_slice=50, iteration_limit=None):
        """
        생성자

        Args:
            pids (list of tuple): (실행할 process의 pid, perf의 pid)들의 list
            time_slice (int): 한 process가 실행될 interval (ms 단위). 기본값은 500
            iteration_limit (int): 각 pid들의 반복횟수를 제한. `None` 으로하면 무제한. 기본값은 `None`

        Raises:
            ValueError: pid의 타입이 이상할 경우

        Notes
            `time_slice` 가 0일경우 제대로 동작안함 (중요하지 않아보여서 처리하지 않음)
        """
        if not pids:
            raise ValueError('`pids` cannot be `None`')

        self._iteration_limit = iteration_limit
        self._time_slice = time_slice
        self._all_pids = list(pids)
        self._remain_pids = list(pids)
        self.next_proc()

    def next_proc(self):
        # self._print_status()

        self._stop_all()
        # print 'all process stopped'

        next_pid = self.pick_next_proc()

        if not next_pid:
            # print 'no more process to run'
            return

        # print 'next process is : ' + str(next_pid)
        if next_pid[0] is not None:
            os.kill(next_pid[0], SIGCONT)

        if next_pid[1] is not None:
            os.kill(next_pid[1], SIGCONT)

        Timer(self._time_slice / 1000, self.next_proc).start()

    def pick_next_proc(self):
        """
        `ProcessArbitrator` 에 포함된 process중에서 다음 time slice때 실행될 process의 pid를 구한다.
        더이상 실행할 process가 없을 때, 혹은 `iteration_limit` 에 도달했을때 `None` 을 반환한다.

        Returns:
            tuple of int: 다음 time slice에 실행할 process의 pid
        """
        while True:
            if len(self._remain_pids) is 0:
                if len(self._all_pids) is 0:
                    return None

                elif self._iteration_limit is 1:
                    self._resume_all()
                    return None

                else:
                    self._remain_pids.extend(self._all_pids)
                    if self._iteration_limit:
                        self._iteration_limit -= 1

            next_pid = self._remain_pids.pop()

            is_ps1_dead = False
            is_ps2_dead = False

            try:
                if psutil.Process(next_pid[0]).status() in dead_status:
                    is_ps1_dead = True
            except psutil.NoSuchProcess:
                is_ps1_dead = True

            try:
                if psutil.Process(next_pid[1]).status() in dead_status:
                    is_ps2_dead = True
            except psutil.NoSuchProcess:
                is_ps2_dead = True

            if is_ps1_dead and not is_ps2_dead:
                return None, next_pid[1]

            elif not is_ps1_dead and is_ps2_dead:
                return next_pid[0], None

            elif not is_ps1_dead and not is_ps2_dead:
                return next_pid

            else:
                self._all_pids.remove(next_pid)

    def set_time_slice(self, time_slice):
        self._time_slice = time_slice

    def _stop_all(self):
        try:
            for pid in self._all_pids:
                os.kill(pid[0], SIGSTOP)
                os.kill(pid[1], SIGSTOP)
        except:
            pass

    def _resume_all(self):
        try:
            for pid in self._all_pids:
                os.kill(pid[0], SIGCONT)
                os.kill(pid[1], SIGCONT)
        except:
            pass

    def _print_status(self):
        for pid in self._all_pids:
            try:
                process = psutil.Process(pid[0])
                sys.stdout.write(str(process.pid) + ':' + process.status() + ', ')
            except psutil.NoSuchProcess:
                pass
        print()


def main():
    num = 4
    from datetime import datetime

    def test_thread(name):
        for i in range(num):
            time.sleep(1)
            sys.stderr.write('{}\t{}({})\t{}\n'.format(datetime.now(), name, os.getpid(), i))

        return

    processes = []

    try:
        proc_num = 2

        pids = []

        for n in range(proc_num):
            process = multiprocessing.Process(target=test_thread, args=('process #' + str(n),))
            process.start()
            process2 = multiprocessing.Process(target=test_thread, args=('process #' + str(n) + '\'s sidekick',))
            process2.start()
            pids.append((process.pid, process2.pid))

            processes.append(process)
            processes.append(process2)

        ProcessArbitrator(pids, 50)

        for process in processes:
            print('start to join {0}'.format(process.pid))
            process.join()
            print('end of {0}'.format(process.pid))

    except KeyboardInterrupt:
        for process in processes:
            process.terminate()


if __name__ == '__main__':
    main()

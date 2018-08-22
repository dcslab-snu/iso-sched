# coding: UTF-8

import json
from pathlib import Path
from typing import Dict

from ..metric_container.basic_metric import BasicMetric

data_map: Dict[str, BasicMetric] = dict()


def _init() -> None:
    for data in Path(__file__).parent.iterdir():  # type: Path
        if data.match('*.json'):
            metric = json.loads(data.read_text())

            item = BasicMetric(metric['l2miss'],
                               metric['l3miss_load'],
                               metric['l3miss'],
                               metric['instructions'],
                               metric['cycles'],
                               metric['stall_cycles'],
                               metric['wall_cycles'],
                               metric['intra_coh'],
                               metric['inter_coh'],
                               metric['llc_size'],
                               metric['local_mem'],
                               metric['remote_mem'])

            data_map[metric['name']] = item


_init()

import argparse
import csv
import os.path
import random
import signal
import sqlite3
import sys
import time
from decimal import Decimal
from pathlib import Path
import random

import yaml

from dhalsim.py3_logger import get_logger


class GenericAgent:
    """
    This class represent a control agent which can work with Differential Evolution or DQN.
    This agent knows the state of the network by reading the yaml file at intermediate_yaml_path and communicating
    with SCADA.
    """
    def __init__(self, intermediate_yaml_path):
        with intermediate_yaml_path.open() as yaml_file:
            self.intermediate_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.logger = get_logger(self.intermediate_yaml['log_level'])

        self.logger.info('YYYYYYYEEEEEEEEEEEEEEEEEEEEEEEEEEEE NEW CONTROL AGENT!!!!')
        time.sleep(1000)










def is_valid_file(parser_instance, arg):
    if not os.path.exists(arg):
        parser_instance.error(arg + " does not exist")
    else:
        return arg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start everything for a scada')
    parser.add_argument(dest="intermediate_yaml",
                        help="intermediate yaml file", metavar="FILE",
                        type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()


    agent = GenericAgent(intermediate_yaml_path=Path(args.intermediate_yaml))



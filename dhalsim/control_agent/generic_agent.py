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
import zmq
import random

import yaml

from dhalsim.py3_logger import get_logger
from dhalsim.init_database import ControlDatabase


class GenericAgent:
    """
    This class represent a control agent which can work with Differential Evolution or DQN.
    This agent knows the state of the network by reading the yaml file at intermediate_yaml_path and communicating
    with SCADA.
    """
    def __init__(self, agent_yaml_path, intermediate_yaml_paths):
        with agent_yaml_path.open(mode='r') as yaml_file:
            self.agent_config = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.logger = get_logger(self.agent_config['log_level'])

        # Debug the intermediate yaml paths
        for i, yaml_path in enumerate(intermediate_yaml_paths):
            self.logger.debug(str(i) + ': ' + str(yaml_path))

        # Read the first intermediate yaml to get db_path
        self.intermediate_yaml_data = None
        with Path(intermediate_yaml_paths[0]).open(mode='r') as yaml_file:
            self.intermediate_yaml_data = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.control_db_path = self.intermediate_yaml_data['db_control_path']

        # Debug prints to see if it works
        # TODO: delete these lines
        self.logger.info('YYYYYYYEEEEEEEEEEEEEEEEEEEEEEEEEEEE NEW CONTROL AGENT!!!!')

        self.control_db = ControlDatabase(agent_yaml_path, Path(intermediate_yaml_paths[0]))
        self.conn = sqlite3.connect(self.intermediate_yaml_data['db_control_path'])
        self.cur = self.conn.cursor()

    def get_scada_ready(self):
        """
        Check if scada has sent
        """
        pass


def is_valid_file(parser_instance, arg):
    if not os.path.exists(arg):
        parser_instance.error(arg + " does not exist")
    else:
        return arg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start the control agent process')
    parser.add_argument(dest="agent_yaml_path",
                        help="agent config yaml file", metavar="FILE",
                        type=lambda x: is_valid_file(parser, x))
    parser.add_argument('-n', '--intermediate_yaml_paths', nargs='+', default=[],
                        help="paths of intermediate yaml")

    args = parser.parse_args()

    agent = GenericAgent(agent_yaml_path=Path(args.agent_yaml_path),
                         intermediate_yaml_paths=args.intermediate_yaml_paths)



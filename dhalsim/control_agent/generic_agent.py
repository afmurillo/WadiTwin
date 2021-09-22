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
from dhalsim.init_database import ControlDatabase
from dhalsim.control_agent.dqn.dqn import DQNAgent


class GenericAgent:
    """
    This class represent a control agent which can work with Differential Evolution or DQN.
    This agent knows the state of the network by reading the yaml file at intermediate_yaml_path and communicating
    with SCADA.
    """
    def __init__(self, agent_yaml_path):
        """

        """
        self.agent_yaml_path = agent_yaml_path

        with self.agent_yaml_path.open(mode='r') as yaml_file:
            self.agent_config = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.logger = get_logger(self.agent_config['log_level'])

        # Intermediate yaml and control database data, useful during the simulation
        self.intermediate_yaml_data = None
        self.control_db = None

        self.agent = None
        self.control_db = ControlDatabase()

    def on_simulation_init(self, intermediate_yaml_path):
        """
        Creation of the agent and control database if not already existent.
        Reset the agent environment for the next simulation.

        :param intermediate_yaml_path: path of the current simulation intermediate_yaml file
        :type intermediate_yaml_path: Path
        """
        self.control_db.init_tables(intermediate_yaml_path)

        if self.agent is None:
            self.agent = DQNAgent(self.agent_yaml_path)

        self.agent.reset_environment(intermediate_yaml_path)
        self.agent.start_control_problem()

    def on_simulation_done(self):
        """
        Communicates to the agent that the simulation has terminated.
        TODO: understand if we can do in this way
        """
        pass
        # self.agent.done()


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

    agent = GenericAgent(agent_yaml_path=Path(args.agent_yaml_path))
                         #intermediate_yaml_paths=args.intermediate_yaml_paths)



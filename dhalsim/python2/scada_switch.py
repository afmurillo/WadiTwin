import argparse
import csv
import os.path
import random
import signal
import sqlite3
import sys
import subprocess
import time
from abc import ABC

import zmq
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from socket import *

from pathlib import Path

import yaml
from automatic_node import NodeControl

from py2_logger import get_logger
import threading
import thread


# TODO: scada has to communicate with switch that can communicate with external host
class ScadaSwitch(NodeControl):
    """
    Client process of the scada interface
    """
    def __init__(self, intermediate_yaml_path):
        super(ScadaSwitch, self).__init__(intermediate_yaml_path)

        self.logger = get_logger(self.data['log_level'])

        self.logger.info("SWITCH INITIALIZED")
        self.socket = None

    def start_listener(self):
        """
        Start the agent server which will receive data from SCADA client
        """
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:5555")
        self.on_listening()

    def on_listening(self):
        """

        """
        while True:
            self.logger.info('SWITCH LISTENING')
            #  Wait for next request from client
            message = self.socket.recv()
            self.logger.info("Received message: " + str(message))
            time.sleep(1)
            # self.socket.send_string("World from %s" % "5555")


def is_valid_file(parser_instance, arg):
    """
    Verifies whether the intermediate yaml path is valid.

    :param parser_instance: instance of argparser
    :param arg: the path to check
    """
    if not os.path.exists(arg):
        parser_instance.error(arg + " does not exist.")
    else:
        return arg


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start everything for a scada')
    parser.add_argument(dest="intermediate_yaml",
                        help="intermediate yaml file", metavar="FILE",
                        type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()

    switch = ScadaSwitch(intermediate_yaml_path=Path(args.intermediate_yaml))

import argparse
import csv
import os.path
import random
import signal
import socket
import sqlite3
import sys
import subprocess
import time
import zmq
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from socket import *

from pathlib import Path

import yaml
from basePLC import BasePLC

from py2_logger import get_logger
import threading
import thread

empty_loc = '/dev/null'


class Error(Exception):
    """Base class for exceptions in this module."""


class TagDoesNotExist(Error):
    """Raised when tag you are looking for does not exist"""


class InvalidControlValue(Error):
    """Raised when tag you are looking for does not exist"""


class DatabaseError(Error):
    """Raised when not being able to connect to the database"""


class GenericScada(BasePLC):
    """
    This class represents a scada. This scada knows what plcs it is collecting data from by reading the
    yaml file at intermediate_yaml_path and looking at the plcs.
    """

    DB_TRIES = 10
    """Amount of times a db query will retry on a exception"""

    DB_SLEEP_TIME = random.uniform(0.01, 0.1)
    """Amount of time a db query will wait before retrying"""

    SCADA_CACHE_UPDATE_TIME = 2
    """ Time in seconds the SCADA server updates its cache"""

    def __init__(self, intermediate_yaml_path):
        with intermediate_yaml_path.open() as yaml_file:
            self.intermediate_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.logger = get_logger(self.intermediate_yaml['log_level'])
        self.logger.info("WOOOOO SCADA")
        self.socket = None
        # self.start_client()
        # self.other_client()

        # Initialize connection to the databases (also control_db if used)
        self.conn = None
        self.cur = None
        self.control_conn = None
        self.control_cur = None
        self.initialize_db()

        self.output_path = Path(self.intermediate_yaml["output_path"]) / "scada_values.csv"
        self.output_path.touch(exist_ok=True)

        # Create state from db values
        state = {
            'name': "plant",
            'path': self.intermediate_yaml['db_path']
        }

        # Create server, real tags are generated
        scada_server = {
            'address': self.intermediate_yaml['scada']['local_ip'],
            'tags': self.generate_real_tags(self.intermediate_yaml['plcs'])
        }

        # Create protocol
        scada_protocol = {
            'name': 'enip',
            'mode': 1,
            'server': scada_server
        }

        self.plc_data = self.generate_plcs()
        self.saved_values = [['iteration', 'timestamp']]

        for PLC in self.intermediate_yaml['plcs']:
            if 'sensors' not in PLC:
                PLC['sensors'] = list()

            if 'actuators' not in PLC:
                PLC['actuators'] = list()
            self.saved_values[0].extend(PLC['sensors'])
            self.saved_values[0].extend(PLC['actuators'])

        self.update_cache_flag = False
        self.plcs_ready = False

        self.cache = {}
        for ip in self.plc_data:
            self.cache[ip] = [0] * len(self.plc_data[ip])

        self.do_super_construction(scada_protocol, state)

    def do_super_construction(self, scada_protocol, state):
        """
        Function that performs the super constructor call to SCADAServer
        Introduced to better facilitate testing
        """
        super(GenericScada, self).__init__(name='scada', state=state, protocol=scada_protocol)

    def start_client(self):
        """
        Start the SCADA client which has to communicate with the control agent server
        """
        context = zmq.Context()
        self.logger.info("CONNECTING TO SWITCH...")
        self.socket = context.socket(zmq.REQ)
        # localhost = self.intermediate_yaml['scada']['local_ip']
        # self.logger.info("Localhost: " + str(localhost))
        self.socket.connect("tcp://0.0.0.0:5555")
        self.logger.info("CONNECTED")
        self.socket.send('HELLO')
        #message = self.socket.recv()

    def other_client(self):
        self.socket = socket(AF_INET, SOCK_STREAM)
        self.logger.info('OTHER SOCKET CLIENT...')
        self.socket.setsockopt(SOL_SOCKET, 25, 'enp0s8'+'\0')
        self.socket.connect(('localhost', 5556))
        self.socket.send(b'HELLO')

    def initialize_db(self):
        """
        Function that initializes PLC connection to the database
        Introduced to better facilitate testing
        """
        self.conn = sqlite3.connect(self.intermediate_yaml["db_path"])
        self.cur = self.conn.cursor()

        if self.intermediate_yaml['use_control_agent']:
            self.control_conn = sqlite3.connect(self.intermediate_yaml['db_control_path'])
            self.control_cur = self.control_conn.cursor()

    @staticmethod
    def generate_real_tags(plcs):
        """
        Generates real tags with all sensors and actuators attached to plcs in the network.

        :param plcs: list of plcs
        """
        real_tags = []

        for plc in plcs:
            if 'sensors' not in plc:
                plc['sensors'] = list()

            if 'actuators' not in plc:
                plc['actuators'] = list()

            for sensor in plc['sensors']:
                if sensor != "":
                    real_tags.append((sensor, 1, 'REAL'))
            for actuator in plc['actuators']:
                if actuator != "":
                    real_tags.append((actuator, 1, 'REAL'))

        return tuple(real_tags)

    @staticmethod
    def generate_tags(taggable):
        """
        Generates tags from a list of taggable entities (sensor or actuator)

        :param taggable: a list of strings containing names of things like tanks, pumps, and valves
        """
        tags = []

        if taggable:
            for tag in taggable:
                if tag and tag != "":
                    tags.append((tag, 1))

        return tags

    def pre_loop(self, sleep=0.5):
        """
        The pre loop of a SCADA. In which setup actions are started.

        :param sleep:  (Default value = 0.5) The time to sleep after setting everything up
        """
        self.logger.debug('SCADA enters pre_loop')

        if self.intermediate_yaml['use_control_agent']:
            self.send_actuator_values_flag = True
            self.actuators_state_lock = threading.Lock()
            self.init_actuator_values()

        signal.signal(signal.SIGINT, self.sigint_handler)
        signal.signal(signal.SIGTERM, self.sigint_handler)

        self.keep_updating_flag = True
        self.cache_update_process = None
        time.sleep(sleep)

    def db_query(self, query, parameters=None):
        """
        Execute a query on the database
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.
        This is necessary because of the limited concurrency in SQLite.

        :param query: The SQL query to execute in the db
        :type query: str

        :param parameters: The parameters to put in the query. This must be a tuple.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        for i in range(self.DB_TRIES):
            try:
                if parameters:
                    self.cur.execute(query, parameters)
                else:
                    self.cur.execute(query)
                return
            except sqlite3.OperationalError as exc:
                self.logger.info(
                    "Failed to connect to db with exception {exc}. Trying {i} more times.".format(
                        exc=exc, i=self.DB_TRIES - i - 1))
                time.sleep(self.DB_SLEEP_TIME)
        self.logger.error(
            "Failed to connect to db. Tried {i} times.".format(i=self.DB_TRIES))
        raise DatabaseError("Failed to get master clock from database")

    def get_sync(self, cur):
        """
        Get the sync flag of the scada.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :return: False if physical process wants the plc to do a iteration, True if not.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        self.db_query("SELECT flag FROM sync WHERE name IS 'scada'")
        flag = bool(cur.fetchone()[0])
        return flag

    def set_sync(self, flag):
        """
        Set the scada's sync flag in the sync table. When this is 1, the physical process
        knows that the scada finished the requested iteration.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :param flag: True for sync to 1, False for sync to 0
        :type flag: bool

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        self.db_query("UPDATE sync SET flag=? WHERE name IS 'scada'",
                         (int(flag),))
        self.conn.commit()

    def stop_cache_update(self):
        self.update_cache_flag = False

    def sigint_handler(self, sig, frame):
        """
        Shutdown protocol for the scada, writes the output before exiting.
        """
        self.stop_cache_update()
        self.logger.debug("SCADA shutdown")
        self.write_output()
        self.send_actuator_values_flag = False

        sys.exit(0)

    def write_output(self):
        """
        Writes the csv output of the scada
        """
        with self.output_path.open(mode='wb') as output:
            writer = csv.writer(output)
            writer.writerows(self.saved_values)

    def generate_plcs(self):
        """
        Generates a list of tuples, the first part being the ip of a PLC,
        and the second  being a list of tags attached to that PLC.
        """
        plcs = OrderedDict()

        for PLC in self.intermediate_yaml['plcs']:
            if 'sensors' not in PLC:
                PLC['sensors'] = list()

            if 'actuators' not in PLC:
                PLC['actuators'] = list()

            tags = []

            tags.extend(self.generate_tags(PLC['sensors']))
            tags.extend(self.generate_tags(PLC['actuators']))

            plcs[PLC['public_ip']] = tags

        return plcs

    def get_master_clock(self):
        """
        Get the value of the master clock of the physical process through the database.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :return: Iteration in the physical process.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        self.db_query("SELECT time FROM master_time WHERE id IS 1")
        master_time = self.cur.fetchone()[0]
        return master_time

    def update_cache(self, lock, cache_update_time):
        """
        Update the cache of the scada by receiving all the required tags.
        When something cannot be received, the previous values are used.
        """

        while self.update_cache_flag:
            for plc_ip in self.cache:
                try:
                    values = self.receive_multiple(self.plc_data[plc_ip], plc_ip)
                    with lock:
                        self.cache[plc_ip] = values
                    #self.logger.debug("PLC values received by SCADA from IP: " + str(plc_ip) + " is " + str(values) + ".")
                except Exception as e:
                    self.logger.error(
                        "PLC receive_multiple with tags {tags} from {ip} failed with exception '{e}'".format(
                            tags=self.plc_data[plc_ip],
                            ip=plc_ip, e=str(e)))
                    time.sleep(cache_update_time)
                    continue
            time.sleep(cache_update_time)

    def init_actuator_values(self):
        """
        This method is only called if the global parameter control is set to DQN_Control.
        Reads intermediate_yaml and initializes the actuators with the values defined in [STATUS] section of .inp file
        """

        if self.intermediate_yaml['use_control_agent']:
            self.actuator_status_cache = {}
            self.sendable_tags = []
            for actuator in self.intermediate_yaml['actuators']:
                if actuator['initial_state'].lower() == 'open':
                    self.actuator_status_cache[actuator['name']] = 1
                else:
                    self.actuator_status_cache[actuator['name']] = 0
                self.sendable_tags.append((actuator['name'], 1))
            #self.logger.debug('Initialized the actuators_status_cache with: ' + str(self.actuator_status_cache))
            #self.logger.debug('Tags for send are: ' + str(self.sendable_tags))

    def update_actuator_values(self):
        """
        This method is only called if the global parameter control is set to DQN_Control
        Method only for testing
        """

        #todo: This function subscribes to the control agent publisher to get each actuator status
        with self.actuators_state_lock:
            for actuator in self.intermediate_yaml['actuators']:
                if self.actuator_status_cache[actuator['name']] == 1:
                    self.actuator_status_cache[actuator['name']] = 0
                elif self.actuator_status_cache[actuator['name']] == 0:
                    self.actuator_status_cache[actuator['name']] = 1
            #self.logger.debug('Updated the actuators_status_cache with: ' + str(self.actuator_status_cache))

    def send_actuator_values(self, a, b):
        """
        This method is only called if the global parameter control is set to DQN_Control
        Method running on a thread that sends the actuator status for the PLCs to query
        """
        while self.send_actuator_values_flag:
            #self.logger.debug('sending actuators tags: ' + str(self.sendable_tags))
            #self.logger.debug('sending actuators values: ' + str(self.actuator_status_cache.values()))
            # We possible need to do (tag,1) here.
            self.send_multiple(self.sendable_tags, self.actuator_status_cache.values(),
                               self.intermediate_yaml['scada']['local_ip'],)
            time.sleep(0.05)

    def main_loop(self, sleep=0.5, test_break=False):
        """
        The main loop of a PLC. In here all the controls will be applied.

        :param sleep:  (Default value = 0.5) Not used
        :param test_break:  (Default value = False) used for unit testing, breaks the loop after one iteration
        """

        self.logger.debug("SCADA enters main_loop")
        lock = None

        if 'DQN_Control' in self.intermediate_yaml and self.intermediate_yaml['DQN_Control']:
            thread.start_new_thread(self.send_actuator_values, (0, 0))

        while True:
            while self.get_sync():
                time.sleep(self.DB_SLEEP_TIME)

            # Wait until we acquire the first sync before polling the PLCs
            if not self.plcs_ready:
                self.plcs_ready = True
                self.update_cache_flag = True
                self.logger.debug("SCADA starting update cache thread")
                lock = threading.Lock()
                thread.start_new_thread(self.update_cache, (lock, self.SCADA_CACHE_UPDATE_TIME))

            # Self.plc_data has all the tag names and the index is plc_ip
            # Self.cache has all the values of tag and the index is plc_ip
            # Better to use a copy and not directly access self.cache values since it has a lock

            master_time = self.get_master_clock()
            results = [master_time, datetime.now()]
            with lock:
                for plc_ip in self.plc_data:
                    results.extend(self.cache[plc_ip])
                    #self.logger.debug("scada values: " + str(self.plc_data[plc_ip]))
                    #self.logger.debug("scada values: " + str(self.cache[plc_ip]))
            self.saved_values.append(results)

            # Save scada_values.csv when needed
            if 'saving_interval' in self.intermediate_yaml and master_time != 0 and \
                    master_time % self.intermediate_yaml['saving_interval'] == 0:
                self.write_output()

            if 'DQN_Control' in self.intermediate_yaml and self.intermediate_yaml['DQN_Control']:
                self.update_actuator_values()

            self.set_sync(1)

            if test_break:
                break


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

    plc = GenericScada(intermediate_yaml_path=Path(args.intermediate_yaml))

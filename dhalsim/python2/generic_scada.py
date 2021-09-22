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

        # Initialize connection to the databases (also control_db if used)
        self.conn = None
        self.cur = None
        self.control_conn = None
        self.control_cur = None
        self.initialize_db()

        # Code executed only if we want to face the control problem
        if self.intermediate_yaml['use_control_agent']:
            # Tags of state and action variables
            self.action_vars = []
            self.state_vars = []
            self.generate_variables_collections()

            self.logger.info(self.action_vars)
            self.logger.info(self.state_vars)

            # Control database prepared statement
            self._table_name = ['state_space', 'action_space']
            self._value = 'value'
            self._what = tuple()
            self._set_query = None
            self._get_query = None

            self._init_what()

            if not self._what:
                raise ValueError('Primary key not found.')
            else:
                self._init_get_query()
                self._init_set_query()

            #TODO: comment
            self.logger.info(self._what)

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

    def generate_variables_collections(self):
        """
        Create lists containing ids of action and state variables
        """
        for i in range(len(self.intermediate_yaml['plcs'])):
            self.action_vars.extend(self.intermediate_yaml['plcs'][i]['actuators'])
            self.state_vars.extend(self.intermediate_yaml['plcs'][i]['sensors'])

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

    def db_query(self, query, cur, parameters=None):
        """
        Execute a query on the databases.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.
        This is necessary because of the limited concurrency in SQLite.

        :param query: The SQL query to execute in the db
        :type query: str

        :param cur: cursor of the database (data_db or control_db)
        :type cur: sqlite3.Connection

        :param parameters: The parameters to put in the query. This must be a tuple.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        for i in range(self.DB_TRIES):
            try:
                if parameters:
                    cur.execute(query, parameters)
                else:
                    cur.execute(query)
                return
            except sqlite3.OperationalError as exc:
                self.logger.info(
                    "Failed to connect to db with exception {exc}. Trying {i} more times.".format(
                        exc=exc, i=self.DB_TRIES - i - 1))
                time.sleep(self.DB_SLEEP_TIME)
        self.logger.error(
            "Failed to connect to db. Tried {i} times.".format(i=self.DB_TRIES))
        raise DatabaseError("Failed to get master clock from database")

    def get_sync(self, use_control_db=False):
        """
        Get the sync flag of the scada or of the control agent.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :param use_control_db: True to use control_db, False otherwise
        :type use_control_db: bool

        :return: False if physical process wants the plc to do a iteration, True if not.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        # Get sync from control_db
        if use_control_db:
            self.db_query("SELECT flag FROM sync WHERE name IS 'agent'", cur=self.control_cur)
            flag = bool(self.control_cur.fetchone()[0])
        # Get sync from data_db
        else:
            self.db_query("SELECT flag FROM sync WHERE name IS 'scada'", cur=self.cur)
            flag = bool(self.cur.fetchone()[0])
        return flag

    def set_sync(self, flag=None, use_control_db=False):
        """
        Set the scada's sync flag in the sync table. When this is 1, the physical process
        knows that the scada finished the requested iteration.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :param flag: True for sync to 1, False for sync to 0
        :type flag: int (0 or 1)

        :param use_control_db: True to use control_db, False otherwise
        :type use_control_db: bool

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        # Set sync for control_db
        if use_control_db:
            self.db_query("UPDATE sync SET flag=1 WHERE name IS 'scada'", cur=self.control_cur)
            self.db_query("UPDATE sync SET flag=0 WHERE name IS 'agent'", cur=self.control_cur)
            self.control_conn.commit()
        # Set sync for data_db
        else:
            self.db_query("UPDATE sync SET flag=? WHERE name IS 'scada'", cur=self.cur, parameters=(int(flag),))
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
        self.db_query("SELECT time FROM master_time WHERE id IS 1", cur=self.cur)
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
        This method is only called if the global parameter control is set to use_control_agent.
        Reads intermediate_yaml and initializes the actuators with the values defined in [STATUS] section of .inp file
        """
        if self.intermediate_yaml['use_control_agent']:
            self.actuator_status_cache = {}
            self.sendable_tags = []

            while not self.check_control_agent_ready():
                time.sleep(0.01)

            # Retrieve new action space variables from control db
            actuators_status_dict = self.get_control_action()
            with self.actuators_state_lock:
                for actuator in actuators_status_dict.keys():
                    self.actuator_status_cache[actuator] = actuators_status_dict[actuator]
        else:
            for actuator in self.intermediate_yaml['actuators']:
                if actuator['initial_state'].lower() == 'open':
                    self.actuator_status_cache[actuator['name']] = 1
                else:
                    self.actuator_status_cache[actuator['name']] = 0
                self.sendable_tags.append((actuator['name'], 1))
                #TODO: check this
            #self.logger.debug('Initialized the actuators_status_cache with: ' + str(self.actuator_status_cache))
            #self.logger.debug('Tags for send are: ' + str(self.sendable_tags))

    def create_vars_translation_dict(self):
        """
        Build the translation dictionary for the variables which will be used for the control problem.
        """
        action_vars = []
        state_vars = []

        for i in range(len(self.intermediate_yaml['plcs'])):
            action_vars.extend(self.intermediate_yaml['plcs'][i]['actuators'])
            state_vars.extend(self.intermediate_yaml['plcs'][i]['sensors'])

        for var in action_vars:
            translation = self.translate_variable(var)
            self.vars_dict['action_vars'][var] = translation

        for var in state_vars:
            translation = self.translate_variable(var)
            self.vars_dict['state_vars'][var] = translation

        self.logger.info(self.vars_dict)

    @staticmethod
    def translate_variable(var):
        """
        Generate the dictionary to translate the signatures of variables used in the two databases.
        For example: J20D is split in (J20, demand) used in control database.

        :param var: variable to translate
        :type var: basestring

        :return: tuple of (node, property)
        """
        translation_tuple = tuple()

        if var[0] == 'P':
            translation_tuple = (var, 'status')
        elif var[0] == 'V':
            translation_tuple = (var, 'status')
        elif var[0] == 'T':
            translation_tuple = (var, 'level')
        elif var[0] == 'J':
            if var[-1] == 'D':
                translation_tuple = (var[:-1], 'demand')
            else:
                translation_tuple = (var, 'pressure')
        return translation_tuple

    def _init_what(self):
        """
        Save a ordered tuple of pk field names in self._what
        """
        query = "PRAGMA table_info(%s)" % self._table_name[0]

        try:
            self.control_cur.execute(query)
            table_info = self.control_cur.fetchall()

            # last tuple element
            primary_keys = []
            for field in table_info:
                if field[-1] > 0:
                    primary_keys.append(field)

            if not primary_keys:
                self.logger.error('Please provide at least 1 primary key. Has sqlite DB been initialized?.'
                                  ' Aborting')
                sys.exit(1)
            else:
                # sort by pk order
                primary_keys.sort(key=lambda x: x[5])

                what_list = []
                for pk in primary_keys:
                    what_list.append(pk[1])

                self._what = tuple(what_list)

        except sqlite3.Error as e:
            self.logger.error('Error initializing the sqlite DB. Exiting. Error: ' + str(e))
            sys.exit(1)

    def _init_set_query(self):
        """
        Prepared statement to update state_space table.
        """
        set_query = 'UPDATE %s SET %s = ? WHERE %s = ?' % (
            self._table_name[0],
            self._value,
            self._what[0])

        # for composite pk
        for pk in self._what[1:]:
            set_query += ' AND %s = ?' % (
                pk)

        self._set_query = set_query

    def _init_get_query(self):
        """
        Prepared statement to retrieve actuators status from action_space table.
        """
        get_query = 'SELECT %s FROM %s WHERE %s = ?' % (
            self._value,
            self._table_name[1],
            self._what[0])

        # for composite pk
        for pk in self._what[1:]:
            get_query += ' AND %s = ?' % (
                pk)

        self._get_query = get_query

    def write_control_db(self, node_id, value):
        """
        Set new state space values inside the state_space table.

        :param node_id: id of the considered couple (node, property)
        :type node_id: basestring

        :param value: value of the property
        :type value: float
        """
        query_args = (value, node_id)

        self.db_query(query=self._set_query, cur=self.control_cur, parameters=query_args)
        self.control_conn.commit()

    def read_control_db(self, node_id):
        """
        Read the value of the actuators status and retrieve it.

        :param node_id: name of the considered couple (node, property)
        :type node_id: basestring

        :return: value of the actuator status
        """
        query_args = (node_id,)

        self.db_query(query=self._get_query, cur=self.control_cur, parameters=query_args)
        record = self.control_cur.fetchone()
        return record[0]

    def check_control_agent_ready(self):
        """
        Check if the control agent is still busy or if it has already finished the step.
        Care that [flag==0 => busy] and [flag==1 => ready] because of the while declaration.
        """
        self.control_cur.execute("""SELECT flag FROM sync WHERE name=='agent';""")
        flag = self.control_cur.fetchone()[0] == 1
        #TODO: remove following line
        flag = 1
        return flag

    def send_state_space(self):
        """
        This method interact with control database to set the new state space variables retrieved by the communication
        with plcs.
        """
        # List of tuples in the following format: (var_name, new_value)
        """
        new_state = []
        # Enumeration of variable signatures
        for i, val in enumerate(self.saved_values[0]):
            if val in self.vars_dict['state_vars'].keys():
                new_state.append((val, float(self.saved_values[-1][i])))

        for var in new_state:
            node, prop = self.vars_dict['state_vars'][var[0]]
            #self.logger.info(node)
            #self.logger.info(prop)
        """

        for i, var in enumerate(self.saved_values[0]):
            if var in self.state_vars:
                self.write_control_db(node_id=var, value=float(self.saved_values[-1][i]))

        self.set_sync(use_control_db=True)

    def get_control_action(self):
        """
        This method retrieves from control database the actions suggested by the control agent, which basically are the
        new status of the actuators.

        :return: dictionary of new actuators status
        """
        new_action = {}

        for var in self.saved_values[0]:
            if var in self.action_vars:
                new_status = self.read_control_db(node_id=var)
                new_action[var] = new_status

        return new_action

    def execute_control_step(self):
        """
        This method is only called if the global parameter control is set to use_control_agent.
        """
        # Send new state space variables to control db
        self.send_state_space()

        while not self.check_control_agent_ready():
            time.sleep(0.01)

        # Retrieve new action space variables from control db
        actuators_status_dict = self.get_control_action()
        self.logger.info("New action: " + str(actuators_status_dict))

        # Update actuators status
        with self.actuators_state_lock:
            for actuator in actuators_status_dict.keys():
                self.actuator_status_cache[actuator] = actuators_status_dict[actuator]
        """
        with self.actuators_state_lock:
            for actuator in self.intermediate_yaml['actuators']:
                if self.actuator_status_cache[actuator['name']] == 1:
                    self.actuator_status_cache[actuator['name']] = 0
                elif self.actuator_status_cache[actuator['name']] == 0:
                    self.actuator_status_cache[actuator['name']] = 1
            # self.logger.debug('Updated the actuators_status_cache with: ' + str(self.actuator_status_cache))
        """

    def send_actuator_values(self, a, b):
        """
        This method is only called if the global parameter control is set to use_control_agent.
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

        if self.intermediate_yaml['use_control_agent']:
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
                    #self.logger.info("plc_data values: " + str(self.plc_data[plc_ip]))
                    #self.logger.info("cache values: " + str(self.cache[plc_ip]))
            self.saved_values.append(results)

            # Save scada_values.csv when needed
            if 'saving_interval' in self.intermediate_yaml and master_time != 0 and \
                    master_time % self.intermediate_yaml['saving_interval'] == 0:
                self.write_output()

            if self.intermediate_yaml['use_control_agent']:
                self.execute_control_step()

            self.set_sync(flag=1)

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

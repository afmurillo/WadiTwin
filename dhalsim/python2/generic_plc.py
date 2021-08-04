import argparse
import os.path
import sqlite3
import threading
import time
from decimal import Decimal
from pathlib import Path
import random

import yaml

from basePLC import BasePLC
from entities.attack import TimeAttack, TriggerBelowAttack, TriggerAboveAttack, TriggerBetweenAttack
from entities.control import AboveControl, BelowControl, TimeControl, SCADAControl
from py2_logger import get_logger

import threading
import thread



class Error(Exception):
    """Base class for exceptions in this module."""


class TagDoesNotExist(Error):
    """Raised when tag you are looking for does not exist"""


class InvalidControlValue(Error):
    """Raised when tag you are looking for does not exist"""


class DatabaseError(Error):
    """Raised when not being able to connect to the database"""


class GenericPLC(BasePLC):
    """
    This class represents a plc. This plc knows what it is connected to by reading the
    yaml file at intermediate_yaml_path and looking at index yaml_index in the plcs section.
    """

    DB_TRIES = 10
    """Amount of times a db query will retry on a exception"""

    DB_SLEEP_TIME = random.uniform(0.01, 0.1)
    """Amount of time a db query will wait before retrying"""

    PLC_CACHE_UPDATE_TIME = 0.5
    """ Time in seconds the SCADA server updates its cache"""

    def __init__(self, intermediate_yaml_path, yaml_index):
        self.yaml_index = yaml_index

        with intermediate_yaml_path.open() as yaml_file:
            self.intermediate_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.logger = get_logger(self.intermediate_yaml['log_level'])

        self.intermediate_plc = self.intermediate_yaml["plcs"][self.yaml_index]

        if 'sensors' not in self.intermediate_plc:
            self.intermediate_plc['sensors'] = list()

        if 'actuators' not in self.intermediate_plc:
            self.intermediate_plc['actuators'] = list()

        # Initialize connection to database
        self.initialize_db()

        self.intermediate_controls = self.intermediate_plc['controls']
        self.controls = self.create_controls(self.intermediate_controls)

        if 'attacks' in self.intermediate_plc.keys():
            self.attacks = self.create_attacks(self.intermediate_plc['attacks'])
        else:
            self.attacks = []

        # Create state from db values
        state = {
            'name': "plant",
            'path': self.intermediate_yaml['db_path']
        }

        # Create list of dependant sensors
        dependant_sensors = []
        for control in self.intermediate_controls:
            if control["type"] != "Time" and control["type"] != "SCADA":
                dependant_sensors.append(control["dependant"])

        # Create list of PLC sensors
        plc_sensors = self.intermediate_plc['sensors']

        # Create server, real tags are generated
        plc_server = {
            'address': self.intermediate_plc['local_ip'],
            'tags': self.generate_real_tags(plc_sensors,
                                            list(set(dependant_sensors) - set(plc_sensors)),
                                            self.intermediate_plc['actuators'])
        }

        # Create protocol
        plc_protocol = {
            'name': 'enip',
            'mode': 1,
            'server': plc_server
        }

        # create cache
        self.cache = {}

        self.update_cache_flag = False
        self.plcs_ready = False

        for tag in set(dependant_sensors) - set(plc_sensors):
            self.cache[tag] = Decimal(0)

        self.do_super_construction(plc_protocol, state)

    def do_super_construction(self, plc_protocol, state):
        """
        Function that performs the super constructor call to basePLC
        Introduced to better facilitate testing
        """
        super(GenericPLC, self).__init__(name=self.intermediate_plc['name'],
                                         state=state, protocol=plc_protocol)

    def initialize_db(self):
        """
        Function that initializes PLC connection to the database
        Introduced to better facilitate testing
        """
        self.conn = sqlite3.connect(self.intermediate_yaml["db_path"])
        self.cur = self.conn.cursor()

    @staticmethod
    def generate_real_tags(sensors, dependants, actuators):
        """
        Generates real tags with all sensors, dependants, and actuators
        attached to the plc.

        :param sensors: list of sensors attached to the plc
        :param dependants: list of dependant sensors (from other plcs)
        :param actuators: list of actuators controlled by the plc
        """
        real_tags = []

        for sensor_tag in sensors:
            if sensor_tag != "":
                real_tags.append((sensor_tag, 1, 'REAL'))
        for dependant_tag in dependants:
            if dependant_tag != "":
                real_tags.append((dependant_tag, 1, 'REAL'))
        for actuator_tag in actuators:
            if actuator_tag != "":
                real_tags.append((actuator_tag, 1, 'REAL'))

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

    @staticmethod
    def create_controls(controls_list):
        """
        Generates list of control objects for a plc

        :param controls_list: a list of the control dicts to be converted to Control objects
        """
        ret = []
        for control in controls_list:
            if control["type"].lower() == "above":
                control_instance = AboveControl(control["actuator"], control["action"], control["dependant"],
                                                control["value"])
                ret.append(control_instance)
            if control["type"].lower() == "below":
                control_instance = BelowControl(control["actuator"], control["action"], control["dependant"],
                                                control["value"])
                ret.append(control_instance)
            if control["type"].lower() == "time":
                control_instance = TimeControl(control["actuator"], control["action"], control["value"])
                ret.append(control_instance)
            if control["type"].lower() == "scada":
                control_instance = SCADAControl(control["actuator"], control["action"], control["value"])
                ret.append(control_instance)

        return ret

    @staticmethod
    def create_attacks(attack_list):
        """This function will create an array of DeviceAttacks

        :param attack_list: A list of attack dicts that need to be converted to DeviceAttacks
        """
        attacks = []
        for attack in attack_list:
            if attack['trigger']['type'].lower() == "time":
                attacks.append(
                    TimeAttack(attack['name'], attack['actuator'], attack['command'],
                               attack['trigger']['start'], attack['trigger']['end']))
            elif attack['trigger']['type'].lower() == "above":
                attacks.append(
                    TriggerAboveAttack(attack['name'], attack['actuator'], attack['command'],
                                       attack['trigger']['sensor'],
                                       attack['trigger']['value']))
            elif attack['trigger']['type'].lower() == "below":
                attacks.append(
                    TriggerBelowAttack(attack['name'], attack['actuator'], attack['command'],
                                       attack['trigger']['sensor'],
                                       attack['trigger']['value']))
            elif attack['trigger']['type'].lower() == "between":
                attacks.append(
                    TriggerBetweenAttack(attack['name'], attack['actuator'], attack['command'],
                                         attack['trigger']['sensor'],
                                         attack['trigger']['lower_value'],
                                         attack['trigger']['upper_value']))
        return attacks

    def pre_loop(self, sleep=0.5):
        """
        The pre loop of a PLC. In everything is setup. Like starting the sending thread through
        the :class:`~dhalsim.python2.basePLC` class.

        :param sleep:  (Default value = 0.5) The time to sleep after setting everything up
        """
        self.logger.debug(self.intermediate_plc['name'] + ' enters pre_loop')

        reader = True

        sensors = self.generate_tags(self.intermediate_plc['sensors'])
        actuators = self.generate_tags(self.intermediate_plc['actuators'])

        values = []
        for tag in sensors:
            values.append(Decimal(self.get(tag)))
        for tag in actuators:
            values.append(int(self.get(tag)))

        lock = threading.Lock()

        noise_scale = self.intermediate_yaml["noise_scale"]

        BasePLC.set_parameters(self, sensors, actuators, values, reader, lock,
                               self.intermediate_plc['local_ip'], noise_scale)
        self.startup()

        self.keep_updating_flag = True
        self.cache_update_process = None

        time.sleep(sleep)

    def get_tag(self, tag):
        """
        Get the value of a tag that is connected to this PLC or over the network.

        :param tag: The tag to get
        :type tag: str
        :return: value of that tag
        :rtype: int
        :raise: TagDoesNotExist if tag cannot be found
        """
        if tag in self.intermediate_plc["sensors"] or tag in self.intermediate_plc["actuators"]:
            return Decimal(self.get((tag, 1)))

        for cached_tag in self.cache:
            if tag == cached_tag:
                return self.cache[tag]

        self.logger.warning(
            "Cache miss in {plc} for tag {tag}".format(plc=self.intermediate_plc["name"], tag=tag))

        for i, plc_data in enumerate(self.intermediate_yaml["plcs"]):
            if i == self.yaml_index:
                continue
            if tag in plc_data["sensors"] or tag in plc_data["actuators"]:
                received = Decimal(self.receive((tag, 1), plc_data["public_ip"]))
                return received

        raise TagDoesNotExist(tag)

    def update_cache(self, lock, cache_update_time):
        """
        Update the cache of this plc by receiving all the required tags.
        When something cannot be received, the previous value is used.
        """

        while self.update_cache_flag:
            for cached_tag in self.cache:
                for i, plc_data in enumerate(self.intermediate_yaml["plcs"]):
                    if i == self.yaml_index:
                        continue
                    if cached_tag in plc_data["sensors"] or cached_tag in plc_data["actuators"]:
                        try:
                            received = Decimal(self.receive((cached_tag, 1), plc_data["public_ip"]))
                            with lock:
                                self.cache[cached_tag] = received
                        except Exception as e:
                            self.logger.info(
                                "{plc} receive {tag} from {ip} failed with exception '{e}'".format(
                                    plc=self.intermediate_plc["name"], tag=cached_tag,
                                    ip=plc_data["public_ip"], e=str(e)))
                            time.sleep(cache_update_time)
                            continue
            time.sleep(cache_update_time)

    def set_tag(self, tag, value):
        """
        Set a tag that is connected to this PLC to a value.

        :param tag: Which tag to set
        :type tag: str
        :param value: value to set the Tag to
        :raise: TagDoesNotExist if tag is not connected to this plc
        """
        if isinstance(value, basestring) and value.lower() == "closed":
            value = 0
        elif isinstance(value, basestring) and value.lower() == "open":
            value = 1
        else:
            raise InvalidControlValue(value)

        if tag in self.intermediate_plc["sensors"] or tag in self.intermediate_plc["actuators"]:
            self.set((tag, 1), value)
        else:
            raise TagDoesNotExist(tag + " cannot be set from " + self.intermediate_plc["name"])

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

    def get_sync(self):
        """
        Get the sync flag of this plc.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :return: False if physical process wants the plc to do a iteration, True if not.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        self.db_query("SELECT flag FROM sync WHERE name IS ?", (self.intermediate_plc["name"],))
        flag = bool(self.cur.fetchone()[0])
        return flag

    def set_sync(self, flag):
        """
        Set this plcs sync flag in the sync table. When this is 1, the physical process
        knows this plc finished the requested iteration.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :param flag: True for sync to 1, False for sync to 0
        :type flag: bool

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        self.db_query("UPDATE sync SET flag=? WHERE name IS ?",
                      (int(flag), self.intermediate_plc["name"],))
        self.conn.commit()

    def set_attack_flag(self, flag, attack_name):
        """
        Set a flag in the attack table. When it is 1, we know that the attack with the
        provided name is currently running. When it is 0, it is not.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :param flag: True for running to 1, False for running to 0
        :type flag: bool

        :param attack_name: The name of the attack
        :type attack_name: str

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        self.db_query("UPDATE attack SET flag=? WHERE name IS ?",
                         (int(flag), attack_name,))
        self.conn.commit()

    def stop_cache_update(self):
        self.update_cache_flag = False

    def main_loop(self, sleep=0.5, test_break=False):
        """
        The main loop of a PLC. In here all the controls will be applied.

        :param sleep:  (Default value = 0.5) Not used
        :param test_break:  (Default value = False) used for unit testing, breaks the loop after one iteration
        """
        self.logger.debug(self.intermediate_plc['name'] + ' enters main_loop')
        while True:
            while self.get_sync():
                time.sleep(self.DB_SLEEP_TIME)

            # Wait until we acquire the first sync before polling the PLCs
            if not self.plcs_ready:
                self.logger.debug("PLC starting update cache thread")
                self.plcs_ready = True
                self.update_cache_flag = True
                update_cache_lock = threading.Lock()
                thread.start_new_thread(self.update_cache, (update_cache_lock, self.PLC_CACHE_UPDATE_TIME))

            for control in self.controls:
                control.apply(self, self.intermediate_yaml['scada']['public_ip'])

            for attack in self.attacks:
                attack.apply(self)

            self.set_sync(1)

            if test_break:
                break

def is_valid_file(parser_instance, arg):
    if not os.path.exists(arg):
        parser_instance.error(arg + " does not exist")
    else:
        return arg


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start everything for a plc')
    parser.add_argument(dest="intermediate_yaml",
                        help="intermediate yaml file", metavar="FILE",
                        type=lambda x: is_valid_file(parser, x))
    parser.add_argument(dest="index", help="Index of PLC in intermediate yaml", type=int,
                        metavar="N")

    args = parser.parse_args()
    plc = GenericPLC(
        intermediate_yaml_path=Path(args.intermediate_yaml),
        yaml_index=args.index)

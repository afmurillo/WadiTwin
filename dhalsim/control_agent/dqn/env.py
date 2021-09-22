import pandas as pd
import numpy as np
import random
import time
import sqlite3
import sys
import yaml
from mushroom_rl.core.environment import Environment, MDPInfo
from mushroom_rl.utils.spaces import Discrete, Box


class Error(Exception):
    """Base class for exceptions in this module."""


class DatabaseError(Error):
    """Raised when not being able to connect to the database"""


class WaterNetworkEnvironment(Environment):
    """

    """
    #Amount of times a db query will retry on a exception
    DB_TRIES = 10

    # Amount of time a db query will wait before retrying
    DB_SLEEP_TIME = random.uniform(0.01, 0.1)

    def __init__(self, agent_config_file, db_path):
        """
        Initialize the environment of the control problem.

        :param agent_config_file: agent configuration file
        :type agent_config_file: Path

        :param db_path: control database file
        :type db_path: Path
        """
        with agent_config_file.open(mode='r') as fin:
            self.config_data = yaml.safe_load(fin)

        self.state_vars = self.config_data['env']['state_vars']
        self.action_vars = self.config_data['env']['action_vars']
        self.bounds = self.config_data['env']['bounds']

        # Used to update the pump status every a certain amount of time (e.g. every 4 hours)
        self.update_every = self.config_data['env']['update_every']

        # This is the time of the simulation in seconds (has to be split in days and daytime)
        self.time = None

        self.done = False
        self.total_updates = 0
        self.dsr = 0

        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()

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

        # Two possible values for each pump: 2 ^ n_pumps
        action_space = Discrete(2 ** len(self.action_vars))

        # Current state
        self._state = self.build_current_state(reset=True)

        # Bounds for observation space
        lows = np.array([self.bounds[key]['min'] for key in self.bounds.keys()])
        highs = np.array([self.bounds[key]['max'] for key in self.bounds.keys()])

        # Observation space
        observation_space = Box(low=lows, high=highs, shape=(len(self._state),))

        # TODO: what is horizon?
        mdp_info = MDPInfo(observation_space, action_space, gamma=0.99, horizon=1000000)
        super().__init__(mdp_info)

        print("ENVIRONMENT CREATED")

    def build_current_state(self, reset=False):
        """
        Build current state list, which can be used as input of the nn saved_models
        :param reset:
        :return:
        """
        state = []

        #for var in self.

        state = [np.float32(i) for i in state]
        return state

    def _init_what(self):
        """
        Save a ordered tuple of pk field names in self._what
        """
        query = "PRAGMA table_info(%s)" % self._table_name[0]

        try:
            self.cur.execute(query)
            table_info = self.cur.fetchall()

            # last tuple element
            primary_keys = []
            for field in table_info:
                if field[-1] > 0:
                    primary_keys.append(field)

            if not primary_keys:
                print('ERROR: Please provide at least 1 primary key. Has sqlite DB been initialized?. Aborting!')
                sys.exit(1)
            else:
                # sort by pk order
                primary_keys.sort(key=lambda x: x[5])

                what_list = []
                for pk in primary_keys:
                    what_list.append(pk[1])

                self._what = tuple(what_list)

        except sqlite3.Error as e:
            print('ERROR: Error initializing the sqlite DB. Exiting. Error: ' + str(e))
            sys.exit(1)

    def _init_set_query(self):
        """
        Prepared statement to update action_space table.
        """
        set_query = 'UPDATE %s SET %s = ? WHERE %s = ?' % (
            self._table_name[1],
            self._value,
            self._what[0])

        # for composite pk
        for pk in self._what[1:]:
            set_query += ' AND %s = ?' % (
                pk)

        self._set_query = set_query

    def _init_get_query(self):
        """
        Prepared statement to retrieve the observation space from state_space table.
        """
        get_query = 'SELECT %s FROM %s WHERE %s = ?' % (
            self._value,
            self._table_name[0],
            self._what[0])

        # for composite pk
        for pk in self._what[1:]:
            get_query += ' AND %s = ?' % (
                pk)

        self._get_query = get_query

    def db_query(self, query, parameters=None):
        """
        Execute a query on the database.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.
        This is necessary because of the limited concurrency in SQLite.

        :param query: The SQL query to execute in the db
        :type query: str

        :param parameters: The parameters to put in the query
        :type parameters: tuple

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
                print(
                    "ERROR: Failed to connect to db with exception {exc}. Trying {i} more times.".format(
                        exc=exc, i=self.DB_TRIES - i - 1))
                time.sleep(self.DB_SLEEP_TIME)
        print("ERROR: Failed to connect to db. Tried {i} times.".format(i=self.DB_TRIES))
        raise DatabaseError("Failed to get master clock from database")

    def get_sync(self):
        """
        Get the sync flag of the scada before the beginning of the control step.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :return: False if physical process wants the plc to do a iteration, True if not.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        # Get sync from control_db
        self.db_query("SELECT flag FROM sync WHERE name IS 'scada'")
        flag = bool(self.cur.fetchone()[0])
        return flag

    def set_sync(self):
        """
        Set the agent's sync flag in the sync table and removes the scada's one.
        When this is 1, the scada process knows that the agent finished the control iteration.
        On a :code:`sqlite3.OperationalError` it will retry with a max of :code:`DB_TRIES` tries.
        Before it reties, it will sleep for :code:`DB_SLEEP_TIME` seconds.

        :raise DatabaseError: When a :code:`sqlite3.OperationalError` is still raised after
           :code:`DB_TRIES` tries.
        """
        self.db_query("UPDATE sync SET flag=1 WHERE name IS 'agent'")
        self.db_query("UPDATE sync SET flag=0 WHERE name IS 'scada'")
        self.conn.commit()

    def write_db(self, node_id, value):
        """
        Set new state space values inside the action_space table.

        :param node_id: name of the considered couple (node, property)
        :type node_id: basestring

        :param value: value of the property
        :type value: float
        """
        query_args = (value, node_id)

        self.db_query(query=self._set_query, parameters=query_args)
        self.control_conn.commit()

    def read_db(self, node_id):
        """
        Read the values of the observation space and retrieve them.

        :param node_id: name of the considered couple (node, property)
        :type node_id: basestring

        :return: value of the actuator status
        """
        query_args = (node_id,)

        self.db_query(query=self._get_query, parameters=query_args)
        record = self.control_cur.fetchone()
        return record[0]

    def get_scada_ready(self):
        pass

    def reset(self, state=None):
        return self._state

    def step(self, action):
        # return self._state, reward, self.done, info
        pass

    def render(self):
        pass

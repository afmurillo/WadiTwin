import argparse
import os
import sqlite3
from pathlib import Path
from dhalsim.py3_logger import get_logger
import yaml
import pandas as pd


class DatabaseInitializer:
    def __init__(self, intermediate_yaml: Path):
        self.intermediate_yaml = intermediate_yaml
        with intermediate_yaml.open(mode='r') as file:
            self.data = yaml.safe_load(file)

        self.logger = get_logger(self.data['log_level'])
        self.db_path = Path(self.data["db_path"])
        self.db_path.touch(exist_ok=True)
        self.logger.info("Initializing database.")

    def write(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()

            cur.execute("""CREATE TABLE plant
                (
                    name  TEXT    NOT NULL,
                    pid   INTEGER NOT NULL,
                    value TEXT,
                    PRIMARY KEY (name, pid)
                );""")

            if "actuators" in self.data:
                for actuator in self.data["actuators"]:
                    initial_state = "0" if actuator["initial_state"].lower() == "closed" else "1"
                    cur.execute("INSERT INTO plant VALUES (?, 1, ?);",
                                (actuator["name"], initial_state,))

            if "plcs" in self.data:
                for plc in self.data["plcs"]:
                    for sensor in plc["sensors"]:
                        cur.execute("INSERT INTO plant VALUES (?, 1, 0);", (sensor,))

            # Creates master_time table if it does not yet exist
            cur.execute("CREATE TABLE master_time (id INTEGER PRIMARY KEY, time INTEGER)")

            # Sets master_time to 0
            cur.execute("REPLACE INTO master_time (id, time) VALUES (1, 0)")

            # Creates sync table
            cur.execute("""CREATE TABLE sync (
                name TEXT NOT NULL,
                flag INT NOT NULL,
                PRIMARY KEY (name)
            );""")

            if "plcs" in self.data:
                for plc in self.data["plcs"]:
                    cur.execute("INSERT INTO sync (name, flag) VALUES (?, 1);",
                                (plc["name"],))

            cur.execute("INSERT INTO sync (name, flag) VALUES ('scada', 1);")

            if "network_attacks" in self.data:
                for attacker in self.data["network_attacks"]:
                    cur.execute("INSERT INTO sync (name, flag) VALUES (?, 1);",
                                (attacker["name"],))

            # Creates attack table
            cur.execute("""CREATE TABLE attack (
                name TEXT NOT NULL,
                flag INT NOT NULL,
                PRIMARY KEY (name)
            );""")
            # Add device attacks to attack table
            if "plcs" in self.data:
                for plc in self.data["plcs"]:
                    if "attacks" in plc:
                        for attack in plc["attacks"]:
                            cur.execute("INSERT INTO attack (name, flag) VALUES (?, 0);",
                                        (attack["name"],))
            # Add network attacks to attack table
            if "network_attacks" in self.data:
                for network_attack in self.data["network_attacks"]:
                    cur.execute("INSERT INTO attack (name, flag) VALUES (?, 0);",
                                (network_attack["name"],))

            conn.commit()

    def drop(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS plant;")
            cur.execute("DROP TABLE IF EXISTS master_time;")
            cur.execute("DROP TABLE IF EXISTS sync;")
            cur.execute("DROP TABLE IF EXISTS attack;")
            conn.commit()

    def print(self):
        with sqlite3.connect(self.db_path) as conn:
            self.logger.debug(pd.read_sql_query("SELECT * FROM plant;", conn))
            self.logger.debug(pd.read_sql_query("SELECT * FROM master_time;", conn))
            self.logger.debug(pd.read_sql_query("SELECT * FROM sync;", conn))
            self.logger.debug(pd.read_sql_query("SELECT * FROM attack;", conn))


class ControlDatabase:
    """
    Database used to connect SCADA process with control agent process.
    """
    def __init__(self):
        """
        Create only the instance of the database
        """
        self.data = None
        self.db_path = None
        self.logger = get_logger('info')

    def init_tables(self, intermediate_yaml_path):
        """
        Reset the database with its initial values.
        """
        with intermediate_yaml_path.open(mode='r') as file:
            self.data = yaml.safe_load(file)

        self.db_path = Path(self.data["db_control_path"])
        self.db_path.touch(exist_ok=True)

        self.drop()
        self.create_table()
        self.logger.info("Initializing control agent database.")
        self.print()

    def create_table(self):
        """
        Create the database table with the field for the action variables and the space varaibles
        """
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()

            # Table of the observation space with the variables the scada sends to control agent
            cur.execute("""CREATE TABLE state_space (
                                id      TEXT      NOT NULL,
                                value   INTEGER   NOT NULL,
                                PRIMARY KEY (id)
                            );""")

            if "plcs" in self.data:
                for plc in self.data["plcs"]:
                    for sensor in plc["sensors"]:
                        cur.execute("INSERT INTO state_space VALUES (?, 0);", (sensor,))

            # Table of the action space with status of actuators sent by the control agent to the scada
            cur.execute("""CREATE TABLE action_space (
                                id      TEXT      NOT NULL,
                                value   INTEGER   NOT NULL,
                                PRIMARY KEY (id)
                            );""")

            if "actuators" in self.data:
                for actuator in self.data["actuators"]:
                    initial_state = "0" if actuator["initial_state"].lower() == "closed" else "1"
                    cur.execute("INSERT INTO action_space VALUES (?, ?);",
                                (actuator["name"], initial_state))

            # Table of simulation clock time
            cur.execute("CREATE TABLE master_time (id INTEGER PRIMARY KEY, time INTEGER);")

            # Initialization of master_time table
            cur.execute("INSERT INTO master_time VALUES (1, 0);")

            # Table of synchronization flag
            cur.execute("""CREATE TABLE sync (
                                name TEXT   NOT NULL,
                                flag INT    NOT NULL,
                                PRIMARY KEY (name)
                            );""")

            # Initialization of the sync table
            cur.execute("INSERT INTO sync VALUES ('scada', 1);")
            cur.execute("INSERT INTO sync VALUES ('agent', 0);")

            conn.commit()

    def drop(self):
        """
        Delete already existing tables if any
        """
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS state_space;")
            cur.execute("DROP TABLE IF EXISTS action_space;")
            cur.execute("DROP TABLE IF EXISTS master_time;")
            cur.execute("DROP TABLE IF EXISTS sync;")
            conn.commit()

    def print(self):
        """
        Print the tables of the control database
        """
        with sqlite3.connect(self.db_path) as conn:
            self.logger.info(pd.read_sql_query("SELECT * FROM state_space;", conn))
            self.logger.info(pd.read_sql_query("SELECT * FROM action_space;", conn))
            self.logger.info(pd.read_sql_query("SELECT * FROM master_time;", conn))
            self.logger.info(pd.read_sql_query("SELECT * FROM sync;", conn))


def is_valid_file(file_parser, arg):
    if not os.path.exists(arg):
        file_parser.error(arg + " does not exist.")
    else:
        return arg

import sys

import pandas as pd
import wntr
from antlr4 import *

from ..epynet import epynetUtils
from ..epynet.network import WaterDistributionNetwork
from dhalsim.parser.antlr.controlsLexer import controlsLexer
from dhalsim.parser.antlr.controlsParser import controlsParser


class Error(Exception):
    """Base class for exceptions in this module."""


class NoInpFileGiven(Error):
    """Raised when tag you are looking for does not exist"""


class NotEnoughInitialValues(Error):
    """Raised when there are not enough initial values in a csv"""


def value_to_status(actuator_value):
    """
    Translates int corresponding to actuator status.

    :param actuator_value: The value from the status.value of the actuator
    :type actuator_value: int
    """
    if actuator_value == 0:
        return "closed"
    else:
        return "open"


class InputParser:
    """
    Class handling the parsing of .inp input files.

    :param intermediate_yaml: The intermediate yaml file
    """

    def __init__(self, intermediate_yaml):
        """Constructor method"""
        self.data = intermediate_yaml

        for plc in self.data['plcs']:
            if 'sensors' not in plc:
                plc['sensors'] = list()

            if 'actuators' not in plc:
                plc['actuators'] = list()

        # Get the INP file path
        if 'inp_file' in self.data.keys():
            self.inp_file_path = self.data['inp_file']
        else:
            raise NoInpFileGiven()
        # Read the inp file with WNTR
        self.simulator = self.data["simulator"]

        if self.simulator == 'epynet':
            self.wn = WaterDistributionNetwork(self.inp_file_path)
        else:
            self.wn = wntr.network.WaterNetworkModel(self.inp_file_path)

        self.batch_mode = 'batch_simulations' in self.data

    def write(self):
        """
        Writes all needed inp file sections into the intermediate_yaml.
        """
        # Generate list of actuators + initial values
        self.generate_actuators_list()
        # Generate PLC controls
        self.generate_controls()
        # Generate list of times
        self.generate_times()
        # Generate initial values if batch mode is true
        if 'initial_tank_data' in self.data:
            self.generate_initial_tank_values()
        # Generate network loss values if network loss is true
        if 'network_loss_data' in self.data:
            self.generate_network_losses()
        # Generate network delay values if network delay is true
        if 'network_delay_data' in self.data:
            self.generate_network_delays()
        # Add iterations if not existing
        if "iterations" not in self.data.keys():
            iterations = int(self.data["time"][0]["duration"] / self.data["time"][1]["hydraulic_timestep"])
            if iterations <= 0:
                print(f"Error in inp file section [TIMES]: (duration: {self.data['time'][0]['duration']} / "
                      f"hydraultic timestep: {self.data['time'][1]['hydraulic_timestep']}) = {iterations}")
                sys.exit(1)
            self.data["iterations"] = iterations

        # Return the YAML object
        return self.data

    def generate_controls(self):
        """
        Generates list of controls with their types, values, actuators, and
        potentially dependant; then adds that to self.data to be written to the yaml.
        """

        if 'DQN_Control' in self.data and self.data['DQN_Control']:
            controls = []

            for plc in self.data['plcs']:
                plc['controls'] = []
                actuators = plc['actuators']

                for actuator in actuators:
                    actuator_index = 0

                    #todo: Is there a better way to do this? We need to get the initial status of the actuator
                    for i in range(len(self.data['actuators'])):
                        if self.data['actuators'][i] == actuator:
                            actuator_index = 0
                            break

                    a_control = {
                        "type": 'SCADA',
                        "value": 1,
                        "actuator": actuator,
                        "action": self.data['actuators'][actuator_index]['initial_state']
                    }

                    controls.append(a_control)
                    plc['controls'].append(a_control)

        else:
            input_file = FileStream(self.inp_file_path)
            tree = controlsParser(CommonTokenStream(controlsLexer(input_file))).controls()

            controls = []
            for i in range(0, tree.getChildCount()):
                child = tree.getChild(i)
                # Get all common control values from the control
                actuator = str(child.getChild(1))
                action = str(child.getChild(2))
                if child.getChildCount() == 8:
                    # This is an AT NODE control
                    dependant = str(child.getChild(5))
                    value = float(str(child.getChild(7)))
                    controls.append({
                        "type": str(child.getChild(6)).lower(),
                        "dependant": dependant,
                        "value": value,
                        "actuator": actuator,
                        "action": action.lower()
                    })
                if child.getChildCount() == 6:
                    # This is a TIME control
                    value = float(str(child.getChild(5)))
                    controls.append({
                        "type": "time",
                        "value": int(value),
                        "actuator": actuator,
                        "action": action.lower()
                    })

        for plc in self.data['plcs']:
            plc['controls'] = []
            actuators = plc['actuators']
            for control in controls:
                if control['actuator'] in actuators:
                    plc['controls'].append(control)

    def generate_times(self):
        """
        Generates duration and hydraulic timestep and adds to the
        data to be written to the yaml file.
        """

        # TODO Decide on the timestep (minutes or seconds?)
        if self.simulator == 'epynet':
            times = [
                {'duration': epynetUtils.get_time_parameter(self.wn, epynetUtils.get_time_param_code('EN_DURATION'))},
                {'hydraulic_timestep': epynetUtils.get_time_parameter(
                    self.wn, epynetUtils.get_time_param_code('EN_HYDSTEP'))}
            ]
        else:
            times = [
                {"duration": self.wn.options.time.duration},
                {"hydraulic_timestep": self.wn.options.time.hydraulic_timestep}
            ]
        self.data['time'] = times

    def generate_actuators_list(self):
        """
        Generates list of actuators with their initial states
        and adds to the data to be written to the yaml file.
        """

        pumps = []
        valves = []

        if self.simulator == 'epynet':
            for pump in self.wn.pumps:
                pumps.append({
                    'name': pump.uid,
                    'initial_state': 'open' if pump.initstatus else 'closed'
                })
            for valve in self.wn.valves:
                valves.append({
                    'name': valve.uid,
                    'initial_state': 'open' if valve.initstatus else 'closed'
                })
        else:
            for pump in self.wn.pumps():
                pumps.append({
                    "name": pump[0],
                    "initial_state": value_to_status(pump[1].status.value)
                })
            for valve in self.wn.valves():
                valves.append({
                    "name": valve[0],
                    "initial_state": value_to_status(valve[1].status.value)
                })
        # Append valves to pumps
        pumps.extend(valves)
        self.data['actuators'] = pumps

    def generate_initial_tank_values(self):
        """Generates all tanks with their initial values if running in batch mode"""

        initial_values = {}
        initial_tank_levels = pd.read_csv(self.data['initial_tank_data'])
        self.verify_csv_input(initial_tank_levels, 'initial_tank_data')
        # For all columns in csv
        for index in range(len(initial_tank_levels.columns)):
            name = initial_tank_levels.columns[index]
            # Insert tank value into data
            data_index = self.data["batch_index"] if self.batch_mode else 0
            initial_values[str(name)] = \
                float(initial_tank_levels.iloc[data_index, index])

        self.data['initial_tank_values'] = initial_values

    def generate_network_losses(self):
        """Generates list of routers with their network losses from the input csv"""

        network_loss = {}
        network_loss_data = pd.read_csv(self.data['network_loss_data'])
        self.verify_csv_input(network_loss_data, 'network_loss_data')
        # For all columns in csv
        for index in range(len(network_loss_data.columns)):
            name = network_loss_data.columns[index]
            # Insert loss  value into data
            data_index = self.data["batch_index"] if self.batch_mode else 0
            network_loss[str(name)] = \
                float(network_loss_data.iloc[data_index, index])

        self.data['network_loss_values'] = network_loss

    def generate_network_delays(self):
        """Generates list of routers with their network delays from the input csv"""

        network_delay = {}
        network_delay_data = pd.read_csv(self.data['network_delay_data'])
        self.verify_csv_input(network_delay_data, 'network_delay_data')
        # For all columns in csv
        for index in range(len(network_delay_data.columns)):
            name = network_delay_data.columns[index]
            # Insert tank : value into data
            data_index = self.data["batch_index"] if self.batch_mode else 0
            network_delay[str(name)] = \
                str(network_delay_data.iloc[data_index, index]) + "ms"

        self.data['network_delay_values'] = network_delay

    def verify_csv_input(self, dataframe, data):
        """
        Verifies the csv files have the proper number of rows for a simulation

        :param dataframe: pandas dataframe containing csv data
        :param data: name of data that is being verified
        """
        num_rows = len(dataframe)
        if self.batch_mode:
            if num_rows < self.data['batch_simulations']:
                raise NotEnoughInitialValues("Provided csv has fewer rows than number of batch simulations: " + data)
        else:
            if num_rows <= 0:
                raise NotEnoughInitialValues("Provided csv has no data: " + data)

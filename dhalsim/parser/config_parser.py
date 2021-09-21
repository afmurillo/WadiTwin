import os
import sys
import tempfile
from datetime import datetime

from pathlib import Path

import yaml
from yamlinclude import YamlIncludeConstructor
from schema import Schema, Or, And, Use, Optional, SchemaError, Regex

from dhalsim.parser.input_parser import InputParser


class Error(Exception):
    """Base class for exceptions in this module."""


class EmptyConfigError(Error):
    """Raised when the configuration file is empty"""


class MissingValueError(Error):
    """Raised when there is a value missing in a configuration file"""


class InvalidValueError(Error):
    """Raised when there is a invalid value in a configuration file"""


class DuplicateValueError(Error):
    """Raised when there is a duplicate plc value in the plcs section"""


class NoSuchPlc(Error):
    """Raised when an attack targets a PLC that does not exist"""


class NoSuchTag(Error):
    """Raised when an attack targets a tag the target PLC does not have"""


class NetworkAttackError(Error):
    """Used to raise errors about network attack"""


class TooManyNodes(Error):
    """Raised when there will be too many nodes in the network"""


class SchemaParser:
    """
    Class which handles all schema logic.
    """
    string_pattern = Regex(r'^[a-zA-Z0-9_]+$',
                           error="Error in string: '{}', Can only have a-z, A-Z, 0-9, and _")

    attack_pattern = Regex(r'^[a-zA-Z0-9_<>+-.]+$',
                           error="Error in attack name: '{}', "
                                 "Can only have a-z, A-Z, 0-9, and symbols: _ < > + - . (no whitespaces)")

    trigger = Schema(
        Or(
            {
                'type': And(
                    str,
                    Use(str.lower),
                    'time'
                ),
                'start': And(
                    int,
                    Schema(lambda i: i >= 0, error="'start' must be positive."),
                ),
                'end': And(
                    int,
                    Schema(lambda i: i >= 0, error="'end' must be positive."),
                ),
            },
            {
                'type': And(
                    str,
                    Use(str.lower),
                    Or('below', 'above')
                ),
                'sensor': And(
                    str,
                    string_pattern,
                ),
                'value': And(
                    Or(float, And(int, Use(float))),
                ),
            },
            {
                'type': And(
                    str,
                    Use(str.lower),
                    'between'
                ),
                'sensor': And(
                    str,
                    string_pattern,
                ),
                'lower_value': And(
                    Or(float, And(int, Use(float))),
                ),
                'upper_value': And(
                    Or(float, And(int, Use(float))),
                ),
            },
        )
    )

    device_attacks = Schema({
        'name': And(
            str,
            attack_pattern,
        ),
        'trigger': trigger,
        'actuator': And(
            str,
            string_pattern,
        ),
        'command': And(
            str,
            Use(str.lower),
            Or('open', 'closed')
        )
    })

    network_attacks = Schema(
        Or(
            {
                'type': And(
                    str,
                    Use(str.lower),
                    'naive_mitm',
                ),
                'name': And(
                    str,
                    string_pattern,
                    Schema(lambda name: 1 <= len(name) <= 10,
                           error="Length of name must be between 1 and 10, '{}' has invalid length")
                ),
                'trigger': trigger,
                Or('value', 'offset', only_one=True,
                   error="'tags' should have either a 'value' or 'offset' attribute."): Or(float, And(int, Use(float))),
                'target': And(
                    str,
                    string_pattern
                )
            },
            {
                'type': And(
                    str,
                    Use(str.lower),
                    'mitm',
                ),
                'name': And(
                    str,
                    string_pattern,
                    Schema(lambda name: 1 <= len(name) <= 10,
                           error="Length of name must be between 1 and 10, '{}' has invalid length")
                ),
                'trigger': trigger,
                'target': And(
                    str,
                    string_pattern
                ),
                'tags': [{
                    'tag': And(
                        str,
                        string_pattern,
                    ),
                    Or('value', 'offset', only_one=True,
                       error="'tags' should have either a 'value' or 'offset' attribute."): Or(float, And(int, Use(float))),
                }]
            }
        )
    )

    @staticmethod
    def path_schema(data: dict, config_path: Path) -> dict:
        """
        For all the values that need to be a path, this function converts them to absolute paths,
        checks if they exists, and checks the suffix if applicable.

        :param data: data from the config file
        :type data: dict
        :param config_path: That to the config file
        :type config_path:

        :return: the config data, but with existing absolute path objects
        :rtype: dict
        """
        return Schema(
            And(
                {
                    'inp_file': And(
                        Use(Path),
                        Use(lambda p: config_path.absolute().parent / p),
                        Schema(lambda l: Path.is_file, error="'inp_file' could not be found."),
                        Schema(lambda f: f.suffix == '.inp',
                               error="Suffix of 'inp_file' should be .inp.")),
                    Optional('output_path', default=config_path.absolute().parent / 'output'): And(
                        Use(str, error="'output_path' should be a string."),
                        Use(Path),
                        Use(lambda p: config_path.absolute().parent / p),
                    ),
                    Optional('initial_tank_data'): And(
                        Use(Path),
                        Use(lambda p: config_path.absolute().parent / p),
                        Schema(lambda l: Path.is_file, error="'initial_tank_data' could not be found."),
                        Schema(lambda f: f.suffix == '.csv',
                               error="Suffix of initial_tank_data should be .csv")),
                    Optional('demand_patterns'): And(
                        Use(Path),
                        Use(lambda p: config_path.absolute().parent / p),
                        Schema(lambda l: Path.exists, error="'demand_patterns' path does not exist."),
                        Or(
                            Path.is_dir,
                            Schema(lambda f: f.suffix == '.csv',
                                   error="Suffix of demand_patterns should be .csv"))),
                    Optional('network_loss_data'): And(
                        Use(Path),
                        Use(lambda p: config_path.absolute().parent / p),
                        Schema(lambda l: Path.is_file, error="'network_loss_data' could not be found."),
                        Schema(lambda f: f.suffix == '.csv',
                               error="Suffix of network_loss_data should be .csv")),
                    Optional('network_delay_data'): And(
                        Use(Path),
                        Use(lambda p: config_path.absolute().parent / p),
                        Schema(lambda l: Path.is_file, error="'network_delay_data' could not be found."),
                        Schema(lambda f: f.suffix == '.csv',
                               error="Suffix of network_delay_data should be .csv")),
                    Optional('agent_config_file'): And(
                        Use(Path),
                        Use(lambda p: config_path.absolute().parent.parent.parent / 'dhalsim' / 'control_agent' / p),
                        Schema(lambda l: Path.is_file, error="'agent_config_file' could not be found."),
                        Schema(lambda f: f.suffix == '.yaml',
                               error="Suffix of agent_config_file should be .yaml")),
                    str: object
                }
            )
        ).validate(data)

    @staticmethod
    def validate_schema(data: dict) -> dict:
        """
        Apply a schema to the data. This schema make sure that every reuired parameter is given.
        It also fills in default values for missing parameters.
        It will test for types of parameters as well.
        Besides that, it converts some strings to lower case, like those of :code:'log_level'.

        :param data: data from the config file
        :type data: dict

        :return: A verified version of the data of the config file
        :rtype: dict
        """
        plc_schema = Schema([{
            'name': And(
                str,
                SchemaParser.string_pattern,
                Schema(lambda name: 1 <= len(name) <= 10,
                       error="Length of name must be between 1 and 10, '{}' has invalid length")
            ),
            Optional('sensors'): [And(
                str,
                SchemaParser.string_pattern
            )],
            Optional('actuators'): [And(
                str,
                SchemaParser.string_pattern
            )]
        }])

        config_schema = Schema({
            'plcs': plc_schema,
            'inp_file': Path,
            Optional('network_topology_type', default='simple'): And(
                str,
                Use(str.lower),
                Or('complex', 'simple')),
            'output_path': Path,
            Optional('iterations'): And(
                int,
                Schema(lambda i: i > 0, error="'iterations' must be positive.")),
            Optional('mininet_cli', default=False): bool,
            Optional('log_level', default='info'): And(
                str,
                Use(str.lower),
                Or('debug', 'info', 'warning', 'error', 'critical', error="'log_level' should be "
                                                                          "one of the following: "
                                                                          "'debug', 'info', 'warning', "
                                                                          "'error' or 'critical'.")),
            Optional('demand', default='pdd'): And(
                str,
                Use(str.lower),
                Or('pdd', 'dd'), error="'demand' should be one of the following: 'pdd' or 'dd'."),
            Optional('noise_scale', default=0.0): And(
                float,
                Schema(lambda i: i >= 0, error="'noise_scale' must be positive.")),
            Optional('attacks'): {
                Optional('device_attacks'): [SchemaParser.device_attacks],
                Optional('network_attacks'): [SchemaParser.network_attacks],
            },
            Optional('batch_simulations'): And(
                int,
                Schema(lambda i: i > 0, error="'batch_simulations' must be positive.")),
            Optional('saving_interval'): And(
                int,
                Schema(lambda i: i > 0, error="'saving_interval' must be positive.")),
            Optional('initial_tank_data'): Path,
            Optional('demand_patterns'): Path,
            Optional('network_loss_data'): Path,
            Optional('network_delay_data'): Path,
            Optional('simulator', default='wntr'): And(
                str,
                Use(str.lower),
                Or('wntr', 'epynet')),
            Optional('use_control_agent', default=False): bool,
            Optional('agent_config_file'): Path,
        })

        return config_schema.validate(data)


class ConfigParser:
    """
    Class handling the parsing of the input config data.

    :param config_path: The path to the config file of the experiment in yaml format
    :type config_path: Path
    """

    def __init__(self, config_path: Path):
        self.batch_index = None
        self.yaml_path = None
        self.db_path = None
        self.control_problem_directory = None

        self.config_path = config_path.absolute()

        YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader,
                                                   base_dir=config_path.absolute().parent)

        try:
            self.data = self.apply_schema(self.config_path)
        except SchemaError as exc:
            sys.exit(exc.code)

        try:
            self.do_checks(self.data)
        except Error as exc:
            sys.exit(exc)

        self.batch_mode = 'batch_simulations' in self.data
        if self.batch_mode:
            self.batch_simulations = self.data['batch_simulations']

        if self.data['use_control_agent']:
            self.control_problem_directory = self.control_problem_path

    @staticmethod
    def do_checks(data: dict):
        """
        Perform various checks on the data provided

        :param data: The data to check
        """
        ConfigParser.not_too_many_nodes(data)

    @staticmethod
    def not_too_many_nodes(data: dict):
        """
        Check if there are not more then 250 plcs and network attacks.
        This would cause trouble with assigning IP and MAC addresses.

        :param data: the data to check on
        :raise TooManyNodes: When there are more then 250 nodes in the network
        """
        raise_message = "There are too many nodes in the network. Only 250 nodes are supported."

        if 'plcs' in data:
            n_plcs = len(data["plcs"])
            if n_plcs > 250:
                raise TooManyNodes(raise_message)
            if 'attacks' in data and 'network_attacks' in data['attacks'] and \
                    n_plcs + len(data['attacks']['network_attacks']) > 250:
                raise TooManyNodes(raise_message)
        elif 'attacks' in data and 'network_attacks' in data['attacks'] and \
                len(data['attacks']['network_attacks']) > 250:
            raise TooManyNodes(raise_message)

    @staticmethod
    def apply_schema(config_path: Path) -> dict:
        """
        Load the yaml data from the config file, and apply the schema.

        :param config_path: The to the config file
        :type config_path: Path

        :return: A verified version of the data of the config file
        :rtype: dict
        """
        data = ConfigParser.load_yaml(config_path)
        data = SchemaParser.path_schema(data, config_path)
        return SchemaParser.validate_schema(data)

    @staticmethod
    def load_yaml(path: Path) -> dict:
        """
        Uses :code:'pyyaml' and :code'pyyaml-include' to read in a yaml file.
        This means you can use '!include' to include yaml files in other yaml files.

        :param path: path to the yaml file to be loaded.
        :type path: Path
        :return: a dict representing the yaml file
        :rtype: dict
        """
        try:
            with path.open(mode='r') as file:
                data = yaml.load(file, Loader=yaml.FullLoader)
            return data
        except FileNotFoundError as exc:
            sys.exit(f"File not found: {exc.filename}")

    @property
    def output_path(self):
        """
        Property for the path to the output folder.
        ''output'' by default.

        :return: absolute path to the output folder
        :rtype: Path
        """
        path = self.data['output_path']
        if self.batch_mode:
            path /= 'batch_' + str(self.batch_index)
        return path

    @property
    def control_problem_path(self):
        """
        Generates the temporary directory for the control agent database
        """
        # Create temp directory and intermediate yaml files in /tmp/
        temp_directory = tempfile.mkdtemp(prefix='dhalsimRL_')
        # Change read permissions in tempdir
        os.chmod(temp_directory, 0o775)
        return temp_directory


    @property
    def demand_patterns(self):
        """
        Function that returns path to demand pattern csv

        :return: absolute path to the demand pattern csv
        :rtype: Path
        """
        path = self.data.get('demand_patterns')

        # If running in batch mode, then have to use batch index in name
        if self.batch_mode:
            path /= str(self.batch_index) + '.csv'

        if not path.is_file():
            raise FileNotFoundError(str(path) + " is not a file.")
        return path

    def generate_device_attacks(self, yaml_data):
        """
        This function will add device attacks to the appropriate PLCs in the intermediate yaml

        :param yaml_data: The YAML data without the device attacks
        """
        if 'attacks' in self.data and 'device_attacks' in self.data['attacks']:
            for device_attack in self.data['attacks']['device_attacks']:
                for plc in yaml_data['plcs']:
                    if device_attack['actuator'] in plc['actuators']:
                        if 'attacks' not in plc.keys():
                            plc['attacks'] = []
                        plc['attacks'].append(device_attack)
                        break
        return yaml_data

    def generate_network_attacks(self):
        """
        This function will add device attacks to the appropriate PLCs in the intermediate yaml

        :param network_attacks: The YAML data of the network attacks
        """
        if 'attacks' in self.data and 'network_attacks' in self.data['attacks']:
            network_attacks = self.data['attacks']["network_attacks"]
            for network_attack in network_attacks:
                # Check existence and validity of target PLC
                target = network_attack['target']
                target_plc = None
                for plc in self.data.get("plcs"):
                    if plc['name'] == target:
                        target_plc = plc
                        break
                if not target_plc:
                    raise NoSuchPlc("PLC {plc} does not exists".format(plc=target))

                if network_attack['type'] == 'mitm':
                    # Check existence of tags on target PLC
                    tags = []
                    for tag in network_attack['tags']:
                        tags.append(tag['tag'])
                    if not set(tags).issubset(set(target_plc['actuators'] + target_plc['sensors'])):
                        raise NoSuchTag(
                            f"PLC {target_plc['name']} does not have all the tags specified.")

            return network_attacks
        return []

    def generate_temporary_dirs(self):
        """Generates the temporary directory and yaml/db paths"""
        # Create temp directory and intermediate yaml files in /tmp/
        if self.data['use_control_agent']:
            temp_directory = tempfile.mkdtemp(prefix='dhalsim_', dir=Path(self.control_problem_directory))
        else:
            temp_directory = tempfile.mkdtemp(prefix='dhalsim_')
        # Change read permissions in tempdir
        os.chmod(temp_directory, 0o775)
        print("temp_folder: ", temp_directory)

        self.yaml_path = Path(temp_directory + '/intermediate.yaml')
        self.db_path = temp_directory + '/dhalsim.sqlite'

    def generate_intermediate_yaml(self):
        """Writes the intermediate.yaml file to include all options specified in the config, the plc's and their
        data, and all valves/pumps/tanks etc.

        :return: the path to the yaml file
        :rtype: Path
        """
        self.generate_temporary_dirs()

        yaml_data = {}
        yaml_data['config_path'] = str(self.config_path)

        # Begin with PLC data specified in plcs section
        yaml_data['plcs'] = self.data['plcs']
        # Add path and database information
        yaml_data['inp_file'] = str(self.data['inp_file'])
        yaml_data['output_path'] = str(self.output_path)
        yaml_data['db_path'] = self.db_path
        yaml_data['network_topology_type'] = self.data['network_topology_type']

        # Simulator to be used, it can be EPANET WNTR or EPANET epynet
        yaml_data['simulator'] = self.data['simulator']
        yaml_data['use_control_agent'] = self.data['use_control_agent']

        # DQN controller is Reinforcement Q learning running at an SCADA server it modifies the behaviour of PLCs and
        # SCADA
        if self.data['use_control_agent']:
            yaml_data['db_control_path'] = self.control_problem_directory + '/db_control.sqlite'

        # Add batch mode parameters
        if self.batch_mode:
            yaml_data['batch_index'] = self.batch_index
            yaml_data['batch_simulations'] = self.data['batch_simulations']
        # Initial physical values
        if 'initial_tank_data' in self.data:
            yaml_data['initial_tank_data'] = str(self.data['initial_tank_data'])
        if 'demand_patterns' in self.data:
            yaml_data['demand_patterns_data'] = str(self.demand_patterns)
        # Add network loss parameters
        if 'network_loss_data' in self.data:
            yaml_data['network_loss_data'] = str(self.data['network_loss_data'])
        # Add network delay parameters
        if 'network_delay_data' in self.data:
            yaml_data['network_delay_data'] = str(self.data['network_delay_data'])
        # Mininet cli parameter
        yaml_data['mininet_cli'] = self.data['mininet_cli']
        # Write intermittent saving interval to intermediate yaml
        if 'saving_interval' in self.data:
            yaml_data['saving_interval'] = self.data['saving_interval']
        # Write gaussian noise scale value to intermediate yaml
        if 'noise_scale' in self.data:
            yaml_data['noise_scale'] = self.data['noise_scale']

        # Demand
        yaml_data['demand'] = self.data['demand']

        # Note: if iterations not present then default value will be written in InputParser
        if 'iterations' in self.data:
            yaml_data['iterations'] = self.data['iterations']

        # Log level
        yaml_data['log_level'] = self.data['log_level']

        yaml_data['start_time'] = datetime.now()
        # Write values from IMP file into yaml file (controls, tanks/valves/initial values, etc.)
        yaml_data = InputParser(yaml_data).write()

        # Parse the device attacks from the config file
        yaml_data = self.generate_device_attacks(yaml_data)
        yaml_data["network_attacks"] = self.generate_network_attacks()

        # Write data to yaml file
        with self.yaml_path.open(mode='w') as intermediate_yaml:
            yaml.safe_dump(yaml_data, intermediate_yaml)

        return self.yaml_path

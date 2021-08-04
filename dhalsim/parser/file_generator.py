import datetime
import os
from pathlib import Path
from shutil import copy

import pkg_resources
from wntr.network import WaterNetworkModel
import yaml

time_format = "%Y-%m-%d %H:%M:%S"


class BatchReadmeGenerator:
    """
    Class which deals with generating a readme for each batch.
    :param intermediate_yaml_path: contains the path to intermediate yaml
    :param start_time: is the start time of batch
    :param end_time: is the end time of batch
    :param wn: is WNTR instance
    :param master_time: is current iteration
    :param step: hydraulic timestep of simulation
    """

    def __init__(self, intermediate_yaml_path: Path, readme_path: Path,
                 start_time: datetime.datetime, end_time: datetime.datetime,
                 wn: WaterNetworkModel, master_time: int, step):

        with intermediate_yaml_path.open() as yaml_file:
            self.intermediate_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.readme_path = readme_path
        self.start_time = start_time
        self.end_time = end_time
        self.wn = wn
        self.master_time = master_time
        self.hydraulic_timestep = step

    def write_batch(self):
        """Creates a small readme for each batch."""
        with open(str(self.readme_path), 'w') as readme:
            readme.write(self.get_batch_information())

            # Batch specific values.
            readme.write(self.get_initial_tank_values())
            readme.write(self.get_network_loss_value())
            readme.write(self.get_network_delay_values())

            # Information about this batch.
            readme.write(self.get_time_information())
            readme.write("\n\nFor more information with regard to this experiment, consult "
                         "```configuration/general_readme.md``` in the root of the output "
                         "folder.")

    def get_batch_information(self) -> str:
        """Gets general information about this specific batch."""
        ret_str = ("# Auto-generated README of {file} for batch {no}"
                   .format(file=os.path.basename(str(self.intermediate_yaml['inp_file']))[:-4],
                           no=self.intermediate_yaml['batch_index'] + 1))
        return ret_str + ("\n\nThis is batch {x} out of {y}."
                          .format(x=self.intermediate_yaml['batch_index'] + 1,
                                  y=self.intermediate_yaml['batch_simulations']))

    def get_initial_tank_values(self) -> str:
        """Gets the initial tank values of this batch if they exist."""
        if 'initial_tank_values' in self.intermediate_yaml:
            return "\n\n## Initial tank data\n\n{data}" \
                .format(data=str(self.intermediate_yaml['initial_tank_values']))
        else:
            return ""

    def get_network_loss_value(self) -> str:
        """Gets the network loss values of this batch if they exist."""
        if 'network_loss_values' in self.intermediate_yaml:
            return "\n\n## Network loss values\n\n{data}" \
                .format(data=str(self.intermediate_yaml['network_loss_values']))
        else:
            return ""

    def get_network_delay_values(self) -> str:
        """Gets the network delay values of this batch if they exist."""
        if 'network_delay_values' in self.intermediate_yaml:
            return "\n\n## Network delay values\n\n{data}" \
                .format(data=str(self.intermediate_yaml['network_delay_values']))
        else:
            return ""

    def get_time_information(self) -> str:
        """Gets information w.r.t. time of this batch."""
        ret_str = "\n\n## Information about this batch"
        #ret_str += "\n\nRan for {x} out of {y} iterations with hydraulic timestep {step}." \
        #    .format(x=str(self.master_time), y=str(self.intermediate_yaml['iterations']),
        #            step=str(self.wn.options.time.hydraulic_timestep))

        ret_str += "\n\nRan for {x} out of {y} iterations with hydraulic timestep {step}." \
            .format(x=str(self.master_time), y=str(self.intermediate_yaml['iterations']),
                    step=str(self.hydraulic_timestep))


        ret_str += ("\n\nStarted at {start} and finished at {end}."
                    .format(start=str(self.start_time.strftime(time_format)),
                            end=str(self.end_time.strftime(time_format))))
        return ret_str + ("\n\nThe duration of this batch was {time}."
                          .format(time=str(self.end_time - self.start_time)))


class InputFilesCopier:
    """
    Copies all input files.
    :param config_file: contains the location of the config file
    """

    def __init__(self, config_file: Path, intermediate_yaml_path: Path):
        self.config_file = config_file

        with self.config_file.open(mode='r') as conf:
            self.config = yaml.load(conf, Loader=yaml.FullLoader)

        with intermediate_yaml_path.open() as yaml_file:
            self.intermediate_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

        if 'batch_simulations' in self.intermediate_yaml:
            self.configuration_folder = Path(self.intermediate_yaml['output_path']).parent\
                                        / 'configuration'
        else:
            self.configuration_folder = Path(self.intermediate_yaml['output_path']) / 'configuration'


    def copy_input_files(self):
        """Copies all input files, mandatory and optional ones included."""
        os.makedirs(str(self.configuration_folder), exist_ok=True)

        # Copy mandatory files.
        with open(str(self.configuration_folder / 'config.yaml'), 'w') as config_file:
            yaml.dump(self.config, config_file)

        copy(self.config_file.parent / self.config['inp_file'],
             self.configuration_folder / 'map.inp')

        # Copy optional csv files.
        if 'initial_tank_data' in self.config:
            copy(self.config_file.parent / self.config['initial_tank_data'],
                 self.configuration_folder / 'initial_tank_data.csv')

        self.copy_demand_patterns()

        if 'network_loss_data' in self.config:
            copy(self.config_file.parent / self.config['network_loss_data'],
                 self.configuration_folder / 'network_loss_data.csv')

        if 'network_delay_data' in self.config:
            copy(self.config_file.parent / self.config['network_delay_data'],
                 self.configuration_folder / 'network_delay_data.csv')

    def copy_demand_patterns(self):
        if 'demand_patterns' in self.config:
            if 'batch_simulations' in self.config:
                os.makedirs(self.configuration_folder / 'demand_patterns', exist_ok=True)
                for batch in range(self.config['batch_simulations']):
                    copy(self.config_file.parent / self.config['demand_patterns'] / (str(batch) + ".csv"),
                         self.configuration_folder / 'demand_patterns' / (str(batch) + ".csv"))
            else:
                copy(self.config_file.parent / self.config['demand_patterns'],
                     self.configuration_folder / 'demand_patterns.csv')


class GeneralReadmeGenerator:
    """
    Class which deals with generating a readme.
    :param intermediate_yaml_path: contains the path to intermediate yaml
    :param start_time: starting time of experiment
    :param end_time: ending time of experiment
    :param batch: bool whether this was batch mode
    :param master_time: current master time
    :param wn: instance of WaterNetworkModel
    :param forced_path: optional specifier of Path to force usage
    :param step: hydraulic timestep of the simulation
    """

    def __init__(self, intermediate_yaml_path: Path, start_time: datetime.datetime,
                 end_time: datetime.datetime, batch: bool, master_time: int, wn: WaterNetworkModel, step):

        with intermediate_yaml_path.open() as yaml_file:
            self.intermediate_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)

        self.start_time = start_time
        self.end_time = end_time
        self.batch = batch
        self.master_time = master_time
        self.wn = wn
        self.readme_path = self.get_readme_path()
        self.version = pkg_resources.require('dhalsim')[0].version
        self.hydraulic_timestep = step

    def get_value(self, parameter: str) -> str:
        """
        Gets the value of a required parameter.
        :param parameter: to find the value of
        :return: human readable string
        """
        return "\n\n" + parameter + ": " + str(self.intermediate_yaml[parameter])

    def get_optional(self, parameter: str) -> str:
        """
        Gets the value of an optional parameter.
        :param parameter: to find the value of
        :return: human readable string
        """
        if parameter in self.intermediate_yaml:
            return self.get_value(parameter)
        else:
            return "\n\n" + parameter + ": None"

    def checkbox(self, parameter: str) -> str:
        """
        Returns a string with a checkbox, checked if parameter is used, otherwise unchecked.
        :param parameter: parameter to evaluate
        :return: complete string with checkbox in it
        """
        if parameter in self.intermediate_yaml and len(self.intermediate_yaml[parameter]) > 0:
            case_checkbox = "[x]"
        else:
            case_checkbox = "[ ]"

        return "\n\n- {checkbox} {para}".format(checkbox=case_checkbox, para=parameter)

    def write_readme(self):
        """
        Writes a readme about the current experiment.

        """
        with open(self.readme_path, 'w') as readme:
            readme.write("# Auto-generated README of {file}"
                         .format(file=os.path.basename(str(self.intermediate_yaml['inp_file']))[:-4]))

            readme.write(self.get_input_files())
            readme.write(self.get_optional_data_parameters())
            readme.write(get_mininet_links())

            readme.write(self.get_standalone_parameter_information())

            readme.write(self.get_versioning())
            readme.write(self.get_standalone_iteration_information())
            readme.write(self.get_time_information())
            
    def get_readme_path(self) -> str:
        """Gets the path of the readme, bearing in mind batch mode and possibility of forced
        output path using parameter forced_path."""
        if 'batch_simulations' in self.intermediate_yaml:
            configuration_folder = Path(self.intermediate_yaml['config_path']).parent \
                                   / Path(self.intermediate_yaml['output_path']).parent \
                                   / 'configuration'
        else:
            configuration_folder = Path(self.intermediate_yaml['config_path']).parent \
                                   / self.intermediate_yaml['output_path'] / 'configuration'

        readme_path = str(configuration_folder / 'general_readme.md')

        # Create directories in output folder
        os.makedirs(str(configuration_folder), exist_ok=True)

        return readme_path
            
    def get_input_files(self) -> str:
        """Get a string with information on the location of the input files."""
        ret_str = "\n\n## Input files"
        input_string = "\n\nInput files have been copied to ```{output}```. In case" \
                       " any extra files were used, these files will be copied to the" \
                       " /output/configuration folder as well."

        # We want to write this general readme to the root directory of the original output folder.
        if 'batch_simulations' in self.intermediate_yaml:
            ret_str += input_string.format(
                output=str(Path(self.intermediate_yaml['output_path']).parent))
        else:
            ret_str += input_string.format(output=self.intermediate_yaml['output_path'])
            
        return ret_str
    
    def get_configuration_parameters(self) -> str:
        """Get configuration parameters."""
        ret_str = "\n\n## Configuration parameters"
        ret_str += self.get_value('iterations')
        ret_str += self.get_value('network_topology_type')
        ret_str += self.get_value('mininet_cli')
        ret_str += self.get_value('log_level')
        ret_str += self.get_value('demand')
        ret_str += self.get_value('simulator')
        ret_str += self.get_value('DQN_Control')
        return ret_str + self.get_optional('batch_simulations')
    
    def get_optional_data_parameters(self) -> str:
        """Get optional data parameters."""
        ret_str = "\n\n## Initial conditions"
        ret_str += self.checkbox('initial_tank_data')
        ret_str += self.checkbox('demand_patterns')
        ret_str += self.checkbox('network_loss_data')
        ret_str += self.checkbox('network_delay_data')
        return ret_str + self.checkbox('network_attacks')

    def get_standalone_parameter_information(self) -> str:
        """If not batch mode it will print some extra information about parameters, which is
        normally present in the batch readme's."""
        ret_str = ""

        if not self.batch:
            if 'initial_tank_values' in self.intermediate_yaml\
                    and len(self.intermediate_yaml['initial_tank_values']) > 0:
                ret_str += "\n\n## Initial tank values\n\n{data}" \
                    .format(data=str(self.intermediate_yaml['initial_tank_values']))
            if 'network_loss_values' in self.intermediate_yaml\
                    and len(self.intermediate_yaml['network_loss_values']) > 0:
                ret_str += "\n\n## Network loss values\n\n{data}" \
                    .format(data=str(self.intermediate_yaml['network_loss_values']))
            if 'network_delay_values' in self.intermediate_yaml\
                    and len(self.intermediate_yaml['network_delay_values']) > 0:
                ret_str += "\n\n## Network delay values\n\n{data}" \
                    .format(data=str(self.intermediate_yaml['network_delay_values']))

        return ret_str

    def get_versioning(self) -> str:
        """About this experiment and DHALSIM version."""
        return ("\n\n## About this experiment\n\nRan with DHALSIM v{version}."
                .format(version=str(self.version)))

    def get_standalone_iteration_information(self) -> str:
        """If not batch mode it will print some extra information about the current simulation,
        which is normally present in the batch readme's."""
        ret_str = ""

        if not self.batch:
            #ret_str += ("\n\nRan for {x} out of {y} iterations with hydraulic timestep {step}."
            #            .format(x=str(self.master_time),
            #                    y=str(self.intermediate_yaml['iterations']),
            #                    step=str(self.wn.options.time.hydraulic_timestep)))

            ret_str += ("\n\nRan for {x} out of {y} iterations with hydraulic timestep {step}."
                        .format(x=str(self.master_time),
                                y=str(self.intermediate_yaml['iterations']),
                                step=str(self.hydraulic_timestep)))


        return ret_str

    def get_time_information(self) -> str:
        return "\n\nStarted at {start} and finished at {end}.\n\nThe duration of this simulation" \
               " was {time}.".format(start=str(self.start_time.strftime(time_format)),
                                     end=str(self.end_time.strftime(time_format)),
                                     time=str(self.end_time - self.start_time))


def get_mininet_links() -> str:
    """Gets a string which informs reader about mininet links."""
    return "\n\n## Mininet links\n\nMininet links can be found in the file mininet_links.md " \
           "in this configuration folder."

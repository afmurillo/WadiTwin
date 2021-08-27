import argparse
import os.path
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml

from dhalsim.init_database import DatabaseInitializer
from dhalsim.parser.config_parser import ConfigParser
from dhalsim.parser.file_generator import InputFilesCopier


def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error(arg + " does not exist.")
    else:
        return arg


class Runner():
    def __init__(self, config_file, output_folder):
        self.config_file = config_file
        self.output_folder = output_folder

        signal.signal(signal.SIGINT, self.sigint_handler)
        signal.signal(signal.SIGTERM, self.sigint_handler)

        self.automatic_run = None
        self.control_agent = None

    def sigint_handler(self, sig, frame):
        os.kill(self.automatic_run.pid, signal.SIGTERM)
        if self.control_agent:
            os.kill(self.control_agent.pid, signal.SIGTERM)
        time.sleep(0.3)
        sys.exit(0)

    def run(self):
        config_parser = ConfigParser(self.config_file)

        # Start the generic control agent process, feeding it with its configuration yaml file
        if config_parser.data['use_control_agent']:
            control_agent_path = Path(__file__).parent.absolute() / "control_agent" / "generic_agent.py"
            agent_config_path = Path(__file__).parent.absolute() / "control_agent" / "agent_config.yaml"
            self.control_agent = subprocess.Popen(["python3", str(control_agent_path), str(agent_config_path)])
            # self.control_agent.wait()

        if config_parser.batch_mode:
            # If in batch mode, generate all intermediate yamls and simulate one by one
            yaml_paths = []
            for batch_index in range(config_parser.batch_simulations):
                config_parser.batch_index = batch_index
                yaml_paths.append(config_parser.generate_intermediate_yaml())
            for yaml_path in yaml_paths:
                self.run_simulation(yaml_path)
        else:
            # Else generate the one we need and run the simulation
            intermediate_yaml_path = config_parser.generate_intermediate_yaml()
            self.run_simulation(intermediate_yaml_path)

        # Killing the control agent process
        self.control_agent.send_signal(signal.SIGINT)
        self.control_agent.wait()
        if self.control_agent.poll() is None:
            self.control_agent.terminate()
        if self.control_agent.poll() is None:
            self.control_agent.kill()

    def run_simulation(self, intermediate_yaml_path):

        subprocess.run(["sudo", "pkill - f - u", "root", "python -m cpppo.server.enip"])
        subprocess.run(["sudo", "mn", "-c"])

        InputFilesCopier(self.config_file, intermediate_yaml_path).copy_input_files()

        db_initializer = DatabaseInitializer(intermediate_yaml_path)
        db_initializer.drop()
        db_initializer.write()
        db_initializer.print()

        automatic_run_path = Path(__file__).parent.absolute() / "python2" / "automatic_run.py"
        self.automatic_run = subprocess.Popen(
            ["python2", str(automatic_run_path), str(intermediate_yaml_path)])
        self.automatic_run.wait()


def main():
    parser = argparse.ArgumentParser(description='Executes DHALSIM based on a config file')
    parser.add_argument(dest="config_file",
                        help="config file and its path", metavar="FILE",
                        type=lambda x: is_valid_file(parser, x))
    parser.add_argument('-o', '--output', dest='output_folder', metavar="FOLDER",
                        help='folder where output files will be saved', type=str)

    args = parser.parse_args()

    config_file = Path(args.config_file)
    output_folder = Path(args.output_folder if args.output_folder else "output")

    runner = Runner(config_file, output_folder)
    runner.run()


if __name__ == '__main__':
    main()

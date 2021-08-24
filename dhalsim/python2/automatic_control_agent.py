import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path

from automatic_node import NodeControl
from py2_logger import get_logger

empty_loc = '/dev/null'


class AgentHandler(NodeControl):
    """
    This class is started for a control agent. It starts the agent process.
    """

    def __init__(self, intermediate_yaml):
        super(AgentHandler, self).__init__(intermediate_yaml)

        self.logger = get_logger(self.data['log_level'])

        # self.output_path = Path(self.data["output_path"])
        self.agent_process = None

    def terminate(self):
        """
        This function stops the agent process
        """
        self.logger.debug('Stopping control agent.')

        self.agent_process.send_signal(signal.SIGINT)
        self.agent_process.wait()
        if self.agent_process.poll() is None:
            self.agent_process.terminate()
        if self.agent_process.poll() is None:
            self.agent_process.kill()

    def start_agent(self):
        """
        Start control agent.
        """
        generic_agent_path = Path(__file__).parent.parent.absolute() / "control_agent" / "generic_agent.py"
        self.logger.info(generic_agent_path)

        if self.data['log_level'] == 'debug':
            err_put = sys.stderr
            out_put = sys.stdout
        else:
            err_put = sys.stderr
            # err_put = open(empty_loc, 'w')
            out_put = open(empty_loc, 'w')
        cmd = ["python3", str(generic_agent_path), str(self.intermediate_yaml)]
        generic_agent_process = subprocess.Popen(cmd, shell=False, stderr=err_put, stdout=out_put)
        self.logger.info('something')

        return generic_agent_process

    def main(self):
        """
        This function starts the control agent process and then waits for the
        process to finish.
        """
        self.agent_process = self.start_agent()
        while self.agent_process.poll() is None:
            pass
        self.terminate()


def is_valid_file(parser_instance, arg):
    """
    Verifies whether the intermediate yaml path is valid.
    """
    if not os.path.exists(arg):
        parser_instance.error(arg + " does not exist.")
    else:
        return arg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start everything for a control agent')
    parser.add_argument(dest="intermediate_yaml",
                        help="intermediate yaml file", metavar="FILE",
                        type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()
    agent_handler = AgentHandler(Path(args.intermediate_yaml))
    agent_handler.main()

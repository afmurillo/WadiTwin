import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path

from automatic_node import NodeControl
from py2_logger import get_logger

empty_loc = '/dev/null'


class ScadaControl(NodeControl):
    """
    This class is started for a scada. It starts a tcpdump and a scada process.
    """

    def __init__(self, intermediate_yaml):
        super(ScadaControl, self).__init__(intermediate_yaml)

        self.logger = get_logger(self.data['log_level'])

        self.output_path = Path(self.data["output_path"])
        self.process_tcp_dump = None
        self.scada_process = None

    def terminate(self):
        """
        This function stops the tcp dump and the plc process.
        """
        self.logger.debug("Stopping tcpdump process on SCADA.")

        self.process_tcp_dump.send_signal(signal.SIGINT)
        self.process_tcp_dump.wait()
        if self.process_tcp_dump.poll() is None:
            self.process_tcp_dump.terminate()
        if self.process_tcp_dump.poll() is None:
            self.process_tcp_dump.kill()

        self.logger.debug("Stopping SCADA.")
        self.scada_process.send_signal(signal.SIGINT)
        self.scada_process.wait()
        if self.scada_process.poll() is None:
            self.scada_process.terminate()
        if self.scada_process.poll() is None:
            self.scada_process.kill()

    def main(self):
        """
        This function starts the tcp dump and scada process and then waits for the scada
        process to finish.
        """
        self.process_tcp_dump = self.start_tcpdump_capture()

        self.scada_process = self.start_scada()

        while self.scada_process.poll() is None:
            pass

        self.terminate()

    def start_tcpdump_capture(self):
        """
        Start a tcp dump.
        """
        pcap = self.output_path / "scada-eth0.pcap"

        # Output is not printed to console
        no_output = open(empty_loc, 'w')
        tcp_dump = subprocess.Popen(['tcpdump', '-i', self.data["scada"]["interface"], '-w',
                                     str(pcap)], shell=False, stdout=no_output, stderr=no_output)
        return tcp_dump

    def start_scada(self):
        """
        Start a scada process.
        """
        generic_scada_path = Path(__file__).parent.absolute() / "generic_scada.py"

        if self.data['log_level'] == 'debug':
            err_put = sys.stderr
            out_put = sys.stdout
        else:
            err_put = sys.stderr
            #err_put = open(empty_loc, 'w')
            out_put = open(empty_loc, 'w')
        cmd = ["python2", str(generic_scada_path), str(self.intermediate_yaml)]
        scada_process = subprocess.Popen(cmd, shell=False, stderr=err_put, stdout=out_put)
        return scada_process


def is_valid_file(parser_instance, arg):
    """
    Verifies whether the intermediate yaml path is valid.
    """
    if not os.path.exists(arg):
        parser_instance.error(arg + " does not exist.")
    else:
        return arg


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start everything for a plc')
    parser.add_argument(dest="intermediate_yaml",
                        help="intermediate yaml file", metavar="FILE",
                        type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()
    scada = ScadaControl(Path(args.intermediate_yaml))
    scada.main()

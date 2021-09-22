import pandas as pd
import yaml
from mushroom_rl.core import Core
from mushroom_rl.algorithms.value import DQN
from mushroom_rl.approximators.parametric import TorchApproximator
from mushroom_rl.policy import EpsGreedy
from mushroom_rl.utils.parameters import LinearParameter, Parameter
from mushroom_rl.utils.replay_memory import ReplayMemory
from mushroom_rl.utils.callbacks import CollectDataset, CollectMaxQ
from torch.optim.adam import Adam
from torch.nn import functional as F

from dhalsim.control_agent.dqn.env import WaterNetworkEnvironment


class DQNAgent(DQN):
    """

    """
    def __init__(self, agent_config_file):
        """
        Initialize of the DQN control agent.
        Create of all the data structure needed to run the algorithm, retrieving data from the agent_config_file.
        Initialize the environment.
        """
        self.agent_config_file = agent_config_file
        self.intermediate_yaml_data = None
        self.env = None

    def reset_environment(self, intermediate_yaml_path):
        """
        Reset the environment before the start of a new simulation, but keeps the same agent

        :param intermediate_yaml_path: path of the intermediate_yaml of the current simulation
        """
        with intermediate_yaml_path.open(mode='r') as yaml_file:
            self.intermediate_yaml_data = yaml.load(yaml_file, Loader=yaml.FullLoader)

        if self.env is None:
            self.env = WaterNetworkEnvironment(self.agent_config_file, self.intermediate_yaml_data['db_control_path'])

    def learn(self):
        # self.env.learn() or self.env.eval()
        pass

    def terminate_episode(self):
        pass
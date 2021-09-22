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
        with self.agent_config_file.open(mode='r') as config_file:
            self.config_agent = yaml.load(config_file, Loader=yaml.FullLoader)

        self.intermediate_yaml_data = None
        self.test_simulation = False

        self.env = None

        # Creating the epsilon greedy policy
        self.epsilon_train = LinearParameter(value=1., threshold_value=.1, n=1000000)
        self.epsilon_test = Parameter(value=.05)
        self.epsilon_random = Parameter(value=1)
        self.pi = EpsGreedy(epsilon=self.epsilon_random)

        # Create the optimizer dictionary
        self.optimizer = dict()
        self.optimizer['class'] = Adam
        self.optimizer['params'] = self.config_agent['optimizer']

        self.replay_buffer = None
        self.dataset = None
        self.core = None
        self.scores = []

    def reset_environment(self, intermediate_yaml_path):
        """
        Reset the environment before the start of a new simulation, but keeps the same agent

        :param intermediate_yaml_path: path of the intermediate_yaml of the current simulation
        """
        with intermediate_yaml_path.open(mode='r') as yaml_file:
            self.intermediate_yaml_data = yaml.load(yaml_file, Loader=yaml.FullLoader)
            # TODO: add test o train in the intermediate yaml of each simul
            # self.test_simulation = self.intermediate_yaml_data['test_simulation']

        if self.env is None:
            self.env = WaterNetworkEnvironment(self.agent_config_file, self.intermediate_yaml_data['db_control_path'])
            self.build_model()

        self.start()

    def build_model(self):
        """
        Build the entire model with all the relative data structure. We have to wait the creation of the environment
        to perform this operation.
        """
        # Set parameters of neural network taken by the torch approximator
        nn_params = dict(hidden_size=self.config_agent['nn']['hidden_size'])

        # Create the approximator from the neural network we have implemented
        approximator = TorchApproximator

        # Set parameters of approximator
        approximator_params = dict(
            network=nn.NN10Layers,
            input_shape=self.env.info.observation_space.shape,
            output_shape=(self.env.info.action_space.n,),
            n_actions=self.env.info.action_space.n,
            optimizer=self.optimizer,
            loss=F.smooth_l1_loss,
            batch_size=0,
            use_cuda=False,
            **nn_params
        )

        # Build replay buffer
        self.replay_buffer = ReplayMemory(initial_size=self.config_agent['agent']['initial_replay_memory'],
                                          max_size=self.config_agent['agent']['max_replay_size'])

        super().__init__(mdp_info=self.env.info,
                         policy=self.pi,
                         approximator=approximator,
                         approximator_params=approximator_params,
                         batch_size=self.config_agent['agent']['batch_size'],
                         target_update_frequency=self.config_agent['agent']['target_update_frequency'],
                         replay_memory=self.replay_buffer,
                         initial_replay_size=self.config_agent['agent']['initial_replay_memory'],
                         max_replay_size=self.config_agent['agent']['max_replay_size']
                         )

        # Callbacks
        # self.dataset = CollectDataset()
        self.core = Core(self, self.env, callbacks_fit=[self.dataset])

    def start(self):
        """
        Start the control agent and the related control problem.
        """
        self.pi.set_epsilon(self.epsilon_random)

        if self.replay_buffer.size < self.config_agent['agent']['initial_replay_memory']:
            # Fill replay memory with random data
            self.core.learn(n_steps=self.config_agent['agent']['initial_replay_memory'] - self.replay_buffer.size,
                            n_steps_per_fit=self.config_agent['agent']['initial_replay_memory'], render=False)

    def terminate_episode(self):
        pass

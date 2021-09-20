import torch
import torch.nn as nn
import torch.nn.functional as F


class NN10Layers(nn.Module):
    def __init__(self, input_shape, output_shape, hidden_size, weights=None, **kwargs):
        """

        :param input_shape:
        :param output_shape:
        :param hidden_size:
        :param kwargs:
        """
        super(NN10Layers, self).__init__()

        self.hidden_size = hidden_size
        n_input = input_shape[0]
        n_output = output_shape[0]

        self._collect_qs = False
        self._q_values = []

        self.input = nn.Linear(n_input, hidden_size)
        self.hidden1 = nn.Linear(hidden_size, hidden_size)
        self.hidden2 = nn.Linear(hidden_size, hidden_size)
        self.hidden3 = nn.Linear(hidden_size, hidden_size)
        self.hidden4 = nn.Linear(hidden_size, hidden_size)
        self.hidden5 = nn.Linear(hidden_size, hidden_size)
        self.hidden6 = nn.Linear(hidden_size, hidden_size)
        self.hidden7 = nn.Linear(hidden_size, hidden_size)
        self.hidden8 = nn.Linear(hidden_size, hidden_size)
        self.output = nn.Linear(hidden_size, n_output)

    def forward(self, input, action=None):
        """

        :param x:
        :param action:
        :return:
        """
        x = F.relu(self.input(input))
        x = F.relu(self.hidden1(x))
        x = F.relu(self.hidden2(x))
        x = F.relu(self.hidden3(x))
        x = F.relu(self.hidden4(x))
        x = F.relu(self.hidden5(x))
        x = F.relu(self.hidden6(x))
        x = F.relu(self.hidden7(x))
        x = F.relu(self.hidden8(x))
        # x = F.relu(self.hidden8(x.view(-1, self.hidden_size)))
        q = self.output(x)

        # collects q values
        if self._collect_qs:
            self._q_values.extend([list([input.tolist()[0][:2], q.tolist()[0]])])

        # action is for backpropagation
        if action is None:
            return q
        else:
            q_acted = torch.squeeze(q.gather(1, action.long()))
            return q_acted

    def collect_qs_enabled(self, bool):
        """
        Enables the collection of Q values
        :param bool:
        :return:
        """
        if bool:
            self._q_values = []
        self._collect_qs = bool

    def retrieve_qs(self):
        """
        Retrieve the list of collected Q values
        :return:
        """
        return self._q_values


class CustomNN(nn.Module):
    def __init__(self, input_shape, output_shape, hidden_size, n_layers):
        """

        :param input_shape:
        :param output_shape:
        :param hidden_size: list of tuples of 2 values (input and output of that layer)
        :param n_layers:
        """
        super(CustomNN, self).__init__()
        self.layers = nn.ModuleList([nn.Linear(input_shape, hidden_size[0][0])])
        self.layers.extend([nn.Linear(hidden_size[i][0], hidden_size[i][1]) for i in range(0, n_layers - 2 - 1)])
        self.layers.extend([nn.Linear(hidden_size[-1][1], output_shape)])

    def forward(self, x):
        """

        :param x:
        :return:
        """
        for i in range(len(self.layers) - 1):
            x = F.relu(self.layers[i](x))
        x = self.layers[-1](x)

        return F.log_softmax(x, dim=1)

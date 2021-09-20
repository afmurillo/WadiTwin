from mushroom_rl.utils.dataset import compute_metrics
from mushroom_rl.core import Logger
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use("TkAgg")


class InfoLogger:
    def __init__(self, folder_name=None, folder_path=None):
        self.logger = None
        if folder_path and folder_path:
            self.logger = Logger(log_name=folder_name, results_dir=folder_path, log_console=True)

    def experiment_summary(self, info: str):
        if self.logger:
            self.logger.strong_line()
            self.logger.info(info)
            self.logger.strong_line()
        else:
            print(info)

    def fill_replay_memory(self):
        if self.logger:
            self.logger.info('Filling replay memory')
            self.logger.weak_line()

    def print_epoch(self, epoch):
        if self.logger:
            self.logger.epoch_info(epoch=epoch)
            self.logger.weak_line()

    def get_stats(self, dataset):
        score = compute_metrics(dataset)
        # self.logger.info('min_reward: %f, max_reward: %f, mean_reward: %f, games_completed: %d')
        return score

    def training_phase(self):
        if self.logger:
            self.logger.info('Learning...')
            self.logger.weak_line()
        else:
            print('Learning...')

    def evaluation_phase(self):
        if self.logger:
            self.logger.info('Evaluation..')
            self.logger.weak_line()
        else:
            print('Evaluating...')

    def end_phase(self):
        if self.logger:
            self.logger.strong_line()

    def results(self, dsr, n_updates):
        if self.logger:
            self.logger.info('DSR: ' + str(dsr))
            self.logger.info('Total updates: ' + str(n_updates))
            self.logger.weak_line()
        else:
            print('DSR: ' + str(dsr))
            print('Total updates: ' + str(n_updates))


class Plotter:
    def __init__(self, title):
        plt.ion()
        self.min_x = 0
        self.max_x = 10
        self.title = title
        self.sample_count = 0
        self.on_launch()
        self.xdata = []
        self.ydata = []

    def on_launch(self):
        self.figure, self.ax = plt.subplots()
        self.lines, = self.ax.plot([], [], 'o-', label='DSR')
        plt.title(self.title)
        self.ax.set_autoscaley_on(True)
        # self.ax.set_xlim(self.min_x, self.max_x)
        self.ax.grid()
        plt.show()

    def update(self, ydata):
        # Update data (with the new _and_ the old points)
        self.xdata.append(self.sample_count)
        self.ydata.append(ydata)
        self.lines.set_xdata(self.xdata)
        self.lines.set_ydata(self.ydata)
        self.sample_count += 1
        # Need both of these in order to rescale
        self.ax.relim()
        self.ax.autoscale_view()
        # We need to draw *and* flush
        self.figure.canvas.draw()
        self.figure.canvas.flush_events()

import matplotlib.pyplot as plt
from huiAudioCorpus.model.Histogram import Histogram
from huiAudioCorpus.utils.PathUtil import PathUtil
import logging
logging.getLogger('matplotlib.font_manager').disabled = True
logging.getLogger('matplotlib.colorbar').disabled = True

class Plot:
    def __init__(self, show_duration: int, save_path: str = ''):
        self.show_duration = show_duration
        self.save_path = save_path
        self.path_util = PathUtil()

    def histogram(self, histogram: Histogram, name: str, log_scale_y=False, log_scale_x=False):
        plt.clf()
        _, ax = plt.subplots()

        ax.bar(histogram.bins, histogram.values, width=1)  # type: ignore
        ax.set_ylabel('count')  # type: ignore
        ax.set_xlabel('bins')  # type: ignore
        ax.set_title(name)  # type: ignore
        if log_scale_y:
            ax.set_yscale('log')
        if log_scale_x:
            ax.set_xscale('log')

    def show(self):
        plt.show(block=False)
        plt.pause(self.show_duration)
        plt.close()

    def save(self, filename: str):
        filename = self.save_path + '/' + filename
        self.path_util.create_folder_for_file(filename)
        plt.savefig(filename, dpi=200)

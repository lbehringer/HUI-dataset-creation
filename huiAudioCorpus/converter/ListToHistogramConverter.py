from huiAudioCorpus.model.Histogram import Histogram
from typing import List, TypeVar

import numpy as np
Number = TypeVar('Number', int, float)

class ListToHistogramConverter:
    def __init__(self, step_size: int):
        self.step_size = step_size

    def convert(self, input_list: List[Number]):
        bins = np.arange(round(min(1, min(input_list))) - 1, max(input_list) + 2 * self.step_size, self.step_size)
        export_bins: List[Number]
        values: List[Number]
        values_numpy, export_bins_numpy = np.histogram(input_list, bins=bins)
        export_bins = export_bins_numpy.tolist()
        values = values_numpy.tolist()
        histogram = Histogram(export_bins[:-1], values)
        return histogram

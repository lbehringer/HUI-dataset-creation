import numpy as np
from huiAudioCorpus.model.Statistic import Statistic

from typing import List, TypeVar

Number = TypeVar('Number', int, float)

class ListToStatisticConverter:

    def convert(self, input_list: List[Number]):
        count = len(input_list)
        maximum = max(input_list)
        minimum = min(input_list)
        total = sum(input_list)
        median: float
        median = np.median(input_list)
        std = np.std(input_list)
        var = np.var(input_list)
        average = total / count
        statistic = Statistic(count, maximum, minimum, median, average, total, std, var)
        return statistic

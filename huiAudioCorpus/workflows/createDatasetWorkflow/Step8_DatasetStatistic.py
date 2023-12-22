from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.utils.PathUtil import PathUtil
import pandas as pd
from huiAudioCorpus.components.AudioStatisticComponent import AudioStatisticComponent
from huiAudioCorpus.components.TextStatisticComponent import TextStatisticComponent
from typing import List
from huiAudioCorpus.ui.Plot import Plot
from pandas_profiling import ProfileReport

class Step8DatasetStatistic:
    def __init__(self, save_path: str, load_path: str, special_speakers: List[str], data_filter, path_util: PathUtil,
                 audio_statistic_component: AudioStatisticComponent, text_statistic_component: TextStatisticComponent, plot: Plot):
        self.save_path = save_path
        self.path_util = path_util
        self.load_path = load_path
        self.special_speakers = special_speakers
        self.data_filter = data_filter
        self.audio_statistic_component = audio_statistic_component
        self.text_statistic_component = text_statistic_component
        self.plot = plot

    def run(self):
        done_marker = DoneMarker(self.save_path)
        result = done_marker.run(self.script, delete_folder=False)
        return result

    def script(self):
        raw_data = pd.read_csv(self.load_path, sep='|', index_col='id')

        print('Audios before: ', raw_data.shape[0])
        info_text = " - full"
        if self.data_filter is not None:
            info_text = " - clean"
            raw_data = self.data_filter(raw_data)
        print('Audios after: ', raw_data.shape[0])

        print(raw_data)

        # all Speakers
        self.save_summary(raw_data, self.save_path + '/complete', "All speakers" + info_text)

        # every Speaker
        for speaker_id, data in raw_data.groupby('speaker'):
            self.save_summary(data, self.save_path + '/speaker/' + speaker_id, "Speaker: " + speaker_id + info_text)

        # others
        others = raw_data[~raw_data['speaker'].isin(self.special_speakers)]
        if not others.empty:
            self.save_summary(others, self.save_path + '/others', "Other speakers" + info_text)

    def save_summary(self, raw_data, save_path: str, title: str):
        print(title)
        statistics_text, _, counter, unique_words_with_minimum = self.text_statistic_component.get_statistic(raw_data)
        statistics_audio, _ = self.audio_statistic_component.get_statistic(raw_data)

        statistics = {**statistics_audio, **statistics_text}
        file_path = save_path + '/statistic.txt'
        self.path_util.create_folder_for_file(file_path)

        raw_data.to_csv(save_path + '/overview.csv', sep='|')

        profile = raw_data.profile_report(title=title)
        profile.to_file(save_path + "/profilingReport.html")

        self.path_util.save_json(save_path + '/wordCounts.json', counter)
        self.path_util.save_json(save_path + '/uniqueWordsWithMinimalNumberOfOccurrences.json', unique_words_with_minimum)

        with open(file_path, 'w') as text_file:
            for statistic in statistics.values():
                text_file.write(statistic['description'])
                text_file.write('\n')
                text_file.write(statistic['statistic'].__str__())
                text_file.write('\n')
                text_file.write('\n')
                text_file.write('\n')

        histogram_data = {}
        extract_histogram = lambda hist: {'bins': hist.bins, 'values': hist.values}

        for statistic in statistics.values():
            if statistic['name'] == 'samplingrate':
                continue
            self.plot.histogram(statistic['histogram'], statistic['description'])
            self.plot.save_path = save_path
            self.plot.save(statistic['name'])
            histogram_data[statistic['name']] = extract_histogram(statistic['histogram'])

        self.path_util.save_json(save_path + '/histogrammData.json', histogram_data)

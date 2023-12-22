from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.components.AudioStatisticComponent import AudioStatisticComponent
from huiAudioCorpus.ui.Plot import Plot


class Step2_1_AudioStatistic:
    def __init__(self, save_path: str, audio_statistic_component: AudioStatisticComponent, plot: Plot):
        self.save_path = save_path
        self.audio_statistic_component = audio_statistic_component
        self.plot = plot

    def run(self):
        done_marker = DoneMarker(self.save_path)
        result = done_marker.run(self.script, delete_folder=False)
        return result

    def script(self):
        statistics, raw_data = self.audio_statistic_component.run()

        self.plot.histogram(statistics['duration']['histogram'], statistics['duration']['description'])
        self.plot.save('audioLength')
        self.plot.show()

        with open(self.save_path + '/statistic.txt', 'w') as text_file:
            for statistic in statistics.values():
                print(statistic['description'])
                print(statistic['statistic'])
                text_file.write(statistic['description'])
                text_file.write('\n')
                text_file.write(statistic['statistic'].__str__())
                text_file.write('\n')

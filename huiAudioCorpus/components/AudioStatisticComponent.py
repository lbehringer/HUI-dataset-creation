from huiAudioCorpus.model.Audio import Audio
from pandas.core.frame import DataFrame
from huiAudioCorpus.converter.ListToHistogramConverter import ListToHistogramConverter
from huiAudioCorpus.converter.ListToStatisticConverter import ListToStatisticConverter
from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from joblib import Parallel, delayed

class AudioStatisticComponent:
    def __init__(self, audio_persistenz: AudioPersistenz, list_to_statistic_converter: ListToStatisticConverter, list_to_histogram_converter: ListToHistogramConverter):
        self.audio_persistenz = audio_persistenz
        self.list_to_statistic_converter = list_to_statistic_converter
        self.list_to_histogram_converter = list_to_histogram_converter
        self.columns = ['id', 'duration', 'loudness', 'min_silence_db', 'sampling_rate', 'silence_percent', 'average_frequency']

    def run(self):
        raw_data = self.load_audio_files()
        return self.get_statistic(raw_data)

    def get_statistic(self, raw_data):
        descriptions = ['Length in seconds', 'Loudness in DB', 'Minimum silence in DB', 'Sampling rate in Hz', 'Silence in percent', 'Average frequency in Hz']
        statistics = {}
        for column in raw_data:
            if column not in self.columns:
                continue
            statistics[column] = {
                'name': column,
                'statistic': self.list_to_statistic_converter.convert(raw_data[column].tolist()),
                'histogram': self.list_to_histogram_converter.convert(raw_data[column].tolist()),
                'description': descriptions[len(statistics)]
            }

        return statistics, raw_data

    def load_audio_files(self):
        result = Parallel(n_jobs=4, verbose=10, batch_size=100)(delayed(self.load_audio)(audio) for audio in self.audio_persistenz.get_ids())
        raw_data = DataFrame(result, columns=self.columns)
        raw_data = raw_data.set_index('id')
        return raw_data

    def load_audio(self, audio_id: str):
        audio = self.audio_persistenz.load(audio_id)
        return [audio.id.split("\\")[-1].split("/")[-1], round(audio.duration, 1), round(audio.loudness, 1),
                round(audio.silence_DB, 1), audio.sampling_rate, round(audio.silence_percent * 100),
                round(audio.average_frequency)]

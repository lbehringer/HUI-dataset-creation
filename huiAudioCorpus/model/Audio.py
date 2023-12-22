import numpy as np
from numpy.lib.function_base import average
from huiAudioCorpus.utils.ModelToStringConverter import ToString
from nptyping import NDArray
import pyloudnorm as pyln
import librosa

class Audio(ToString):
    def __init__(self, audio_time_series: NDArray, sampling_rate: int, audio_id: str, name: str):
        self.time_series = audio_time_series
        self.sampling_rate = sampling_rate
        self.name = name
        self.id = audio_id

    @property
    def samples(self) -> int:
        return self.time_series.shape[0]

    @property
    def duration(self) -> float:
        return self.samples / self.sampling_rate

    def __add__(self, other: 'Audio') -> 'Audio':
        audio_time_series = self.time_series.tolist() + other.time_series.tolist()
        audio_time_series = np.array(audio_time_series)
        audio_id = self.id + '&' + other.id
        name = self.name + '&' + other.name

        sampling_rate_self = self.sampling_rate
        sampling_rate_other = other.sampling_rate
        if sampling_rate_other != sampling_rate_self:
            raise ValueError(f"The sampling rates from the audio files are different: sr1={sampling_rate_self}, sr2={sampling_rate_other}, from the audio files with the combined id: {audio_id} and name: {name}")

        audio = Audio(audio_time_series, sampling_rate_self, audio_id, name)
        return audio

    def __radd__(self, other):
        return self

    @property
    def loudness(self) -> float:
        meter = pyln.Meter(self.sampling_rate)  # create BS.1770 meter
        loudness = meter.integrated_loudness(self.time_series)
        return loudness

    @property
    def silence_db(self) -> float:
        silence_duration_in_seconds = 0.05
        frame_length = int(silence_duration_in_seconds * self.sampling_rate)
        for silence_decibel in range(100, 1, -1):
            splitted = librosa.effects.split(y=self.time_series, top_db=silence_decibel, frame_length=frame_length, hop_length=int(frame_length / 4))
            if len(splitted) > 1:
                return -silence_decibel
        return 0

    @property
    def silence_percent(self) -> float:
        states = self.is_loud()
        silence_percent = 1 - sum(states) / len(states)
        return silence_percent

    def is_loud(self):
        rms = librosa.feature.rms(y=self.time_series)[0]  # type: ignore
        r_normalized = (rms - 0.02) / np.std(rms)
        p = np.exp(r_normalized) / (1 + np.exp(r_normalized))  # type: ignore
        transition = librosa.sequence.transition_loop(2, [0.5, 0.6])
        full_p = np.vstack([1 - p, p])
        states = librosa.sequence.viterbi_discriminative(full_p, transition)
        return states

    @property
    def average_frequency(self) -> float:
        try:
            cent = librosa.feature.spectral_centroid(y=self.time_series, sr=self.sampling_rate)[0]  # type: ignore
            loud_positions = self.is_loud()
            cent_at_loud = [cent[index] for index in range(len(cent)) if loud_positions[index] == 1]
            average_frequency = round(average(cent_at_loud))  # type: ignore
            return average_frequency
        except:
            return -1

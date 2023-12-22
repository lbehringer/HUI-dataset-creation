import numpy as np
from huiAudioCorpus.model.Audio import Audio


class AudioAddSilenceTransformer:

    def __init__(self, start_duration_seconds: float, end_duration_seconds: float):
        self.start_duration_seconds = start_duration_seconds
        self.end_duration_seconds = end_duration_seconds

    def transform(self, audio: Audio):
        silence_audio_front = self.generate_silence(self.start_duration_seconds, audio.sampling_rate)
        silence_audio_back = self.generate_silence(self.end_duration_seconds, audio.sampling_rate)
        new_audio = silence_audio_front + audio + silence_audio_back
        return new_audio

    def generate_silence(self, duration: float, sampling_rate: int):
        silence_data_points = int(duration * sampling_rate)
        silence = np.zeros(silence_data_points)
        silence_audio = Audio(silence, sampling_rate, 's', 's')
        return silence_audio

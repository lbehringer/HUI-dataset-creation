from huiAudioCorpus.model.Audio import Audio
import numpy as np

class AudioFadeTransformer:

    def __init__(self, fade_in_duration: float = 0.1, fade_out_duration: float = 0.1):
        self.fade_in_duration = fade_in_duration
        self.fade_out_duration = fade_out_duration

    def transform(self, audio: Audio):
        audio = self.fade_out(audio)
        audio = self.fade_in(audio)
        return audio
        

    def fade_out(self, audio: Audio) -> Audio: 
        count_of_samples = int(self.fade_out_duration * audio.sampling_rate)
        end = audio.samples
        start = end - count_of_samples

        # compute fade out curve
        # linear fade
        fade_curve = np.linspace(1.0, 0.0, count_of_samples)

        # apply the curve
        audio.time_series[start:end] = audio.time_series[start:end] * fade_curve
        return audio

    def fade_in(self, audio: Audio) -> Audio: 
        count_of_samples = int(self.fade_in_duration * audio.sampling_rate)
        end = count_of_samples
        start = 0

        # compute fade in curve
        # linear fade
        fade_curve = np.linspace(0.0, 1.0, count_of_samples)

        # apply the curve
        audio.time_series[start:end] = audio.time_series[start:end] * fade_curve
        return audio

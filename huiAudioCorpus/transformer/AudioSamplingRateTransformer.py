import librosa
from huiAudioCorpus.model.Audio import Audio

class AudioSamplingRateTransformer():

    def __init__(self, target_sampling_rate: int = None):
        self.target_sampling_rate = target_sampling_rate

    def transform(self, audio: Audio):
        if self.target_sampling_rate is None:
            return audio
        if audio.sampling_rate == self.target_sampling_rate:
            return audio
        audio_time_series = audio.time_series
        sampling_rate = audio.sampling_rate
        resampled_time_series = librosa.core.resample(y=audio_time_series, orig_sr=sampling_rate, target_sr=self.target_sampling_rate)
        resampled_audio = Audio(resampled_time_series, self.target_sampling_rate, audio.id, audio.name)
        return resampled_audio

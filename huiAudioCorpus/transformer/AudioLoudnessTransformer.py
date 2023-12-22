from huiAudioCorpus.model.Audio import Audio
import pyloudnorm as pyln

class AudioLoudnessTransformer:

    def __init__(self, loudness_target: int):
        self.loudness_target = loudness_target

    def transform(self, audio: Audio):
        meter = pyln.Meter(audio.sampling_rate)  # create BS.1770 meter

        loudness_normalized_audio = pyln.normalize.loudness(audio.time_series, audio.loudness, self.loudness_target)
        new_audio = Audio(loudness_normalized_audio, audio.sampling_rate, audio.id, audio.name)
        return new_audio

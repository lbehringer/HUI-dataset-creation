import librosa
from huiAudioCorpus.model.Audio import Audio

class AudioRemoveSilenceTransformer:

    def __init__(self, decibel_threshold: int):
        self.decibel_threshold = decibel_threshold

    def transform(self, audio: Audio):
        new_audio_timeline, _ = librosa.effects.trim(audio.time_series, self.decibel_threshold)
        new_audio = Audio(new_audio_timeline, audio.sampling_rate, audio.id, audio.name)
        return new_audio

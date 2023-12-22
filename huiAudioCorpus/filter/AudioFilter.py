from huiAudioCorpus.model.Audio import Audio
from typing import List

class AudioFilter:

    def __init__(self, max_duration=None, names: List[str] = None):
        self.max_duration = float('inf') if max_duration is None else max_duration
        self.names = names

    def is_allowed(self, audio: Audio):
        if audio.duration >= self.max_duration:
            return False
        if self.names is not None and audio.name not in self.names:
            return False
        return True

    def filter(self, audios: List[Audio]):
        filtered_audios = [audio for audio in audios if self.is_allowed(audio)]
        return filtered_audios

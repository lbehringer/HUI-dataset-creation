

from huiAudioCorpus.persistence.AudiosFromLibrivoxPersistence import AudiosFromLibrivoxPersistence
from huiAudioCorpus.utils.DoneMarker import DoneMarker


class Step1_DownloadAudio:

    def __init__(self, audios_from_librivox_persistence: AudiosFromLibrivoxPersistence, save_path: str):
        self.save_path = save_path
        self.audios_from_librivox_persistence = audios_from_librivox_persistence

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        self.audios_from_librivox_persistence.save()

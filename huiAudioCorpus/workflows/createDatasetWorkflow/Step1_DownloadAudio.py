

from huiAudioCorpus.persistenz.AudiosFromLibrivoxPersistenz import AudiosFromLibrivoxPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker


class Step1_DownloadAudio:

    def __init__(self, audios_from_librivox_persistenz: AudiosFromLibrivoxPersistenz, save_path: str):
        self.save_path = save_path
        self.audios_from_librivox_persistenz = audios_from_librivox_persistenz

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        self.audios_from_librivox_persistenz.save()

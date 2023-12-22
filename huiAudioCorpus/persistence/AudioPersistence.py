import librosa
import soundfile
from huiAudioCorpus.model.Audio import Audio
from nptyping import NDArray
from huiAudioCorpus.utils.FileListUtil import FileListUtil
from huiAudioCorpus.utils.PathUtil import PathUtil
from natsort import natsorted

class AudioPersistence:
    def __init__(self, load_path: str, save_path: str = None, file_extension: str = 'wav'):
        self.save_path = load_path if save_path is None else save_path
        self.load_path = load_path
        self.file_extension = file_extension
        self.file_list_util = FileListUtil()
        self.path_util = PathUtil()

    def load(self, audio_id: str, duration=None, offset=0.0):
        """Load an audio file and return it as an Audio object"""
        audio_time_series: NDArray
        sampling_rate: int
        target_path = self.load_path + '/' + audio_id + '.' + self.file_extension
        name = self.path_util.filename_without_extension(target_path)
        # give duration higher priority than offset:
        # if the file is not long enough to allow the specified duration with the specified offset, reduce offset
        if offset > 0.0:
            file_duration = librosa.get_duration(path=target_path)
            if file_duration - offset < duration:
                offset = max(0, file_duration - duration)
        audio_time_series, sampling_rate = librosa.core.load(target_path, sr=None, duration=duration, offset=offset)  # type: ignore
        audio = Audio(audio_time_series, sampling_rate, audio_id, name)
        return audio

    def save(self, audio: Audio):
        """Save an Audio object to a file"""
        target_path = self.save_path + '/' + audio.id + '.wav'
        self.path_util.create_folder_for_file(target_path)
        audio_time_series = audio.time_series
        sampling_rate = audio.sampling_rate
        soundfile.write(target_path, audio_time_series, sampling_rate)

    def get_names(self):
        names = [self.path_util.filename_without_extension(audio_id) for audio_id in self.get_ids()]
        return names

    def get_ids(self):
        audio_files = self.file_list_util.get_files(self.load_path, self.file_extension)
        audio_files = [file.replace(self.load_path, '')[1:-len(self.file_extension)-1] for file in audio_files]
        audio_files = natsorted(audio_files)

        return audio_files

    def load_all(self, duration=None, offset=0.0):
        ids = self.get_ids()
        for audio_id in ids:
            yield self.load(audio_id, duration=duration, offset=offset)

from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.transformer.AudioSplitTransformer import AudioSplitTransformer
from joblib import Parallel, delayed


class Hifi_QA1:
    def __init__(
            self, 
            audio_split_transformer: AudioSplitTransformer, 
            audio_persistenz: AudioPersistenz, 
            save_path: str, 
            book_name: str,
            analyzed_seconds=30):
        self.audio_persistenz = audio_persistenz
        self.save_path = save_path
        self.audio_split_transformer = audio_split_transformer
        self.book_name = book_name
        self.analyzed_seconds = analyzed_seconds

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistenz.loadAll()
        Parallel(n_jobs=1, verbose=10, batch_size= "auto")(delayed(self.get_split_to_analyze)(audio, idx, self.analyzed_seconds) for idx, audio in enumerate(audios))

    def get_split_to_analyze(self, audio: Audio, idx: int):
        """If possible, trims an Audio object to the first `analyzed_seconds`, and returns the trimmed Audio."""
        if audio.timeSeries / audio.samplingRate > self.analyzed_seconds:
            audio.timeSeries = audio.timeSeries[:audio.samplingRate * self.analyzed_seconds]
            audio = self.audio_split_transformer.set_x_seconds_id(audio, self.book_name, idx+1, self.analyzed_seconds)
        return audio
    
    def set_x_seconds_id(self, audio: Audio, book_name: str, chapter: int, analyzed_seconds):
        """Assigns an ID to an Audio object, following the format `<book_name>_<chapter>_<analyzed_seconds>sec`. 
        Returns the Audio with the attributes `id` and `name` (with identical values)."""
        name = f"{book_name}_{chapter:02d}_f{analyzed_seconds}sec"
        audio.id = name
        audio.name = name
        return audio
    
    def apply_vad():
        # TODO:
        # 1. resample to 16kHz
        # 2. apply silero VAD with Parallel (https://github.com/snakers4/silero-vad/blob/master/examples/parallel_example.ipynb)
        pass
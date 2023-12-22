

from typing import List
from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.transformer.AudioSplitTransformer import AudioSplitTransformer
from huiAudioCorpus.transformer.AudioLoudnessTransformer import AudioLoudnessTransformer
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from joblib import Parallel, delayed

class Step2_SplitAudio:

    def __init__(self, audio_split_transformer: AudioSplitTransformer, audio_persistenz: AudioPersistenz, save_path: str, book_name: str, audio_loudness_transformer: AudioLoudnessTransformer, remap_sort: List[int] = None):
        self.audio_persistenz = audio_persistenz
        self.save_path = save_path
        self.audio_split_transformer = audio_split_transformer
        self.book_name = book_name
        self.audio_loudness_transformer = audio_loudness_transformer
        self.remap_sort = remap_sort

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistenz.load_all()
        if self.remap_sort:
            audios = list(audios)
            audios = [audios[i] for i in self.remap_sort]

        Parallel(n_jobs=1, verbose=10, batch_size="auto")(delayed(self.split_one_audio)(audio, index) for index, audio in enumerate(audios))

    def split_one_audio(self, audio: Audio, index: int):
        splitted_audios = self.audio_split_transformer.transform(audio, self.book_name, index + 1)
        for split_audio in splitted_audios:
            loudness_audio = self.audio_loudness_transformer.transform(split_audio)
            self.audio_persistenz.save(loudness_audio)


from itertools import count
from typing import List
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.persistence.TranscriptsPersistence import TranscriptsPersistence
from pandas.core.frame import DataFrame
from huiAudioCorpus.model.Transcripts import Transcripts
from huiAudioCorpus.persistence.AudioPersistence import AudioPersistence
from huiAudioCorpus.converter.AudioToSentenceConverter import AudioToSentenceConverter
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from tqdm import tqdm
import numpy as np
from joblib import Parallel, delayed

class Step4TranscriptAudio:

    def __init__(self, save_path: str, audio_to_sentence_converter: AudioToSentenceConverter, audio_persistence: AudioPersistence, transcripts_persistence: TranscriptsPersistence, number_worker=4):
        self.save_path = save_path
        self.audio_to_sentence_converter = audio_to_sentence_converter
        self.audio_persistence = audio_persistence
        self.transcripts_persistence = transcripts_persistence
        self.number_worker = number_worker

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        ids = self.audio_persistence.get_ids()
        # TODO: Parallel crashes here
        # chunks = np.array_split(ids, self.number_worker)
        # print(f"Starting parallel ASR with {self.number_worker} jobs.")
        # parallel_result = Parallel(n_jobs=self.number_worker)(delayed(self.load_one_chunk)(audio_ids, chunk_id) for chunk_id, audio_ids in enumerate(chunks))

        # results = [[sentence.id, sentence.sentence] for level in parallel_result for sentence in level]
        results = self.load_ids(ids)
        results = [[sentence.id, sentence.sentence] for sentence in results]

        df = DataFrame(results)
        transcripts = Transcripts(df, 'transcripts', 'transcripts')
        self.transcripts_persistence.save(transcripts)

    def load_one_chunk(self, ids: List[str], chunk_id: int):
        print("loading one chunk")
        sentences = []
        for id in tqdm(ids, desc="Chunk " + str(chunk_id) + ": "):
            audio = self.audio_persistence.load(id)
            sentence = self.audio_to_sentence_converter.convert(audio)
            sentences.append(sentence)
            print(f"Transcript for id {id}: {sentence.sentence}")
        return sentences

    def load_ids(self, ids: List[str]):
        """Based on given IDs, loads the corresponding audios and transcribes them via ASR, then returns the recognized sentences."""
        sentences = []
        for id in tqdm(ids):
            audio = self.audio_persistence.load(id)
            sentence = self.audio_to_sentence_converter.convert(audio)
            sentences.append(sentence)
        return sentences


from itertools import count
from typing import List
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.persistenz.TranscriptsPersistenz import TranscriptsPersistenz
from pandas.core.frame import DataFrame
from huiAudioCorpus.model.Transcripts import Transcripts
from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.converter.AudioToSentenceConverter import AudioToSentenceConverter
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from tqdm import tqdm
import numpy as np
from joblib import Parallel, delayed

class Step4_TranscriptAudio:

    def __init__(self, savePath: str, audioToSentenceConverter: AudioToSentenceConverter, audioPersistenz: AudioPersistenz, transcriptsPersistenz: TranscriptsPersistenz, numberWorker = 4):
        self.savePath = savePath
        self.audioToSentenceConverter = audioToSentenceConverter
        self.audioPersistenz = audioPersistenz
        self.transcriptsPersistenz = transcriptsPersistenz
        self.numberWorker = numberWorker


    def run(self):
        return DoneMarker(self.savePath).run(self.script)
    
    def script(self):
        ids = self.audioPersistenz.getIds()
        # TODO: Parallel crashes here
        # chunks = np.array_split(ids, self.numberWorker)
        # print(f"Starting parallel ASR with {self.numberWorker} jobs.")
        # parallelResult = Parallel(n_jobs=self.numberWorker)(delayed(self.loadOneChunk)(audioIds, chunkId) for chunkId, audioIds in enumerate(chunks))

        # results = [[sentence.id, sentence.sentence] for level in parallelResult for sentence in level]
        results = self.loadIds(ids)
        results = [[sentence.id, sentence.sentence] for sentence in results]


        df =  DataFrame(results)
        transcripts = Transcripts(df, 'transcripts', 'transcripts')
        self.transcriptsPersistenz.save(transcripts)

    def loadOneChunk(self, ids: List[str], chunkId: int):
        print("loading one chunk")
        sentences = []
        for id in tqdm(ids, desc="Chunk " + str(chunkId) + ": "):
            audio = self.audioPersistenz.load(id)
            sentence = self.audioToSentenceConverter.convert(audio)
            sentences.append(sentence)
            print(f"Transcript for id {id}: {sentence.sentence}")
        return sentences

    def loadIds(self, ids: List[str]):
        """Based on given IDs, loads the corresponding audios and transcribes them via ASR, then returns the recognized sentences."""
        sentences = []
        for id in tqdm(ids):
            audio = self.audioPersistenz.load(id)
            sentence = self.audioToSentenceConverter.convert(audio)
            sentences.append(sentence)
        return sentences

from itertools import count
from typing import Dict, List
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.persistenz.TranscriptsPersistenz import TranscriptsPersistenz
from pandas.core.frame import DataFrame
from huiAudioCorpus.model.Transcripts import Transcripts
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from tqdm import tqdm
import numpy as np
import pandas as pd
from joblib import Parallel, delayed

class Step4_1_NormalizeTranscript:

    def __init__(self, savePath: str, textReplacement: Dict[str,str], transcriptsPersistenz: TranscriptsPersistenz):
        self.savePath = savePath
        self.textReplacement = textReplacement
        self.transcriptsPersistenz = transcriptsPersistenz

    def run(self):
        return DoneMarker(self.savePath).run(self.script)
    
    def script(self):
        # load transcripts objects (as of now, this Generator should always yield a single Transcripts object)
        transcripts = self.transcriptsPersistenz.loadAll()
        for t in transcripts:
        # normalize transcripts with replacements specified in config
            df = t.transcripts
            df.reset_index(drop=True, inplace=True)
            normalized_trancripts = []
            for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Normalizing transcript"):
                normalized_trancripts.append(self.replace(row[1], self.textReplacement))
            df[1] = normalized_trancripts
            t.transcripts = df

            self.transcriptsPersistenz.save(t)
    
    def replace(self, text: str, textReplacement: Dict[str, str]):
        for input, target in textReplacement.items():
            text = text.replace(input,target)
        return text

from itertools import count
from typing import Dict, List
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.persistence.TranscriptsPersistence import TranscriptsPersistence
from pandas.core.frame import DataFrame
from huiAudioCorpus.model.Transcripts import Transcripts
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from tqdm import tqdm
import numpy as np
import pandas as pd
from joblib import Parallel, delayed

class Step4_1NormalizeTranscript:

    def __init__(self, save_path: str, text_replacement: Dict[str, str], transcripts_persistence: TranscriptsPersistence):
        self.save_path = save_path
        self.text_replacement = text_replacement
        self.transcripts_persistence = transcripts_persistence

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        # load transcripts objects (as of now, this Generator should always yield a single Transcripts object)
        transcripts = self.transcripts_persistence.load_all()
        for t in transcripts:
            # normalize transcripts with replacements specified in config
            df = t.transcripts
            df.reset_index(drop=True, inplace=True)
            normalized_transcripts = []
            for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Normalizing transcript"):
                normalized_transcripts.append(self.replace(row[1], self.text_replacement))
            df[1] = normalized_transcripts
            t.transcripts = df

            self.transcripts_persistence.save(t)
    
    def replace(self, text: str, text_replacement: Dict[str, str]):
        for input, target in text_replacement.items():
            text = text.replace(input, target)
        return text

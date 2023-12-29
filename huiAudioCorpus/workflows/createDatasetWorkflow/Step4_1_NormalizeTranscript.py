
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
import re
from nemo_text_processing.text_normalization import Normalizer

class Step4_1_NormalizeTranscript:

    def __init__(self, save_path: str, text_replacement: Dict[str, str], transcripts_persistence: TranscriptsPersistence, language: str):
        self.save_path = save_path
        self.text_replacement = text_replacement
        self.transcripts_persistence = transcripts_persistence
        self.language = language

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        # load transcripts objects (as of now, this Generator should always yield a single Transcripts object)
        transcripts = self.transcripts_persistence.load_all()
        if self.language == "en":
            nemo_norm = Normalizer(
                input_case="cased", 
                lang=self.language,
                )
        
        for t in transcripts:
            # normalize transcripts with replacements specified in config
            df = t.transcripts
            df.reset_index(drop=True, inplace=True)
            normalized_transcripts = []
            for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Normalizing transcript"):
                normalized_sentence = self.replace(row[1], self.text_replacement)
                if self.language == "en" and re.search(r"\d", normalized_sentence):
                    normalized_sentence = nemo_norm.normalize(normalized_sentence)
                normalized_transcripts.append(normalized_sentence)
            df['asr_sentence'] = normalized_transcripts
            t.transcripts = df

            self.transcripts_persistence.save(t)
    
    def replace(self, text: str, text_replacement: Dict[str, str]):
        for input, target in text_replacement.items():
            text = text.replace(input, target)
        return text

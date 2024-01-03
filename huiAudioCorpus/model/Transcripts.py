from huiAudioCorpus.model.Sentence import Sentence
from typing import List
from huiAudioCorpus.utils.ModelToStringConverter import ToString
from pandas.core.frame import DataFrame

class Transcripts(ToString):
    def __init__(self, transcripts: DataFrame, id: str, name: str):
        self.transcripts = transcripts
        self.id = id
        self.name = name
    

    @property
    def transcripts_count(self):
        return self.transcripts.shape[0]

    @property
    def example(self):
        return self.transcripts.values[0][0]

    @property
    def keys(self) -> List[str]:
        return list(self.transcripts['id'].values) # type:ignore

    @property
    def text(self)-> List[str]:
        if 'source_text_sentence' in self.transcripts.columns:
            return list(self.transcripts.source_text_sentence.values) # type:ignore
        elif 'asr_sentence' in self.transcripts.columns:
            return list(self.transcripts.asr_sentence.values) # type:ignore
        raise KeyError
    
    def sentences(self) -> List[Sentence]:
        sentences = []
        for key, text in zip(self.keys, self.text):
            if type(text) == str:
                sentences.append(Sentence(text,key))
        return sentences
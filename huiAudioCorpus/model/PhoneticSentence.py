from typing import List
from huiAudioCorpus.utils.ModelToStringConverter import ToString

class PhoneticSentence(ToString):
    def __init__(self, sentence: str, sub_words: List[str]):
        self.sentence = sentence
        self.sub_words = sub_words

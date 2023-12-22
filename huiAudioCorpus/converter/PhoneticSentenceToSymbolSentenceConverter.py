from huiAudioCorpus.model.PhoneticChars import PhoneticChars
from huiAudioCorpus.model.PhoneticSentence import PhoneticSentence
from huiAudioCorpus.model.SymbolSentence import SymbolSentence

class PhoneticSentenceToSymbolSentenceConverter:
    def __init__(self):
        self.symbols = PhoneticChars().chars
        self.symbol_to_id = {s: i for i, s in enumerate(self.symbols)}

    def convert(self, phonetic_sentence: PhoneticSentence):
        sentence = phonetic_sentence.sentence
        symbols = [self.get_id(char) for char in sentence]
        return SymbolSentence(symbols)

    def get_id(self, char):
        return self.symbol_to_id[char] + 1

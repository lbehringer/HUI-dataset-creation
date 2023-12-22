from huiAudioCorpus.model.Sentence import Sentence
from textblob import TextBlob

class StringToSentencesConverter:
    def convert(self, text: str):
        blob = TextBlob(text)
        sentences = [Sentence(str(sentence)) for sentence in blob.sentences]  # type: ignore
        return sentences

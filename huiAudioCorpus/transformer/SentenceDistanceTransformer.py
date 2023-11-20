from huiAudioCorpus.model.Sentence import Sentence
from Levenshtein import distance as LevenshteinDistance

class SentenceDistanceTransformer:

    def transform(self, sentence1: Sentence, sentence2: Sentence):
        """Calculate the base distance between two sentences."""
        baseDistance = self.distanceTwoSentences(sentence1, sentence2)
        return baseDistance

  
    def distanceTwoSentences(self, sentence1: Sentence, sentence2: Sentence):
        if sentence1.wordsCount == 0 or sentence2.wordsCount == 0:
            return 1
        
        # concatenate words in each sentence to form strings without punctuation
        sentenceString1 = "".join(sentence1.wordsWithoutPunct)
        sentenceString2 = "".join(sentence2.wordsWithoutPunct)

        # determine the maximum length of the two sentence strings
        countCharsMax = max(len(sentenceString1) , len(sentenceString2))
        # calculate the Levenshtein distance between the two sentence strings
        diff = LevenshteinDistance(sentenceString1, sentenceString2)
        # normalize
        distance = diff / countCharsMax
        return distance

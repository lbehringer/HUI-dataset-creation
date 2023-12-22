from huiAudioCorpus.model.Sentence import Sentence
from Levenshtein import distance as levenshtein_distance

class SentenceDistanceTransformer:

    def transform(self, sentence1: Sentence, sentence2: Sentence):
        """Calculate the base distance between two sentences."""
        base_distance = self.distance_two_sentences(sentence1, sentence2)
        return base_distance

  
    def distance_two_sentences(self, sentence1: Sentence, sentence2: Sentence):
        if sentence1.words_count == 0 or sentence2.words_count == 0:
            return 1
        
        # concatenate words in each sentence to form strings without punctuation
        sentence_string1 = "".join(sentence1.words_without_punct)
        sentence_string2 = "".join(sentence2.words_without_punct)

        # determine the maximum length of the two sentence strings
        count_chars_max = max(len(sentence_string1), len(sentence_string2))
        # calculate the Levenshtein distance between the two sentence strings
        diff = levenshtein_distance(sentence_string1, sentence_string2)
        # normalize
        distance = diff / count_chars_max
        return distance

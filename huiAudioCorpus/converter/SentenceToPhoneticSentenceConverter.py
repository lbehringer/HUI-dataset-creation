from typing import List
from nltk.sem.evaluate import Error
from huiAudioCorpus.model.Sentence import Sentence
from huiAudioCorpus.model.PhoneticSentence import PhoneticSentence
import pandas as pd

class SentenceToPhoneticSentenceConverter:
    def __init__(self, library_path: str, use_emphasis: bool = True):
        self.library = self.create_library(library_path)
        self.use_emphasis = use_emphasis

    def convert(self, sentence: Sentence):
        words = sentence.words
        ipa_words, sub_words = self.transform_sentences_to_ipa(words)
        ipa_text = ' '.join(ipa_words)
        ipa_text = self.remove_emphasis(ipa_text)
        return PhoneticSentence(ipa_text, sub_words)

    def create_library(self, library_path: str):
        point_library = pd.DataFrame({
            "text": [",", ".", "?", "-", ";", "!", ":", "'", "s", "ste", "(", ")", ">", "<", '›', '‹', 'é', 'è', '&'],
            "ipa": [",", ".", "?", ",", ",", "!", ":", "'", "s", "stə", ",", ",", "'", "'", "'", "'", 'e', 'e', 'ʊnt']
        })
        library = pd.read_csv(library_path, keep_default_na=False)

        library_lower_case = library.copy(deep=True)
        library_lower_case['text'] = library_lower_case['text'].apply(str.lower)
        library = library.append(point_library)
        library = library.append(library_lower_case)

        library.set_index('text', inplace=True)
        library.sort_index(inplace=True)
        return library

    def transform_sentences_to_ipa(self, words: List[str]):
        ipa_words: List[str] = []
        sub_words: List[str] = []
        index = 0
        while index < len(words):
            word = words[index]
            remaining_words = words[index:]
            count_multi_words, multi_words, multi_word = self.find_multiword_ipa(remaining_words)
            if count_multi_words > 0 and multi_words is not None:
                index += count_multi_words
                sub_words.append(multi_word)
                ipa_words.append(multi_words)
                continue
            ipa, sub_word = self.transform_word_to_ipa(word)
            sub_words.append(sub_word)
            ipa_words.append(ipa)
            index += 1
        return ipa_words, sub_words

    def find_multiword_ipa(self, words: List[str]):
        if len(words) < 2:
            return 0, None, ""
        for count in range(5, 1, -1):
            multi_word = ' '.join(words[:count])
            multi_word_ipa = self.get_ipa_from_library(multi_word)
            if multi_word_ipa is not None:
                return count, multi_word_ipa, multi_word
        return 0, None, ""

    def transform_word_to_ipa(self, word: str):
        complete_ipa_left = ''
        complete_ipa_right = ''
        complete_word_left = []
        complete_word_right = []
        while word != '':
            remaining_word_first, ipa_first, first_part = self.find_first_part_in_word(word)
            remaining_word_last, ipa_last, last_part = self.find_last_part_in_word(word)
            if len(remaining_word_last) < len(remaining_word_first):
                complete_ipa_left = ipa_last + complete_ipa_left
                complete_word_left.insert(0, last_part)
                word = remaining_word_last
            else:
                complete_ipa_right = complete_ipa_right + ipa_first
                complete_word_right.append(first_part)
                word = remaining_word_first
        complete_ipa = complete_ipa_right + complete_ipa_left
        complete_word_right.extend(complete_word_left)
        complete_words = '|'.join(complete_word_right)
        return complete_ipa, complete_words

    def find_first_part_in_word(self, word: str):
        for word_part in range(len(word), 0, -1):
            part = word[:word_part]
            ipa = self.get_ipa_from_library(part)
            if ipa is not None:
                remaining_word = word[word_part:]
                return remaining_word, ipa, part
        raise Error('we have no match for single char in library with char: ' + word[0] + 'with full text:' + word)  # pragma: no cover

    def find_last_part_in_word(self, word: str):
        for word_part in range(0, len(word)):
            part = word[word_part:]
            ipa = self.get_ipa_from_library(part)
            if ipa is not None:
                remaining_word = word[:word_part]
                return remaining_word, ipa, part
        raise Error('we have no match for single char in library with char: ' + word[-1])  # pragma: no cover

    def get_ipa_from_library(self, word: str):
        ipa = self.get_ipa_from_library_exact_string(word)
        if ipa is None:
            word = word.lower()
            ipa = self.get_ipa_from_library_exact_string(word)
        return ipa

    def get_ipa_from_library_exact_string(self, word: str):
        if word in self.library.index:
            ipa: str
            ipa = self.library.loc[word].values[0]
            if type(ipa) is not str:
                ipa = ipa[0]
            return ipa
        return None

    def remove_emphasis(self, text: str):
        if self.use_emphasis:
            return text
        without_emphasis = text.replace("ˈ", "")
        return without_emphasis

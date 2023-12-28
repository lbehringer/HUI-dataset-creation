from os import error
from textblob import TextBlob
from huiAudioCorpus.utils.ModelToStringConverter import ToString
from typing import List, Union
from string import punctuation
import re

class Sentence(ToString):
    """
    Represents a sentence and provides methods for processing and analyzing it
    Params:
        sentence (str): The input sentence
        id (str): An optional identifier for the sentence
    """

    def __init__(self, sentence: str, id: str = ''):
        sentence = self.clean_spaces(sentence)
        # clean spaces around punctuation in the sentence
        sentence = self.clean_spaces_punctuation(sentence)
        if " ' " in sentence:
            sentence = self.postprocess_split_contractions(sentence)


        self.sentence = sentence
        self.id = id

        text_blob = TextBlob(self.sentence.replace('.', ' . '))
        self.words = self.generate_words(text_blob)

        # remove punctuation from `words` and lower-case all elements
        self.words_without_punct: List[str] = [word.lower() for word in text_blob.words] # type: ignore

        # get words and keep lower/upper case
        self.words_without_punct_and_cased: List[str] = [word for word in text_blob.words] # type: ignore

        self.words_count = len(self.words_without_punct)
        self.char_count = len(self.sentence)
        self.words_matching_with_punct = self.generate_words_matching_with_punct(self.words, self.words_without_punct)
        self.raw_chars = "".join(self.words_without_punct)


    def postprocess_split_contractions(self, toks: Union[List, str]):
        """Recombine contractions that were split by TextBlob (e.g. `don ' t` -> `don't`).
        Returns the postprocessed string."""
        if isinstance(toks, str):
             toks = toks.split()
        toks_out = []
        while len(toks) > 2:
                trigram = toks[:3]
                if trigram[1] == "'" and (len(trigram[0]) == 1 or len(trigram[2]) == 1):
                        toks_out.append("".join(trigram))
                        toks = toks[3:]
                else:
                        toks_out.append(trigram[0])
                        toks = toks[1:]
        toks_out.extend(toks)
        toks_out = " ".join(toks_out)
        return toks_out

    def generate_words(self, text_blob:TextBlob):
        """
        Generate a list of words from a TextBlob object.

        Params:
            text_blob (TextBlob): TextBlob object representing the sentence

        Returns:
            List[str]: list of words
        """        

        words = list(text_blob.tokenize())
        for idx, word in enumerate(words):
            # we need to double-check that punctuation was correctly split (TextBlob.tokenize() seems to have issues with hyphen)
            if len(word) > 1:
                # search for punctuation at the beginning of string which was not correctly tokenized
                if re.search(r"^[\!\"#\$%&\'\(\)\*\+,\-\.\/:;<=>\?@\[\\\]\^_`{\|}~]", word):
                    words[idx:idx+1] = (word[0], word[1:])
                # search for punctuation at the end  of string
                if re.search(r"[\!\"#\$%&\'\(\)\*\+,\-\.\/:;<=>\?@\[\\\]\^_`{\|}~]$", word):
                    words[idx:idx+1] = (word[:-1], word[-1])     
        return words
    
    def __getitem__(self, k):
        """
        Get an item from `words_matching_with_punct` at index k.

        Params:
            k: the index of the item to retrieve

        Returns:
            Sentence: a new Sentence object
        """
        return Sentence(" ".join(self.words_matching_with_punct[k]))

    def generate_words_matching_with_punct(self, words:List[str], words_without_punct: List[str]):
        """
        Generate words matching with punctuation.

        Params:
            words (List[str]): list of words
            words_without_punct (List[str]): list of words without punctuation

        Returns:
            word_matching (List[str]): list of words matching with punctuation
        """        
        word_matching = []
        word_pointer = 0
        for word in words:
            if word_pointer < len(words_without_punct) and words_without_punct[word_pointer] == word.lower():
                word_pointer+=1
                word_matching.append(word)
            else:
                if len(word_matching) == 0:
                    continue
                # if the last element of word_matching has more than 1000 characters, there is something wrong
                if len(word_matching[-1]) > 1000:
                    print(word_matching[-1])
                    raise Exception("Problems during creation of word matchings.")
                word_matching[-1]+=' ' + word
        return word_matching


    def clean_spaces(self, text: str):
        """Collapse multiple spaces to one space."""
        text =  text.replace('  ', ' ').replace('  ',' ').replace('  ',' ').replace('  ',' ')
        return text

    def clean_spaces_punctuation(self, text: str):
        """
        Clean spaces around punctuation in the text.

        Params:
            text (str): input text

        Returns:
            text (str): text with cleaned spaces around punctuation
        """        
        punctuations = '.,;?!:"'
        # punctuations_with_preceding_space = '-()'
        # add trailing space
        for char in punctuations:
            text = text.replace(char, char+' ')
        # remove preceding space
        for char in punctuations:
            text = text.replace(' ' + char,char)
        # # add preceding space
        # for char in punctuations_with_preceding_space:
        #     text = text.replace(char, ' '+char)
        # # add trailing space
        # for char in punctuations_with_preceding_space:
        #     text = text.replace(char, char+' ')

        text = text.replace('  ', ' ').replace('  ', ' ')
        return text.strip()

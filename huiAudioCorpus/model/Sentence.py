from os import error
from textblob import TextBlob
from huiAudioCorpus.utils.ModelToStringConverter import ToString
from typing import List

class Sentence(ToString):
    """
    Represents a sentence and provides methods for processing and analyzing it
    Params:
        sentence (str): The input sentence
        id (str): An optional identifier for the sentence
    """

    def __init__(self, sentence: str, id: str = ''):
        sentence = self.cleanSpaces(sentence)
        # clean spaces around punctuation in the sentence
        sentence = self.cleanSpacesPunctuation(sentence)

        self.sentence = sentence
        self.id = id

        textBlob = TextBlob(self.sentence.replace('.',' . ') )
        self.words = self.generateWords(textBlob)

        # remove punctuation from `words` and lower-case all elements
        self.wordsWithoutPunct: List[str] = [word.lower() for word in textBlob.words] # type: ignore

        # get words and keep lower/upper case
        self.wordsWithoutPunctAndCased: List[str] = [word for word in textBlob.words] # type: ignore

        self.wordsCount = len(self.wordsWithoutPunct)
        self.charCount = len(self.sentence)
        self.wordsMatchingWithChars = self.generateWordsMatchingWithChars(self.words, self.wordsWithoutPunct)
        self.rawChars = "".join(self.wordsWithoutPunct)

    def generateWords(self, textBlob:TextBlob):
        """
        Generate a list of words from a TextBlob object.

        Params:
            textBlob (TextBlob): TextBlob object representing the sentence

        Returns:
            List[str]: list of words
        """        

        words = list(textBlob.tokenize())
        return words
    
    def __getitem__(self, k):
        """
        Get an item from `wordsMatchingWithChars` at index k.

        Params:
            k: the index of the item to retrieve

        Returns:
            Sentence: a new Sentence object
        """
        return Sentence(" ".join(self.wordsMatchingWithChars[k]))

    def generateWordsMatchingWithChars(self, words:List[str], wordsWithoutPunct: List[str]):
        """
        Generate words matching with characters.

        Params:
            words (List[str]): list of words
            wordsWithoutPunct (List[str]): list of words without punctuation

        Returns:
            wordMatching (List[str]): list of words matching with characters
        """        
        wordMatching = []
        wordPointer = 0
        for word in words:
            if wordPointer<len(wordsWithoutPunct) and wordsWithoutPunct[wordPointer] == word.lower():
                wordPointer+=1
                wordMatching.append(word)
            else:
                if len(wordMatching) == 0:
                    continue
                # if the last element of wordMatching is longer than 1000, there is something wrong
                if len(wordMatching[-1]) > 1000:
                    print(wordMatching[-1])
                    raise Exception("Problems during creation of word matchings.")
                wordMatching[-1]+=' ' + word
        return wordMatching


    def cleanSpaces(self, text: str):
        """Collapse multiple spaces to one space."""
        text =  text.replace('  ', ' ').replace('  ',' ').replace('  ',' ').replace('  ',' ')
        return text

    def cleanSpacesPunctuation(self, text: str):
        """
        Clean spaces around punctuation in the text.

        Params:
            text (str): input text

        Returns:
            text (str): text with cleaned spaces around punctuation
        """        
        punctuations = '.,;?!:"'
        punctuations_with_preceding_space = '-()'
        # add trailing space
        for char in punctuations:
            text = text.replace(char, char+' ')
        # remove preceding space
        for char in punctuations:
            text = text.replace(' ' + char,char)
        # add preceding space
        for char in punctuations_with_preceding_space:
            text = text.replace(char, ' '+char)
        # add trailing space
        for char in punctuations_with_preceding_space:
            text = text.replace(char, char+' ')

        text = text.replace('  ', ' ').replace('  ', ' ')
        return text.strip()
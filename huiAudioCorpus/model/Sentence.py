from os import error
from textblob import TextBlob
from huiAudioCorpus.utils.ModelToStringConverter import ToString
from typing import List

class Sentence(ToString):
    def __init__(self, sentence: str, id: str = ''):
        sentence = self.cleanSpaces(sentence)
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
        words = list(textBlob.tokenize())
        return words
    
    def __getitem__(self, k):
        return Sentence(" ".join(self.wordsMatchingWithChars[k]))

    def generateWordsMatchingWithChars(self, words:List[str], wordsWithoutPunct: List[str]):
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
        text =  text.replace('  ', ' ').replace('  ',' ').replace('  ',' ').replace('  ',' ')
        return text

    def cleanSpacesPunctuation(self, text: str):
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
import operator
from nltk.sem.evaluate import Error
from tqdm import tqdm
from huiAudioCorpus.model.SentenceAlignment import SentenceAlignment
from typing import List
from huiAudioCorpus.model.Sentence import Sentence
from huiAudioCorpus.transformer.SentenceDistanceTransformer import SentenceDistanceTransformer
from joblib import Parallel, delayed

# range of words to consider when aligning sentences
WORDRANGE = 40

class AlignSentencesIntoTextCalculator:
    """
    A class for aligning sentences to text based on distance metrics.
    """

    def __init__(self, sentenceDistanceTransformer: SentenceDistanceTransformer):
        self.sentenceDistanceTransformer = sentenceDistanceTransformer

    def calculate(self, original_text: Sentence, sentences_to_align: List[Sentence]):
        """
        Calculate sentence alignments between an original sentence and ASR-generated sentences, evaluate whether alignment is perfect or imperfect, and find missing words.

        Params:
            original_text (Sentence): original text to which sentences should be aligned
            sentences_to_align (List[Sentence]): list of sentences to align

        Returns:
            alignments (List[SentenceAlignment]): list of SentenceAlignment objects representing the alignments
        """

        alignments = self.calculateAlignments(original_text, sentences_to_align)
        alignments = self.evaluateIfPerfektStartAndEnd(alignments, original_text.wordsCount)
        alignments = self.getMissingWordsBetweenAlignments(alignments, original_text)
        return alignments


    def calculateAlignments(self, original_text: Sentence, sentences_to_align: List[Sentence]):
        """
        Calculate sentence alignments based on distance metrics.

        Params:
            original_text (Sentence): original text to which sentences should be aligned
            sentences_to_align (List[Sentence]): list of sentences to align

        Returns:
            alignments (List[SentenceAlignment]): list of SentenceAlignment objects representing the alignments
        """        
        with Parallel(n_jobs=4, batch_size="auto") as parallel:
            alignments:List[SentenceAlignment] = []
            start = 0
            additionalRange = 0
            distance_threshold = 0.2
            for sent in tqdm(sentences_to_align):

                range_start= max(0, start - WORDRANGE - additionalRange)
                range_end = min(range_start + 2*(WORDRANGE + additionalRange) + sent.wordsCount, original_text.wordsCount + 1)

                # Find the best position for the current text within the given range
                if range_end - range_start > 2000:
                    raise Exception('more than 2000 Words in search text')

                (newStart, end), distance = self.bestPosition(parallel, original_text[range_start:range_end], sent, 0, range_end - range_start)
                newStart += range_start
                end += range_start

                align = SentenceAlignment(sent, original_text[newStart: end],newStart, end, distance)

                if distance > distance_threshold:
                    print('*****************')
                    print(f'skip because of too high distance (threshold {distance_threshold}): {sent.id} {distance:.3f}')
                    print('*****************')
                    print(f"Sentence to be aligned:\n{sent.sentence}")
                    print('___________________')
                    print(f"Best alignment:\n{align.alignedText.sentence}")                    
                    print('___________________')
                    print(f"Original text search range:\n{original_text[range_start:range_end].sentence}")
                    print('########################')

                    align.isAboveThreshold = True
                    additionalRange += 30 + sent.wordsCount
                else: 
                    start = end
                    additionalRange= 0

                alignments.append(align)
            return alignments


    def bestPosition(self, parallel: Parallel, original_text: Sentence, sentence_to_align: Sentence, range_start: int, range_end: int):
        """
        Find the best position for aligning a sentence within a given range.

        Params:
            parallel (Parallel): parallel processing object
            original_text (Sentence): original text
            sentence_to_align (Sentence): the sentence to align
            range_start (int): start index of the range
            range_end (int): end index of the range

        Returns:
            bestPosition (Tuple[Tuple[int, int], float]): tuple containing the best start and end positions and the distance
        """        
        startEnds = []
        # Generate possible start and end positions within the given range
        for end in range(range_start, range_end):
            for start in range(max(range_start,end-sentence_to_align.wordsCount-10), end):
                startEnds.append((start, end))

        # Use parallel processing to find positions and distances
        positions = parallel(delayed(self.positionOneSentence)(original_text, sentence_to_align, start, end) for start, end in startEnds)
        #positions = [self.positionOneSentence(original_text, sentence_to_align, start, end) for start, end in startEnds]
        
        # Find the best position based on the minimum distance
        bestPosition = min(positions, key=operator.itemgetter(1)) # type: ignore
        return bestPosition


    def positionOneSentence(self, original_text: Sentence , sentence_to_align: Sentence, start: int, end: int):
        """
        Calculate the distance between a part of the original sentence and the sentence to align.

        Parameters:
            original_text (Sentence): original text
            sentence_to_align (Sentence): the sentence to align
            start (int): start index
            end (int): end index

        Returns:
            Tuple[Tuple[int, int], float]: tuple containing the start and end positions and the distance
        """        
        distance = self.sentenceDistanceTransformer.transform(original_text[start:end], sentence_to_align)
        return [(start, end), distance]


    def evaluateIfPerfektStartAndEnd(self, alignments: List[SentenceAlignment], original_text_length: int):
        """
        Evaluate if the start and end positions of alignments are perfect.

        Params:
            alignments (List[SentenceAlignment]): list of SentenceAlignment objects
            original_text_length (int): length of the original text.

        Returns:
            alignments (List[SentenceAlignment]): The list of SentenceAlignment objects with evaluated perfection.
        """        
        for index, align in enumerate(alignments):
            align.leftIsPerfekt = False
            align.rightIsPerfekt = False
            align.isFirst = index ==0
            align.isLast = index == len(alignments)-1

            # Check if the alignment starts or ends perfectly
            if align.start==0:
                align.leftIsPerfekt=True
            if align.end == original_text_length:
                align.rightIsPerfekt= True

            try:
                if align.start == alignments[index-1].end:
                    align.leftIsPerfekt=True
            except:
                pass
            try:
                if align.end == alignments[index+1].start:
                    align.rightIsPerfekt=True
            except:
                pass
            
            # Determine if the alignment is perfect
            align.isPerfect = (align.leftIsPerfekt or align.isFirst) and (align.rightIsPerfekt or align.isLast) and not align.isAboveThreshold
        return alignments


    def getMissingWordsBetweenAlignments(self, alignments: List[SentenceAlignment], original_text: Sentence):
        """
        Print missing words between non-perfect alignments.

        Params:
            alignments (List[SentenceAlignment]): list of SentenceAlignment objects
            original_text (Sentence): original text
        Returns:
            alignments(List[SentenceAlignment]): unchanged list of SentenceAlignment objects
        """        
        for index, align in enumerate(alignments[:-1]):
            
            # Print missing words between non-perfect alignments
            if not align.rightIsPerfekt:
                print(original_text[align.end:alignments[index+1].start])

        return alignments
import operator
from nltk.sem.evaluate import Error
from tqdm import tqdm
from huiAudioCorpus.model.SentenceAlignment import SentenceAlignment
from typing import List
from huiAudioCorpus.model.Sentence import Sentence
from huiAudioCorpus.transformer.SentenceDistanceTransformer import SentenceDistanceTransformer
from joblib import Parallel, delayed

# range of words to consider when aligning sentences
word_range = 40

class AlignSentencesIntoTextCalculator:
    """
    A class for aligning sentences to text based on distance metrics.
    """

    def __init__(self, sentence_distance_transformer: SentenceDistanceTransformer):
        self.sentence_distance_transformer = sentence_distance_transformer

    def calculate(self, original_text: Sentence, sentences_to_align: List[Sentence]):
        """
        Calculate sentence alignments between an original sentence and ASR-generated sentences, evaluate whether alignment is perfect or imperfect, and find missing words.

        Params:
            original_text (Sentence): original text to which sentences should be aligned
            sentences_to_align (List[Sentence]): list of sentences to align

        Returns:
            alignments (List[SentenceAlignment]): list of SentenceAlignment objects representing the alignments
        """

        alignments = self.calculate_alignments(original_text, sentences_to_align)
        alignments = self.evaluate_if_perfect_start_and_end(alignments, original_text.words_count)
        alignments = self.get_missing_words_between_alignments(alignments, original_text)
        return alignments


    def calculate_alignments(self, original_text: Sentence, sentences_to_align: List[Sentence]):
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
            additional_range = 0
            distance_threshold = 0.2
            max_range_threshold = 2000
            first_alignment_found = False
            max_consecutive_unaligned_sents_threshold = 5
            consecutive_unaligned_sents = 0
            for idx, asr_sent in enumerate(tqdm(sentences_to_align)):
                # Find the best position for the current text within the given range
                range_start = max(0, start - word_range - additional_range)
                range_end = min(range_start + 2 * (word_range + additional_range) + asr_sent.words_count, original_text.words_count + 1)

                # if no alignment below distance threshold is found, move start of search range or raise error
                if range_end - range_start > max_range_threshold:
                    if not first_alignment_found:
                        start = max_range_threshold
                        range_start += max_range_threshold
                        additional_range = 0
                    else:
                        raise Exception(f'more than {max_range_threshold} Words in search text')
                    
                # update start of search range if too many consecutive sentences are above alignment threshold
                if consecutive_unaligned_sents >= max_consecutive_unaligned_sents_threshold:
                    start = end
                    range_start = start
                    range_end = max(range_start + word_range, range_end) # to avoid search range <= 0
                    
                    at_least_one_below_alignment_threshold = False
                    # while none of sentences_to_align[-max_consecutive_unaligned_sents_threshold:] align, move search range
                    # until at least one of the last sentences aligns below the threshold
                    print("Too many consecutive sentences above alignment threshold.\nMoving search range to align most recent sentences.")
                    while not at_least_one_below_alignment_threshold:
                        for recent_asr_sent in sentences_to_align[idx-max_consecutive_unaligned_sents_threshold:idx]:
                            (new_start, end), distance = self.best_position(parallel, 
                                                                            original_text=original_text[range_start:range_end], 
                                                                            sentence_to_align=recent_asr_sent, 
                                                                            range_start=0, 
                                                                            range_end=range_end - range_start)
                            new_start += range_start
                            end += range_start
                            align = SentenceAlignment(recent_asr_sent, original_text[new_start: end], new_start, end, distance)       

                            if distance <= distance_threshold:
                                at_least_one_below_alignment_threshold = True
                                first_alignment_found = True
                                start = end
                                additional_range = 0
                                consecutive_unaligned_sents = 0
                                break
                            additional_range += 30 + recent_asr_sent.words_count
                        if not at_least_one_below_alignment_threshold:
                            range_start += additional_range
                            range_end = min(range_start + max_range_threshold, range_start + 2 * (word_range + additional_range) + asr_sent.words_count, original_text.words_count + 1)


                (new_start, end), distance = self.best_position(parallel, 
                                                                original_text=original_text[range_start:range_end], 
                                                                sentence_to_align=asr_sent, 
                                                                range_start=0, 
                                                                range_end=range_end - range_start)
                new_start += range_start
                end += range_start

                align = SentenceAlignment(asr_sent, original_text[new_start: end], new_start, end, distance)

                if distance > distance_threshold:
                    print('*****************')
                    print(f'Above distance threshold ({distance_threshold}): {asr_sent.id} {distance:.3f}')
                    print('*****************')
                    print(f"ASR transcript:\n{asr_sent.sentence}")
                    print('___________________')
                    print(f"Best alignment:\n{align.aligned_text.sentence}")                    
                    print('___________________')
                    print(f"Original text search range:\n{original_text[range_start:range_end].sentence}")
                    print('########################')

                    align.is_above_threshold = True
                    additional_range += 30 + asr_sent.words_count
                    consecutive_unaligned_sents += 1
                else: 
                    first_alignment_found = True
                    start = end
                    additional_range = 0
                    consecutive_unaligned_sents = 0

                alignments.append(align)
            return alignments


    def best_position(self, parallel: Parallel, original_text: Sentence, sentence_to_align: Sentence, range_start: int, range_end: int):
        """
        Find the best position for aligning a sentence within a given range.

        Params:
            parallel (Parallel): parallel processing object
            original_text (Sentence): original text
            sentence_to_align (Sentence): the sentence to align
            range_start (int): start index of the range
            range_end (int): end index of the range

        Returns:
            best_position (Tuple[Tuple[int, int], float]): tuple containing the best start and end positions and the distance
        """        
        start_ends = []
        # Generate possible start and end positions within the given range
        for end in range(range_start, range_end):
            for start in range(max(range_start, end - sentence_to_align.words_count - 10), end):
                start_ends.append((start, end))

        # Use parallel processing to find positions and distances
        positions = parallel(delayed(self.position_one_sentence)(original_text, sentence_to_align, start, end) for start, end in start_ends)
        # positions = [self.position_one_sentence(original_text, sentence_to_align, start, end) for start, end in start_ends]
        
        # Find the best position based on the minimum distance
        best_position = min(positions, key=operator.itemgetter(1)) # type: ignore
        return best_position


    def position_one_sentence(self, original_text: Sentence , sentence_to_align: Sentence, start: int, end: int):
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
        distance = self.sentence_distance_transformer.transform(original_text[start:end], sentence_to_align)
        return [(start, end), distance]


    def evaluate_if_perfect_start_and_end(self, alignments: List[SentenceAlignment], original_text_length: int):
        """
        Evaluate if the start and end positions of alignments are perfect.

        Params:
            alignments (List[SentenceAlignment]): list of SentenceAlignment objects
            original_text_length (int): length of the original text.

        Returns:
            alignments (List[SentenceAlignment]): The list of SentenceAlignment objects with evaluated perfection.
        """        
        for index, align in enumerate(alignments):
            align.left_is_perfect = False
            align.right_is_perfect = False
            align.is_first = index == 0
            align.is_last = index == len(alignments) - 1

            # Check if the alignment starts or ends perfectly
            if align.start == 0:
                align.left_is_perfect = True
            if align.end == original_text_length:
                align.right_is_perfect = True

            try:
                if align.start == alignments[index - 1].end:
                    align.left_is_perfect = True
            except:
                pass
            try:
                if align.end == alignments[index + 1].start:
                    align.right_is_perfect = True
            except:
                pass
            
            # Determine if the alignment is perfect
            align.is_perfect = (align.left_is_perfect or align.is_first) and (align.right_is_perfect or align.is_last) and not align.is_above_threshold
        return alignments


    def get_missing_words_between_alignments(self, alignments: List[SentenceAlignment], original_text: Sentence):
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
            if not align.right_is_perfect:
                print(original_text[align.end:alignments[index + 1].start])

        return alignments

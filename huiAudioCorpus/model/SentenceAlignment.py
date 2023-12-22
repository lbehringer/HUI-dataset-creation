from huiAudioCorpus.utils.ModelToStringConverter import ToString
from huiAudioCorpus.model.Sentence import Sentence

class SentenceAlignment(ToString):
    def __init__(self, source_text: Sentence, aligned_text: Sentence, start: int, end: int, distance: float, left_is_perfect: bool = False, right_is_perfect: bool = False, is_first: bool = False, is_last: bool = False, is_perfect: bool = False, is_above_threshold: bool = False):
        self.source_text = source_text
        self.aligned_text = aligned_text
        self.start = start
        self.end = end
        self.distance = distance
        self.left_is_perfect = left_is_perfect
        self.right_is_perfect = right_is_perfect
        self.is_first = is_first
        self.is_last = is_last
        self.is_perfect = is_perfect
        self.is_above_threshold = is_above_threshold

from tqdm import tqdm  
from huiAudioCorpus.converter.ListToHistogramConverter import ListToHistogramConverter
from huiAudioCorpus.converter.ListToStatisticConverter import ListToStatisticConverter
from huiAudioCorpus.persistence.TranscriptsPersistence import TranscriptsPersistence
from huiAudioCorpus.converter.TranscriptsToSentencesConverter import TranscriptsToSentencesConverter
from pandas.core.frame import DataFrame
from collections import Counter
from huiAudioCorpus.model.Sentence import Sentence

class TextStatisticComponent:
    def __init__(self, transcripts_persistence: TranscriptsPersistence, transcripts_to_sentences_converter: TranscriptsToSentencesConverter, list_to_statistic_converter: ListToStatisticConverter, list_to_histogram_converter: ListToHistogramConverter):
        self.transcripts_persistence = transcripts_persistence
        self.transcripts_to_sentences_converter = transcripts_to_sentences_converter
        self.list_to_statistic_converter = list_to_statistic_converter
        self.list_to_histogram_converter = list_to_histogram_converter

    def run(self):
        raw_data = self.load_text_files()
        return self.get_statistic(raw_data)

    def get_statistic(self, raw_data):
        descriptions = ['Words count in audio', 'Chars count in audio']
        ids = ['word_count', 'char_count']
        statistics = {}
        for column in raw_data:
            if column not in ids:
                continue
            statistics[column] = {
                'name': column,
                'statistic': self.list_to_statistic_converter.convert(raw_data[column].tolist()),
                'histogram': self.list_to_histogram_converter.convert(raw_data[column].tolist()),
                'description': descriptions[len(statistics)]
            }

        if 'text' not in raw_data:
            counter = Counter()
            unique_words_with_minimum = {}

        else:
            counter = Counter([word for sentence in tqdm(raw_data['text']) for word in Sentence(sentence).words_without_punct])

            counter_values = counter.values()
            unique_words_with_minimum = {}
            remaining_counts = counter_values
            for min_word_occurrence in tqdm(list(range(1, max(counter_values) + 1))):
                remaining_counts = [count for count in remaining_counts if count >= min_word_occurrence]
                unique_words_with_minimum[min_word_occurrence] = len(remaining_counts)
                if len(remaining_counts) == 1:
                    break

        return statistics, raw_data, counter, unique_words_with_minimum

    def load_text_files(self):
        all_sentences = [sentence for transcripts in tqdm(self.transcripts_persistence.load_all(), total=len(self.transcripts_persistence.get_ids())) for sentence in self.transcripts_to_sentences_converter.convert(transcripts)]
        result = [[sentence.id.split("\\")[-1].split("/")[-1], sentence.words_count, sentence.char_count, sentence.sentence] for sentence in  tqdm(all_sentences)]
        raw_data = DataFrame(result, columns=['id', 'word_count', 'char_count', 'text'])
        raw_data = raw_data.set_index('id')
        return raw_data

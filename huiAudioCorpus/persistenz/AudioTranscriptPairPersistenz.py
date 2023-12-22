from huiAudioCorpus.model.AudioTranscriptPair import AudioTranscriptPair
from huiAudioCorpus.error.MatchingNotFoundError import MatchingNotFoundError
from typing import List
from huiAudioCorpus.converter.TranscriptsToSentencesConverter import TranscriptsToSentencesConverter
from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.persistenz.TranscriptsPersistenz import TranscriptsPersistenz

class AudioTranscriptPairPersistenz:

    def __init__(self, audio_persistenz: AudioPersistenz, transcripts_persistenz: TranscriptsPersistenz, transcripts_to_sentences_converter: TranscriptsToSentencesConverter, check_for_consistency: bool = True):
        self.audio_persistenz = audio_persistenz
        self.transcripts_persistenz = transcripts_persistenz
        self.transcripts_to_sentences_converter = transcripts_to_sentences_converter

    def load(self, audio_id: str, sentence_id: str):
        audio = self.audio_persistenz.load(audio_id)
        sentence = self.get_all_sentences()[sentence_id]
        element_pair = AudioTranscriptPair(sentence, audio)
        return element_pair

    def get_ids(self, check_for_consistency=True):
        audio_ids = self.audio_persistenz.get_ids()
        audio_names = self.audio_persistenz.get_names()
        sentences_ids = list(self.get_all_sentences().keys())

        if check_for_consistency:
            self.check_ids(audio_names, sentences_ids)
        else:
            audio_ids, audio_names, sentences_ids = self.remove_nonexistent_ids(audio_ids, audio_names, sentences_ids)

        ids = self.sort_ids(audio_ids, audio_names, sentences_ids)
        return ids

    def sort_ids(self, audio_ids, audio_names, sentences_ids):
        zipped_audios = list(zip(audio_ids, audio_names))
        zipped_audios.sort(key=lambda x: x[1])
        audio_ids = [element[0] for element in zipped_audios]
        sentences_ids.sort()
        return list(zip(audio_ids, sentences_ids))

    def load_all(self, check_for_consistency=True):
        ids = self.get_ids(check_for_consistency)
        for audio_id, sentence_id in ids:
            yield self.load(audio_id, sentence_id)

    def get_all_sentences(self):
        transcripts = list(self.transcripts_persistenz.load_all())
        sentences = [sentence for transcript in transcripts for sentence in self.transcripts_to_sentences_converter.convert(transcript)]
        sentence_dict = {sentence.id: sentence for sentence in sentences}
        return sentence_dict

    def check_ids(self, audio_ids: List[str], sentence_ids: List[str]):
        missing_audio_ids = [id for id in sentence_ids if not id in audio_ids]
        missing_sentence_ids = [id for id in audio_ids if not id in sentence_ids]
        if missing_audio_ids or missing_sentence_ids:
            raise MatchingNotFoundError(missing_audio_ids, missing_sentence_ids, 'audioFiles', 'Transcripts')

    def remove_nonexistent_ids(self, audio_ids: List[str], audio_names: List[str], sentence_ids: List[str]):
        audio_ids = [id for id, name in zip(audio_ids, audio_names) if name in sentence_ids]
        audio_names = [name for name in audio_names if name in sentence_ids]
        sentence_ids = [id for id in sentence_ids if id in audio_names]
        return audio_ids, audio_names, sentence_ids

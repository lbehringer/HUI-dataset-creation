from huiAudioCorpus.model.Transcripts import Transcripts
from pandas.core.frame import DataFrame
from huiAudioCorpus.model.Sentence import Sentence
from huiAudioCorpus.calculator.AlignSentencesIntoTextCalculator import AlignSentencesIntoTextCalculator
from huiAudioCorpus.persistenz.TranscriptsPersistenz import TranscriptsPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker

class Step5AlignText:

    def __init__(self, save_path: str, align_sentences_into_text_calculator: AlignSentencesIntoTextCalculator, transcripts_persistenz: TranscriptsPersistenz, text_to_align_path: str):
        self.save_path = save_path
        self.align_sentences_into_text_calculator = align_sentences_into_text_calculator
        self.transcripts_persistenz = transcripts_persistenz
        self.text_to_align_path = text_to_align_path

    def run(self):
        done_marker = DoneMarker(self.save_path)
        result = done_marker.run(self.script, delete_folder=False)
        return result

    def script(self):
        # load (normalized) ASR-generated transcripts (from step 4_1)
        transcripts = list(self.transcripts_persistenz.load_all())
        sentences = transcripts[0].sentences()

        # load prepared source text (from step 3_1)
        with open(self.text_to_align_path, 'r', encoding='utf8') as f:
            input_text = f.read()
        input_sentence = Sentence(input_text)

        alignments = self.align_sentences_into_text_calculator.calculate(input_sentence, sentences)

        # print alignments that are kept despite not being perfect
        not_perfect_alignments = [align for align in alignments if not align.is_perfect and not align.is_above_threshold]
        for align in not_perfect_alignments:
            print('------------------')
            print(align.source_text.id)
            print(f"Transcribed text which was aligned:\n{align.aligned_text.sentence}")
            print(f"Source text: {align.source_text.sentence}")
            print(f"Left alignment perfect: {align.left_is_perfect}")
            print(f"Right alignment perfect: {align.right_is_perfect}")
            print(f"Distance: {align.distance}")

        print("not_perfect_alignments Percent", len(not_perfect_alignments) / len(alignments) * 100)

        results = [[align.source_text.id, align.aligned_text.sentence, align.source_text.sentence, align.distance] for align in alignments if align.is_perfect]
        csv = DataFrame(results)
        transcripts = Transcripts(csv, 'transcripts', 'transcripts')
        self.transcripts_persistenz.save(transcripts)

        results_not_perfect = [[align.source_text.id, align.aligned_text.sentence, align.source_text.sentence, align.distance] for align in alignments if not align.is_perfect]
        csv = DataFrame(results_not_perfect)
        transcripts = Transcripts(csv, 'transcripts_not_perfect', 'transcripts_not_perfect')
        self.transcripts_persistenz.save(transcripts)
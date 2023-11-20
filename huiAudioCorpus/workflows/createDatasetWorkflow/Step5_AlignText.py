from huiAudioCorpus.model.Transcripts import Transcripts
from pandas.core.frame import DataFrame
from huiAudioCorpus.model.Sentence import Sentence
from huiAudioCorpus.calculator.AlignSentencesIntoTextCalculator import AlignSentencesIntoTextCalculator
from huiAudioCorpus.persistenz.TranscriptsPersistenz import TranscriptsPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker

class Step5_AlignText:

    def __init__(self, savePath: str, alignSentencesIntoTextCalculator: AlignSentencesIntoTextCalculator, transcriptsPersistenz: TranscriptsPersistenz, textToAlignPath: str):
        self.savePath = savePath
        self.alignSentencesIntoTextCalculator = alignSentencesIntoTextCalculator
        self.transcriptsPersistenz = transcriptsPersistenz
        self.textToAlignPath = textToAlignPath

    def run(self):
        doneMarker = DoneMarker(self.savePath)
        result = doneMarker.run(self.script, deleteFolder=False)
        return result

    def script(self):
        # load (normalized) ASR-generated transcripts (from step 4_1)
        transcripts = list(self.transcriptsPersistenz.loadAll())
        sentences = transcripts[0].sentences()

        # load prepared source text (from step 3_1)
        with open(self.textToAlignPath, 'r', encoding='utf8') as f:
            inputText = f.read()
        inputSentence = Sentence(inputText)
        
        alignments = self.alignSentencesIntoTextCalculator.calculate(inputSentence, sentences)

        # print alignments that are kept despite not being perfect
        notPerfektAlignments = [align for align in alignments if not align.isPerfect and not align.isAboveThreshold]
        for align in notPerfektAlignments:
            print('------------------')
            print(align.sourceText.id)
            print(f"Transcribed text which was aligned:\n{align.alignedText.sentence}")
            print(f"Source text: {align.sourceText.sentence}")
            print(f"Left alignment perfect: {align.leftIsPerfekt}")
            print(f"Right alignment perfect: {align.rightIsPerfekt}")
            print(f"Distance: {align.distance}")

        print("notPerfektAlignments Percent",len(notPerfektAlignments)/len(alignments)*100)

        results = [[align.sourceText.id, align.alignedText.sentence, align.sourceText.sentence, align.distance] for align in alignments if align.isPerfect]
        csv =  DataFrame(results)
        transcripts = Transcripts(csv, 'transcripts', 'transcripts')
        self.transcriptsPersistenz.save(transcripts)

        resultsNotPerfect = [[align.sourceText.id, align.alignedText.sentence, align.sourceText.sentence, align.distance] for align in alignments if not align.isPerfect]
        csv =  DataFrame(resultsNotPerfect)
        transcripts = Transcripts(csv, 'transcriptsNotPerfect', 'transcriptsNotPerfect')
        self.transcriptsPersistenz.save(transcripts)
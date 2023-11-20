from typing import List
from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.transformer.AudioSamplingRateTransformer import AudioSamplingRateTransformer
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.model.Sentence import Sentence
import numpy as np
from tqdm import tqdm
import whisper
# from whisper.tokenizer import get_tokenizer
# from huiAudioCorpus.utils.whisper_utils import get_language_code

try:
    from deepspeech import Model
except:
    print('failed to load deepspeech, if you need it, try to install it')

from huiAudioCorpus.sttInference import deepspeechModel

class AudioToSentenceConverter:
    def __init__(self, use_whisper=True, whisper_decode_lang="fr"):
        self.model = None
        self.use_whisper = use_whisper
        if self.use_whisper:
            self.whisper_sr = 16000
            self.whisper_sr_transformer = AudioSamplingRateTransformer(targetSamplingRate=self.whisper_sr)
            self.decode_lang = whisper_decode_lang
            # self.tokenizer = get_tokenizer(multilingual=True, language=whisper_decode_lang)
            # get number tokens so we can suppress them to enforce literal transcription (cf. https://github.com/openai/whisper/discussions/1041)
            # this helps with getting correct transcriptions for languages in which numbers can be pronounced in various ways
            # e.g. French 1848: `dix-huit cent quarante-huit` or `mille huit cent quarante-huit`
            # TODO: Currently not working, numeric is omitted entirely instead!
            # self.number_tokens = [
            #     i for i in range(self.tokenizer.eot) if all(c in "0123456789" for c in self.tokenizer.decode([i]).strip())
            # ]            
        else:
            self.modelPath = deepspeechModel.__path__[0]
        

    def convert(self, audio: Audio, samplingRate:int = 15000):
        if self.use_whisper:
            if self.model is None:
                print("Loading Whisper model")
                self.model = whisper.load_model("small") # choices: tiny (~1GB VRAM), base (~1GB), small (~2GB), medium (~5GB), large (~10GB)
            audioSamplingRateTransformer = self.whisper_sr_transformer
            audioSampled = audioSamplingRateTransformer.transform(audio)
            # decode_options = {"language": self.decode_lang, "suppress_tokens": [-1] + self.number_tokens}
            decode_options = {"language": self.decode_lang}
            transcript = self.model.transcribe(audioSampled.timeSeries, **decode_options)["text"]
        else:
            if self.model is None:
                self.model, self.samplingRate = self.loadDeepspeech(self.modelPath)
            audioSamplingRateTransformer = AudioSamplingRateTransformer(self.samplingRate)
            audioSampled = audioSamplingRateTransformer.transform(audio)
            timeSeries =  audioSampled.timeSeries
            timeSeries /= 1.414
            timeSeries *= 32767
            audioNumpy = timeSeries.astype(np.int16)

            transcript = self.model.stt(audioNumpy)

        sentence = Sentence(transcript, audio.id)
        return sentence

    def loadDeepspeech(self, modelPath: str):
        model = Model(modelPath+"/output_graph.pb")
        model.enableExternalScorer(modelPath+"/kenlm.scorer")
        desiredSamplingRate = model.sampleRate()
        return model, desiredSamplingRate


if __name__ == "__main__":
    import librosa
    path = '/media/ppuchtler/LangsameSSD/Projekte/textToSpeech/datasetWorkflow/Step2_SplitAudio/audio/'
    
    addAudio = AudioPersistenz(path).load('acht_gesichter_am_biwasee_01_f000177')
    audio = AudioPersistenz(path).load('acht_gesichter_am_biwasee_01_f000077')

    audio = AudioPersistenz(path).load('acht_gesichter_am_biwasee_01_f000030')
    audio1 = AudioPersistenz(path).load('acht_gesichter_am_biwasee_01_f000105')
    audio = AudioPersistenz(path).load('acht_gesichter_am_biwasee_01_f000166')

    #audioRemove = AudioPersistenz(path).load('acht_gesichter_am_biwasee_01_f000001')
    #audio = AudioAddSilenceTransformer(10, 10).transform(audio)
    #audio = audio + audio

    converter = AudioToSentenceConverter() 
    transcript = converter.convert(addAudio +audio + addAudio)

    print(transcript.sentence)
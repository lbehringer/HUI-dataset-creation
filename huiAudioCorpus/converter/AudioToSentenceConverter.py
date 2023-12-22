from typing import List
from huiAudioCorpus.persistence.AudioPersistence import AudioPersistence
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
            self.model_path = deepspeechModel.__path__[0]
        

    def convert(self, audio: Audio, sampling_rate:int = 15000):
        if self.use_whisper:
            if self.model is None:
                print("Loading Whisper model")
                self.model = whisper.load_model("small") # choices: tiny (~1GB VRAM), base (~1GB), small (~2GB), medium (~5GB), large (~10GB)
            audio_sampling_rate_transformer = self.whisper_sr_transformer
            audio_sampled = audio_sampling_rate_transformer.transform(audio)
            # decode_options = {"language": self.decode_lang, "suppress_tokens": [-1] + self.number_tokens}
            decode_options = {"language": self.decode_lang}
            transcript = self.model.transcribe(audio_sampled.time_series, **decode_options)["text"]
        else:
            if self.model is None:
                self.model, self.sampling_rate = self.load_deepspeech(self.model_path)
            audio_sampling_rate_transformer = AudioSamplingRateTransformer(self.sampling_rate)
            audio_sampled = audio_sampling_rate_transformer.transform(audio)
            time_series =  audio_sampled.time_series
            time_series /= 1.414
            time_series *= 32767
            audio_numpy = time_series.astype(np.int16)

            transcript = self.model.stt(audio_numpy)

        sentence = Sentence(transcript, audio.id)
        return sentence

    def load_deepspeech(self, model_path: str):
        model = Model(model_path+"/output_graph.pb")
        model.enable_external_scorer(model_path+"/kenlm.scorer")
        desired_sampling_rate = model.sample_rate()
        return model, desired_sampling_rate


if __name__ == "__main__":
    import librosa
    path = '/media/ppuchtler/LangsameSSD/Projekte/textToSpeech/datasetWorkflow/Step2_SplitAudio/audio/'
    
    add_audio = AudioPersistence(path).load('acht_gesichter_am_biwasee_01_f000177')
    audio = AudioPersistence(path).load('acht_gesichter_am_biwasee_01_f000077')

    audio = AudioPersistence(path).load('acht_gesichter_am_biwasee_01_f000030')
    audio1 = AudioPersistence(path).load('acht_gesichter_am_biwasee_01_f000105')
    audio = AudioPersistence(path).load('acht_gesichter_am_biwasee_01_f000166')

    #audio_remove = AudioPersistence(path).load('acht_gesichter_am_biwasee_01_f000001')
    #audio = AudioAddSilenceTransformer(10, 10).transform(audio)
    #audio = audio + audio

    converter = AudioToSentenceConverter() 
    transcript = converter.convert(add_audio + audio + add_audio)

    print(transcript.sentence)

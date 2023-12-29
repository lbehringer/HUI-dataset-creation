from typing import List
from huiAudioCorpus.persistence.AudioPersistence import AudioPersistence
from huiAudioCorpus.transformer.AudioSamplingRateTransformer import AudioSamplingRateTransformer
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.model.Sentence import Sentence
import numpy as np
from tqdm import tqdm
import whisper
# from whisper.tokenizer import get_tokenizer
from huiAudioCorpus.utils.whisper_utils import get_language_code


class AudioToSentenceConverter:
    def __init__(self, language):
        self.model = None
        self.whisper_sr = 16000
        self.whisper_sr_transformer = AudioSamplingRateTransformer(target_sampling_rate=self.whisper_sr)
        self.language = get_language_code(language)
        # self.tokenizer = get_tokenizer(multilingual=True, language=whisper_decode_lang)
        # get number tokens so we can suppress them to enforce literal transcription (cf. https://github.com/openai/whisper/discussions/1041)
        # this helps with getting correct transcriptions for languages in which numbers can be pronounced in various ways
        # e.g. French 1848: `dix-huit cent quarante-huit` or `mille huit cent quarante-huit`
        # TODO: Currently not working, numeric is omitted entirely instead!
        # self.number_tokens = [
        #     i for i in range(self.tokenizer.eot) if all(c in "0123456789" for c in self.tokenizer.decode([i]).strip())
        # ]            
        

    def convert(self, audio: Audio):
        if self.model is None:
            print("Loading Whisper model")
            self.model = whisper.load_model("small") # choices: tiny (~1GB VRAM), base (~1GB), small (~2GB), medium (~5GB), large (~10GB)
        audio_sampling_rate_transformer = self.whisper_sr_transformer
        audio_sampled = audio_sampling_rate_transformer.transform(audio)
        # decode_options = {"language": self.decode_lang, "suppress_tokens": [-1] + self.number_tokens}
        decode_options = {"language": self.language}
        transcript = self.model.transcribe(audio_sampled.time_series, **decode_options)["text"]
        sentence = Sentence(transcript, audio.id)
        return sentence


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

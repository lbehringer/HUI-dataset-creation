from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.transformer.AudioSamplingRateTransformer import AudioSamplingRateTransformer
from huiAudioCorpus.transformer.AudioLoudnessTransformer import AudioLoudnessTransformer
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import torch
from pprint import pprint
from tqdm import tqdm


class QA2_HifiSNR:
    def __init__(
            self, 
            audio_persistenz: AudioPersistenz, 
            audio_sr_transformer: AudioSamplingRateTransformer,
            audio_loudness_transformer: AudioLoudnessTransformer,
            save_path: str, 
            book_name: str,
            seconds_to_analyze=30,
            target_sr=16000):
        self.audio_persistenz = audio_persistenz
        self.save_path = save_path
        self.book_name = book_name
        self.seconds_to_analyze = seconds_to_analyze
        self.audio_sr_transformer = audio_sr_transformer
        self.audio_loudness_transformer = audio_loudness_transformer
        self.target_sr = target_sr
        self.audio_sr_transformer.targetSamplingRate = target_sr # silero requires 8kHz or 16kHz
        self.vad_model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad',
                                model='silero_vad',
                                force_reload=False,
                                onnx=False)
        (self.get_speech_timestamps, _, self.read_audio, _, _) = utils
        self.vad_models = dict()

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistenz.loadAll(duration=self.seconds_to_analyze)
        for idx, audio in enumerate(audios):
            audio = self.audio_loudness_transformer.transform(audio)
            audio.speech_timestamps = self.apply_vad(audio)
            audio.silence_timestamps = self.get_silence_timestamps(audio)
            audio = self.set_x_seconds_id(audio, self.book_name, idx+1, self.seconds_to_analyze)
            # TODO: compute SNR for specified frequency bands
            self.audio_persistenz.save(audio)
    
    def set_x_seconds_id(self, audio: Audio, book_name: str, chapter: int, seconds_to_analyze):
        """Assigns an ID to an Audio object, following the format `<book_name>_<chapter>_<seconds_to_analyze>sec`. 
        Returns the Audio with the attributes `id` and `name` (with identical values)."""
        name = f"{book_name}_{chapter:02d}_f{seconds_to_analyze}sec"
        audio.id = name
        audio.name = name
        return audio

    def init_model(self, model):
        pid = multiprocessing.current_process().pid
        model, _ = torch.hub.load(repo_or_dir='snakers4/silero-vad',
                                        model='silero_vad',
                                        force_reload=False,
                                        onnx=False)
        self.vad_models[pid] = model

    def vad_process(self, audio: Audio):
        """Adapted from https://github.com/snakers4/silero-vad/blob/master/examples/parallel_example.ipynb.
        Takes a loaded Audio object as input and splits it into speech chunks.
        Returns
        -------
        speeches: list of dicts
        list containing ends and beginnings of speech chunks (samples or seconds based on return_seconds)"""
        audio = self.audio_sr_transformer.transform(audio=audio)
        
        pid = multiprocessing.current_process().pid
        wav = torch.from_numpy(audio.timeSeries)
        with torch.no_grad():
            # wav = self.read_audio(audio, sampling_rate=self.target_sr)
            return self.get_speech_timestamps(
                wav,
                self.vad_model,
                threshold=0.5,  # speech prob threshold
                sampling_rate=self.target_sr,  # sample rate
                min_speech_duration_ms=300,  # min speech duration in ms
                # max_speech_duration_s=20,  # max speech duration in seconds
                min_silence_duration_ms=400,  # min silence duration
                window_size_samples=512,  # window size
                speech_pad_ms=150,  # speech pad ms
            )       
        
    def apply_vad(self, audio: Audio):
        """Adapted from https://github.com/snakers4/silero-vad/blob/master/examples/parallel_example.ipynb.
        Takes a loaded Audio object as input and splits it into speech chunks.
        Returns
        -------
        speeches: list of dicts
        list containing ends and beginnings of speech chunks (samples or seconds based on return_seconds)"""
        
        audio = self.audio_sr_transformer.transform(audio=audio)
        wav = torch.from_numpy(audio.timeSeries)
        with torch.no_grad():
            # wav = self.read_audio(audio, sampling_rate=self.target_sr)
            return self.get_speech_timestamps(
                wav,
                self.vad_model,
                threshold=0.5,  # speech prob threshold
                sampling_rate=self.target_sr,  # sample rate
                min_speech_duration_ms=300,  # min speech duration in ms
                # max_speech_duration_s=20,  # max speech duration in seconds
                min_silence_duration_ms=400,  # min silence duration
                window_size_samples=512,  # window size
                speech_pad_ms=150,  # speech pad ms
            )       
        
    def get_silence_timestamps(self, audio: Audio):
        """Takes an Audio object as input which has the instance attribute `speech_timestamps`.
        Computes the complementary silence_timestamps.
        Returns a list of dicts containing start and end of each silence segments."""
        silence_timestamps = []
        start = 0
        for idx, speech_chunk in enumerate(audio.speech_timestamps[:-1]):
            if idx == 0:
                start = speech_chunk["end"]
            else:
                end = speech_chunk["start"]
                silence_timestamps.append({"start": start, "end": end})
                start = speech_chunk["end"]
        return silence_timestamps
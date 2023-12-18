from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.transformer.AudioSamplingRateTransformer import AudioSamplingRateTransformer
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.model.Audio import Audio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import torch
from tqdm import tqdm
import numpy as np
import librosa
from wvmos import get_wvmos
from copy import deepcopy

class QA3_WVMOS:
    def __init__(
            self, 
            audio_persistenz: AudioPersistenz, 
            audio_sr_transformer: AudioSamplingRateTransformer,
            save_path: str, 
            target_sr=16000):
        self.audio_persistenz = audio_persistenz
        self.audio_sr_transformer = audio_sr_transformer
        self.save_path = save_path
        self.target_sr = target_sr

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistenz.loadAll()
        for idx, audio in enumerate(audios):
            # only load model if there are any audios to be analyzed
            if idx == 0:
                self.load_vad_model()
                self.wvmos_model = get_wvmos()
            speech_timestamps = self.apply_vad(audio)
            wvmos_scores = []
            if audio.samplingRate != self.target_sr:
                audio_for_wvmos = self.audio_sr_transformer.transform(audio=audio)
            # if more than 1 element, remove first segment to avoid cut-off samples
                if len(speech_timestamps) > 1:
                    if speech_timestamps[0]["start"] < 1000:
                        speech_timestamps.pop(0)
                    # if still more than 1 element, potentially remove last segment
                    if len(speech_timestamps) > 1:
                        if speech_timestamps[-1]["end"] > len(audio_for_wvmos.timeSeries) - 1000:
                            speech_timestamps.pop()
                    
            for idx, segment in enumerate(speech_timestamps):
                score = self.wvmos_model.calculate_signal(audio_for_wvmos.timeSeries[segment["start"]:segment["end"]], audio_for_wvmos.samplingRate)
                # segment_audio = deepcopy(audio_for_wvmos)
                # segment_audio.timeSeries = audio_for_wvmos.timeSeries[segment["start"]:segment["end"]]
                # segment_audio.id = audio_for_wvmos.id + f"_{idx}"
                # self.audio_persistenz.save(segment_audio)
                wvmos_scores.append(score)
            sufficient_wvmos = np.mean(wvmos_scores) > 4 and min(wvmos_scores) > 3.5
            print(f"{audio.id} | sufficient: {sufficient_wvmos} | {wvmos_scores}")
            if sufficient_wvmos:
                self.audio_persistenz.save(audio)
    
    def load_vad_model(self):
        self.vad_model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad',
                            model='silero_vad',
                            force_reload=False,
                            onnx=False)
        (self.get_speech_timestamps, _, self.read_audio, _, _) = utils
        self.vad_models = dict()

    def apply_vad(self, audio: Audio):
        """Adapted from https://github.com/snakers4/silero-vad/blob/master/examples/parallel_example.ipynb.
        Takes a loaded Audio object as input and splits it into speech chunks.
        Returns
        -------
        speech_timestamps: list of dicts
            list containing ends and beginnings of speech chunks (samples or seconds based on return_seconds)
        silence_timestamps: list of dicts
            list containing ends and beginning of each silence chunk
        """
        
        audio = self.audio_sr_transformer.transform(audio=audio)
        wav = torch.from_numpy(audio.timeSeries)
        min_silence_duration_ms = 500
        min_speech_duration_ms = 250
        # get speech_timestamps with a config that will yield at least one speech chunk and at least one silence chunk
        while min_speech_duration_ms > 10 and min_silence_duration_ms > 10:        
            with torch.no_grad():
                speech_timestamps = self.get_speech_timestamps(
                        wav,
                        self.vad_model,
                        threshold=0.5,  # speech prob threshold
                        sampling_rate=self.target_sr,  # sample rate
                        min_speech_duration_ms=min_speech_duration_ms,  # min speech duration in ms
                        max_speech_duration_s=float('inf'),  # max speech duration in seconds
                        min_silence_duration_ms=min_silence_duration_ms,  # min silence duration
                        window_size_samples=512,  # window size
                        speech_pad_ms=50,  # speech pad ms
                    )
                
            if len(speech_timestamps) > 0:
                return speech_timestamps
            else:
                min_speech_duration_ms = int(min_speech_duration_ms / 2)
        return
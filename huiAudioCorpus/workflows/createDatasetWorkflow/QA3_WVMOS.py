from huiAudioCorpus.persistence.AudioPersistence import AudioPersistence
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
import pandas as pd
import os

class QA3_WVMOS:
    def __init__(
            self, 
            audio_persistence: AudioPersistence, 
            audio_sr_transformer: AudioSamplingRateTransformer,
            save_path: str, 
            hifi_qa_load_path: str,
            hifi_qa_save_path: str,
            vad_threshold: float,
            vad_min_speech_duration_ms: float,
            vad_min_silence_duration_ms: float,
            vad_window_size_samples: int,
            vad_speech_pad_ms: int,                    
            target_sr=16000):
        self.audio_persistence = audio_persistence
        self.audio_sr_transformer = audio_sr_transformer
        self.save_path = save_path
        self.hifi_qa_load_path = hifi_qa_load_path
        self.hifi_qa_save_path = hifi_qa_save_path        
        self.vad_threshold = vad_threshold
        self.vad_min_speech_duration_ms = vad_min_speech_duration_ms
        self.vad_min_silence_duration_ms = vad_min_silence_duration_ms
        self.vad_window_size_samples = vad_window_size_samples
        self.vad_speech_pad_ms = vad_speech_pad_ms        
        self.target_sr = target_sr

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistence.load_all()
        hifi_qa_stat_dict = {}

        for idx, audio in enumerate(audios):
            # only load model if there are any audios to be analyzed
            if idx == 0:
                self.load_vad_model()
                self.wvmos_model = get_wvmos()
            speech_timestamps = self.apply_vad(audio)
            wvmos_scores = []
            if audio.sampling_rate != self.target_sr:
                audio_for_wvmos = self.audio_sr_transformer.transform(audio=audio)

            # if more than 1 element, remove first segment to avoid cut-off samples
            if len(speech_timestamps) > 1:
                if speech_timestamps[0]["start"] < 1000:
                    speech_timestamps.pop(0)
                # if still more than 1 element, potentially remove last segment
                if len(speech_timestamps) > 1:
                    if speech_timestamps[-1]["end"] > len(audio_for_wvmos.time_series) - 1000:
                        speech_timestamps.pop()
                    
            for idx, segment in enumerate(speech_timestamps):
                score = self.wvmos_model.calculate_signal(audio_for_wvmos.time_series[segment["start"]:segment["end"]], audio_for_wvmos.sampling_rate)
                # segment_audio = deepcopy(audio_for_wvmos)
                # segment_audio.time_series = audio_for_wvmos.time_series[segment["start"]:segment["end"]]
                # segment_audio.id = audio_for_wvmos.id + f"_{idx}"
                # self.audio_persistence.save(segment_audio)
                wvmos_scores.append(score)
            mean_wvmos_score = np.mean(wvmos_scores)
            min_wvmos_score = min(wvmos_scores)
            hifi_qa_stat_dict[idx] = [audio.id, mean_wvmos_score, min_wvmos_score, wvmos_scores]
            sufficient_wvmos = mean_wvmos_score > 4 and min_wvmos_score > 3.5
            print(f"{audio.id} | sufficient: {sufficient_wvmos} | {wvmos_scores}")
            if sufficient_wvmos:
                self.audio_persistence.save(audio)

        # save hifi_qa stats
        loaded_df = pd.read_csv(self.hifi_qa_load_path, sep="|")
        hifi_qa_df = pd.DataFrame.from_dict(hifi_qa_stat_dict, orient="index", columns=["id", "mean_wvmos_score", "min_wvmos_score", "wvmos_scores"])
        hifi_qa_df = loaded_df.merge(hifi_qa_df, how="outer", on="id")
        os.makedirs(self.save_path, exist_ok=True)
        hifi_qa_df.to_csv(self.hifi_qa_save_path, sep="|", index=False)
    

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
        wav = torch.from_numpy(audio.time_series)

        # get speech_timestamps with a config that will yield at least one speech chunk and at least one silence chunk
        while self.vad_min_speech_duration_ms > 10 and self.vad_min_silence_duration_ms > 10:        
            with torch.no_grad():
                speech_timestamps = self.get_speech_timestamps(
                        wav,
                        self.vad_model,
                        threshold=0.5,  # speech prob threshold
                        sampling_rate=self.target_sr,  # sample rate
                        min_speech_duration_ms=self.vad_min_speech_duration_ms,  # min speech duration in ms
                        max_speech_duration_s=float('inf'),  # max speech duration in seconds
                        min_silence_duration_ms=self.vad_min_silence_duration_ms,  # min silence duration
                        window_size_samples=self.vad_window_size_samples,  # window size
                        speech_pad_ms=self.vad_speech_pad_ms,  # speech pad ms
                    )
                
            if len(speech_timestamps) > 0:
                return speech_timestamps
            else:
                self.vad_min_speech_duration_ms = int(self.vad_min_speech_duration_ms / 2)
        return

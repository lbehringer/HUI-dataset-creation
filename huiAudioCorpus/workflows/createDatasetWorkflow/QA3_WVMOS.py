from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.model.Audio import Audio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import torch
from tqdm import tqdm
import numpy as np
import librosa
from wvmos import get_wvmos

class QA3_WVMOS:
    def __init__(
            self, 
            audio_persistenz: AudioPersistenz, 
            save_path: str, 
            target_sr=16000):
        self.audio_persistenz = audio_persistenz
        self.save_path = save_path
        self.target_sr = target_sr

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistenz.loadAll()
        for idx, audio in enumerate(audios):
            # only load model if there are any audios to be analyzed
            if idx == 0:
                self.wvmos_model = get_wvmos()
            wvmos_score = self.wvmos_model.calculate_signal(audio.timeSeries, audio.samplingRate)
            print(audio.id, wvmos_score)
            if wvmos_score > 4:
                self.audio_persistenz.save(audio)
            
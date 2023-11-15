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
import numpy as np
import librosa
from scipy.signal import butter, lfilter
import matplotlib.pyplot as plt
import soundfile as sf


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
                

            # TODO: generate 4 timeSeries, one for each frequency band that we want to analyze
            # TODO: Is it better to just compute the mel spectrogram, "cut off" all frequencies outside the passband, and then do an inverse mel transform and an inverse STFT?
            print(audio.samplingRate)
            # design band-pass Butterworth filter
            lowcut = 300
            highcut = 4000
            nyquist = 0.5 * audio.samplingRate
            low = lowcut / nyquist
            high = highcut / nyquist
            b, a = butter(N=4, Wn=[low, high], btype="bandpass")
            # apply filter to signal
            filtered_signal = lfilter(b, a, audio.timeSeries)
            t = np.linspace(0, 1, len(audio.timeSeries), endpoint=False)  # Time vector

            # Plot the original and filtered signals
            plt.figure(figsize=(10, 6))
            plt.plot(t, audio.timeSeries, label='Original Signal')
            plt.plot(t, filtered_signal, label='Filtered Signal')
            plt.xlabel('Time (s)')
            plt.ylabel('Amplitude')
            plt.legend()
            plt.savefig("butterworth.png")
            sf.write("original_audio.wav", audio.timeSeries, audio.samplingRate)
            sf.write("filtered_audio.wav", filtered_signal, audio.samplingRate)
    
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

    def get_snr(self, audio: Audio, speech_timestamps: list, silence_timestamps: list):
        """Estimates the signal-plus-noise power in speech segments and the noise power in silence segments, then computes the SNR."""
        # get speech-only audio and silence-only audio
        speech_audio_segments = [audio.timeSeries[ts['start']:ts['end']] for ts in speech_timestamps]
        silence_audio_segments =  [audio.timeSeries[ts['start']:ts['end']] for ts in silence_timestamps]
        speech_audio = np.concatenate(speech_audio_segments)
        silence_audio = np.concatenate(silence_audio_segments)

        # get signal-plus-noise power in speech audio
        speech_power = self.get_average_power(speech_audio)
        # get noise power in silence audio
        silence_power = self.get_average_power(silence_audio)
        # compute SNR
        return speech_power / silence_power

    def get_average_power(self, y: np.ndarray):
        """Calculates the average power of an audio signal.
        Returns
        -------
        average_power (float)
        """
        stft_result = librosa.stft(y)
        magnitude_spec = np.abs(stft_result)
        power_spec = magnitude_spec**2
        power_across_frequency_bins = np.sum(power_spec, axis=0)
        total_power = np.sum(power_across_frequency_bins)
        duration = len(y) / self.target_sr
        average_power = total_power / duration
        return average_power
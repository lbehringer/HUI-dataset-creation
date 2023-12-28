from huiAudioCorpus.persistence.AudioPersistence import AudioPersistence
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
from copy import deepcopy

class QA2_HifiSNR:
    def __init__(
            self, 
            audio_persistence: AudioPersistence, 
            audio_sr_transformer: AudioSamplingRateTransformer,
            audio_loudness_transformer: AudioLoudnessTransformer,
            save_path: str, 
            book_name: str,
            seconds_to_analyze=30,
            target_sr=16000):
        self.audio_persistence = audio_persistence
        self.save_path = save_path
        self.book_name = book_name
        self.seconds_to_analyze = seconds_to_analyze
        self.audio_sr_transformer = audio_sr_transformer
        self.audio_loudness_transformer = audio_loudness_transformer
        self.target_sr = target_sr

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistence.load_all(duration=self.seconds_to_analyze)
        for idx, audio in enumerate(audios):
            # only load VAD model if there are any audios to be analyzed
            if idx == 0:
                self.load_vad_model()
            audio = self.audio_loudness_transformer.transform(audio)
            speech_timestamps, silence_timestamps = self.apply_vad(audio)
            audio = self.set_x_seconds_id(audio, self.book_name, idx+1, self.seconds_to_analyze)
            sufficient_snr = False
            snr_threshold = 32
            frequency_bands = [(100, 1000), (300, 4000), (4000, 10000), (10000, 15000)]

            # TODO: Find a way to identify whether a noise-removal algorithm has been applied

            for lower_freq, upper_freq in frequency_bands:
                # TODO: we could analyze the other frequency bands too
                if lower_freq == 300 and upper_freq == 4000:
                    snr = self.get_snr(
                        audio=audio, 
                        speech_timestamps=speech_timestamps, 
                        silence_timestamps=silence_timestamps, 
                        lower_freq_threshold=lower_freq, 
                        upper_freq_threshold=upper_freq,
                    )
                    print(f"{audio.id} | SNR for freq band {lower_freq}-{upper_freq}: {snr}")
                    if snr >= snr_threshold:
                        sufficient_snr = True

            if sufficient_snr:
                self.audio_persistence.save(audio)
            else:
                print(f"SNR {snr} is < required threshold ({snr_threshold}). Skipping {audio.id}")
            
            """
            for now, we don't store the speech-only or silence-only audio because 
            the "uncut" samples get the highest WVMOS score
            upsample_factor = audio.sampling_rate / self.target_sr
            speech_signal_segments = [audio.time_series[int(ts['start']*upsample_factor):int(ts['end']*upsample_factor)] for ts in speech_timestamps]
            silence_signal_segments =  [audio.time_series[int(ts['start']*upsample_factor):int(ts['end']*upsample_factor)] for ts in silence_timestamps]
            speech_signal = np.concatenate(speech_signal_segments)
            silence_signal = np.concatenate(silence_signal_segments)
            speech_audio = deepcopy(audio)
            silence_audio = deepcopy(audio)
            speech_audio.time_series, speech_audio.id = speech_signal, audio.id+"speech"
            silence_audio.time_series, silence_audio.id = silence_signal, audio.id+"silence"
            self.audio_persistenz.save(speech_audio)
            self.audio_persistenz.save(silence_audio)
            """


            """
            For now, we don't use a butterworth filter because
            we can instead compute the SNR for different frequency bands.
            # generate 4 time_series, one for each frequency band that we want to analyze
            # TODO: Is it better to just compute the mel spectrogram, "cut off" all frequencies outside the passband, and then do an inverse mel transform and an inverse STFT?
            # design band-pass Butterworth filter
            lowcut = 300
            highcut = 4000
            nyquist = 0.5 * audio.sampling_rate
            low = lowcut / nyquist
            high = highcut / nyquist
            b, a = butter(N=4, Wn=[low, high], btype="bandpass")
            # apply filter to signal
            filtered_signal = lfilter(b, a, audio.time_series)
            t = np.linspace(0, 1, len(audio.time_series), endpoint=False)  # Time vector
            # Plot the original and filtered signals
            plt.figure(figsize=(10, 6))
            plt.plot(t, audio.time_series, label='Original Signal')
            plt.plot(t, filtered_signal, label='Filtered Signal')
            plt.xlabel('Time (s)')
            plt.ylabel('Amplitude')
            plt.legend()
            plt.savefig("butterworth.png")
            sf.write("original_audio.wav", audio.time_series, audio.sampling_rate)
            sf.write("filtered_audio.wav", filtered_signal, audio.sampling_rate)
            """
            
    def load_vad_model(self):
        """Load Silero VAD model and corresponding functions"""
        self.vad_model, utils = torch.hub.load(repo_or_dir='snakers4/silero-vad',
                            model='silero_vad',
                            force_reload=False,
                            onnx=False)
        (self.get_speech_timestamps, _, self.read_audio, _, _) = utils
        self.vad_models = dict()

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
        wav = torch.from_numpy(audio.time_series)
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
        speech_timestamps: list of dicts
            list containing ends and beginnings of speech chunks (samples or seconds based on return_seconds)
        silence_timestamps: list of dicts
            list containing ends and beginning of each silence chunk
        """

        audio = self.audio_sr_transformer.transform(audio=audio)
        wav = torch.from_numpy(audio.time_series)
        min_silence_duration_ms = 150
        min_speech_duration_ms = 250
        # get speech_timestamps with a config that will yield at least one speech chunk and at least one silence chunk
        # we assume that a very short chunk (~10ms) for either speech or silence will result in a too low SNR so the sample will be filtered out        
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
                        speech_pad_ms=30,  # speech pad ms
                    )

            silence_timestamps = self.get_silence_timestamps(speech_timestamps)
            if len(speech_timestamps) == 0:
                min_speech_duration_ms = int(min_speech_duration_ms / 2)
            if len(silence_timestamps) == 0:
                min_silence_duration_ms = int(min_silence_duration_ms / 2)
            else:
                return speech_timestamps, silence_timestamps
        return

    def get_silence_timestamps(self, speech_timestamps: list):
        """Takes as input a list of dicts `speech_timestamps`, containing "start" and "end" of each speech segment.
        Computes the complementary silence_timestamps.
        Returns a list of dicts containing start and end of each silence segments."""
        silence_timestamps = []
        start = 0
        for idx, speech_chunk in enumerate(speech_timestamps[:-1]):
            if idx == 0:
                start = speech_chunk["end"]
            else:
                end = speech_chunk["start"]
                silence_timestamps.append({"start": start, "end": end})
                start = speech_chunk["end"]
        return silence_timestamps

    def get_snr(self, audio: Audio, speech_timestamps: list, silence_timestamps: list, lower_freq_threshold: float, upper_freq_threshold: float):
        """Estimates the signal-plus-noise power in speech segments and the noise power in silence segments, then computes the signal-to-noise ratio (SNR)."""
        # get speech-only audio and silence-only audio
        # upsample timestamps to match audio's sampling rate (factor is 1 if they already match)
        upsample_factor = audio.sampling_rate / self.target_sr
        speech_signal_segments = [audio.time_series[int(ts['start']*upsample_factor):int(ts['end']*upsample_factor)] for ts in speech_timestamps]
        silence_signal_segments =  [audio.time_series[int(ts['start']*upsample_factor):int(ts['end']*upsample_factor)] for ts in silence_timestamps]
        speech_signal = np.concatenate(speech_signal_segments)
        silence_signal = np.concatenate(silence_signal_segments)

        # get signal-plus-noise power in speech audio
        speech_power = self.get_average_power(speech_signal, audio.sampling_rate, lower_freq_threshold, upper_freq_threshold)
        # get noise power in silence audio
        silence_power = self.get_average_power(silence_signal, audio.sampling_rate, lower_freq_threshold, upper_freq_threshold)
        # compute SNR
        return speech_power / silence_power

    def get_average_power(self, y: np.ndarray, sr: int, lower_freq_threshold: float, upper_freq_threshold: float):
        """Calculates the average power of an audio signal.
        Returns
        -------
        average_power (float)
        """
        # compute STFT
        stft_result = librosa.stft(y)
        # compute frequency bins corresponding to STFT
        freq_bins = librosa.fft_frequencies(sr=sr)
        # find the indices corresponding to the chosen frequency band (between lower and upper threshold)
        band_indices = np.where((freq_bins >= lower_freq_threshold) & (freq_bins <= upper_freq_threshold))[0]
        # extract the magnitude spectrum for the chosen frequency band
        magnitude_spec = np.abs(stft_result[band_indices, :])
        power_spec = magnitude_spec**2
        power_across_frequency_bins = np.sum(power_spec, axis=0)
        total_power = np.sum(power_across_frequency_bins)
        duration = len(y) / sr
        average_power = total_power / duration
        return average_power
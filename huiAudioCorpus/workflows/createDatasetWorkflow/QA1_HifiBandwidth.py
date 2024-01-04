from huiAudioCorpus.persistence.AudioPersistence import AudioPersistence
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.transformer.AudioLoudnessTransformer import AudioLoudnessTransformer
import librosa
import numpy as np
import pandas as pd
import os

class QA1_HifiBandwidth:
    def __init__(
            self,
            audio_persistence: AudioPersistence,
            save_path: str,
            book_name: str,
            seconds_to_analyze: int,
            analysis_offset: float,
            bandwidth_hz_threshold: int,
            min_db_threshold_wrt_peak: int,
            hifi_qa_load_path: str,
            hifi_qa_save_path: str,
            audio_loudness_transformer: AudioLoudnessTransformer = None,
            ):
        self.audio_persistence = audio_persistence
        self.save_path = save_path
        self.hifi_qa_load_path = hifi_qa_load_path
        self.hifi_qa_save_path = hifi_qa_save_path
        self.book_name = book_name
        self.seconds_to_analyze = seconds_to_analyze
        self.analysis_offset = analysis_offset
        self.audio_loudness_transformer = audio_loudness_transformer
        self.bandwidth_hz_threshold = bandwidth_hz_threshold
        self.min_db_threshold_wrt_peak = min_db_threshold_wrt_peak

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistence.load_all(duration=self.seconds_to_analyze, offset=self.analysis_offset)
        hifi_qa_stat_dict = {}

        for idx, audio in enumerate(audios):
            # only perform loudness normalization if specified in config
            if self.audio_loudness_transformer:
                audio = self.audio_loudness_transformer.transform(audio)
            audio.bandwidth, lowest_hz, highest_hz = self.get_bandwidth(audio)
            hifi_qa_stat_dict[idx] = [audio.id, audio.bandwidth, lowest_hz, highest_hz]
            # # filter out audios which don't meet the minimum bandwidth threshold
            # if audio.bandwidth >= self.bandwidth_hz_threshold:
            #     self.audio_persistence.save(audio)

        # save hifi_qa stats (reader specific)
        loaded_df = pd.read_csv(self.hifi_qa_load_path, sep="|")
        hifi_qa_df = pd.DataFrame.from_dict(hifi_qa_stat_dict, orient="index", columns=["id", "bandwidth", "lowest_hz", "highest_hz"])
        hifi_qa_df = loaded_df.merge(hifi_qa_df, how="outer", on="id")
        os.makedirs(self.save_path, exist_ok=True)
        hifi_qa_df.to_csv(self.hifi_qa_save_path, sep="|", index=False)

    def set_x_seconds_id(self, audio: Audio, book_name: str, chapter: int, seconds_to_analyze):
        """Assigns an ID to an Audio object, following the format `<book_name>_<chapter>_<seconds_to_analyze>sec`. 
        Returns the Audio with the attributes `id` and `name` (with identical values)."""
        name = f"{book_name}_{chapter:02d}_{seconds_to_analyze}sec"
        audio.id = name
        audio.name = name
        return audio

    def get_bandwidth(self, audio: Audio):
        """Computes the bandwidth of a signal by finding the lowest and highest frequencies which are at least -50 dB relative to the peak value of the power spectrogram.
        Returns a tuple of floats bandwidth, lowest Hz, and highest Hz."""
        stft_result = librosa.stft(audio.time_series)

        # compute power spectrogram from magnitude of the STFT result
        power_spec = np.abs(stft_result)**2

        mean_power = np.mean(power_spec)    
        power_spec_db = librosa.amplitude_to_db(power_spec, ref=np.max)

        # get highest frequency which is at least -50 dB (or higher) relative to the peak value (peak value should be <= 0)
        peak_value = np.max(power_spec_db)
        threshold = peak_value + self.min_db_threshold_wrt_peak  # -50 dB level relative to the peak value
        frequencies_above_threshold = np.where(power_spec_db >= threshold)[0]
        if len(frequencies_above_threshold) > 0:
            bandwidth_start = frequencies_above_threshold[0]
            bandwidth_end = frequencies_above_threshold[-1]
            start_hz = librosa.fft_frequencies(sr=audio.sampling_rate)[bandwidth_start]
            end_hz = librosa.fft_frequencies(sr=audio.sampling_rate)[bandwidth_end]
            # TODO: test correct function of bandwidth estimation
            print(f"{audio.id} | Estimated bandwidth: {start_hz:.2f} Hz to {end_hz:.2f} Hz | Mean of power spectrogram: {mean_power:.2f}")
            return end_hz - start_hz, start_hz, end_hz
        else:
            print("No continuous frequency range falls below the specified threshold.")
            return None

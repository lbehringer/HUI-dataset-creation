from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.transformer.AudioLoudnessTransformer import AudioLoudnessTransformer
import librosa
import numpy as np



class QA1_HifiBandwidth:
    def __init__(
            self, 
            audio_persistenz: AudioPersistenz, 
            save_path: str, 
            book_name: str,
            seconds_to_analyze: int,
            bandwidth_hz_threshold: int,
            audio_loudness_transformer: AudioLoudnessTransformer = None,
            ):
        self.audio_persistenz = audio_persistenz
        self.save_path = save_path
        self.book_name = book_name
        self.seconds_to_analyze = seconds_to_analyze
        self.audio_loudness_transformer = audio_loudness_transformer
        self.bandwidth_hz_threshold = bandwidth_hz_threshold

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        audios = self.audio_persistenz.loadAll(duration=self.seconds_to_analyze)
        for idx, audio in enumerate(audios):
            # only perform loudness normalization if specified in config
            if self.audio_loudness_transformer:
                audio = self.audio_loudness_transformer.transform(audio)
            audio.bandwidth = self.get_bandwidth(audio)
            # filter out audios which don't meet the minimum bandwidth threshold
            if audio.bandwidth >= self.bandwidth_hz_threshold:
                audio = self.set_x_seconds_id(audio, self.book_name, idx+1, self.seconds_to_analyze)
                self.audio_persistenz.save(audio)


    def set_x_seconds_id(self, audio: Audio, book_name: str, chapter: int, seconds_to_analyze):
        """Assigns an ID to an Audio object, following the format `<book_name>_<chapter>_<seconds_to_analyze>sec`. 
        Returns the Audio with the attributes `id` and `name` (with identical values)."""
        name = f"{book_name}_{chapter:02d}_{seconds_to_analyze}sec"
        audio.id = name
        audio.name = name
        return audio

    def get_bandwidth(self, audio: Audio):
        """Computes the bandwidth of a signal by finding the lowest and highest frequencies which are at least -50 dB relative to the peak value of the power spectrogram.
        Returns the bandwidth as a float."""
        stft_result = librosa.stft(audio.timeSeries)

        # compute power spectrogram from magnitude of the STFT result
        power_spec = np.abs(stft_result)**2

        mean_power = np.mean(power_spec)    
        power_spec_db = librosa.amplitude_to_db(power_spec, ref=np.max)

        # get highest frequency which is at least -50 dB (or higher) relative to the peak value (peak value should be <= 0)
        peak_value = np.max(power_spec_db)
        threshold = peak_value - 60  # -50 dB level relative to the peak value
        frequencies_above_threshold = np.where(power_spec_db >= threshold)[0]
        if len(frequencies_above_threshold) > 0:
            bandwidth_start = frequencies_above_threshold[0]
            bandwidth_end = frequencies_above_threshold[-1]
            start_hz = librosa.fft_frequencies(sr=audio.samplingRate)[bandwidth_start]
            end_hz = librosa.fft_frequencies(sr=audio.samplingRate)[bandwidth_end]
            # TODO: test correct function of bandwidth estimation
            print(f"{audio.id} | Estimated bandwidth: {start_hz:.2f} Hz to {end_hz:.2f} Hz | Mean of power spectrogram: {mean_power:.2f}")
            return end_hz - start_hz
        else:
            print("No continuous frequency range falls below the specified threshold.")
            return None

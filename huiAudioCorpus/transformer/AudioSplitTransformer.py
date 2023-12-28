from typing import List
import librosa
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.transformer.AudioFadeTransformer import AudioFadeTransformer
import statistics

class AudioSplitTransformer:
    """Described in the HUI paper in section 3.2"""
    def __init__(self, audio_fade_transformer: AudioFadeTransformer, max_audio_duration: float, min_audio_duration: float, silence_duration_in_seconds: float = 0.2):
        self.max_audio_duration = max_audio_duration
        self.min_audio_duration = min_audio_duration
        self.silence_duration_in_seconds = silence_duration_in_seconds
        self.audio_fade_transformer = audio_fade_transformer

    def transform(self, audio: Audio, book_name: str, chapter: int):
        """Splits an Audio (i.e. one book chapter) into Audios within the min/max duration thresholds and assigns them IDs.
        Returns the split Audios as a list."""
        splitted = self.split_with_best_decibel(audio, self.max_audio_duration - self.min_audio_duration)
        splitted = self.merge_audio_to_target_duration(splitted, self.min_audio_duration)
        merged = self.merge_last_audio_if_too_short(splitted, self.min_audio_duration)
        with_ids = self.set_ids(merged, book_name, chapter)
        with_fading = self.fade(with_ids)
        return with_ids

    def split_with_best_decibel(self, audio: Audio, max_audio_duration: float):
        """Determine the highest-possible db threshold for getting audio splits that stay below the maximum audio duration.
        Returns list of non-silent Audio splits below the maximum audio duration."""
        splitted_audio: List[Audio] = []
        max_duration: float = 0
        for silence_decibel in range(70, -20, -5):
            splitted_audio = self.split(audio, silence_decibel)
            max_duration = max([audio.duration for audio in splitted_audio])
            if max_duration < max_audio_duration:
                print(audio.name, 'used DB:', silence_decibel)
                return splitted_audio
        return splitted_audio

    def split(self, audio: Audio, silence_decibel: int):
        """Splits an Audio instance into non-silent intervals, based on a threshold `silence_decibel`. 
        Returns list of non-silent Audio splits derived from the input Audio."""
        frame_length = int(self.silence_duration_in_seconds * audio.sampling_rate)
        splitted = librosa.effects.split(y=audio.time_series, top_db=silence_decibel, frame_length=frame_length, hop_length=int(frame_length/4))
        audios = []
        for i in range(len(splitted)):
            (start, end) = splitted[i]
            is_next_element_available = len(splitted) > i + 1
            if is_next_element_available: 
                (next_start, _) = splitted[i + 1]
                better_end = int(statistics.mean([end, next_start]))
            else:
                better_end = end

            is_previous_element_available = not i == 0
            if is_previous_element_available:
                (_, previous_end) = splitted[i - 1]
                better_start = int(statistics.mean([previous_end, start]))
            else:
                better_start = start

            new_audio = Audio(audio.time_series[better_start:better_end], audio.sampling_rate, 'id', 'name')
            audios.append(new_audio)
        return audios

    def merge_audio_to_target_duration(self, audios: List[Audio], target_duration: float):
        """Merges (concatenates) Audios to get audios which meet the minimum duration requirement.
        Returns the list of concatenated audios."""
        merged_audios: List[Audio] = []

        for audio in audios:
            if len(merged_audios) > 0 and merged_audios[-1].duration < target_duration:
                merged_audios[-1] = merged_audios[-1] + audio
            else:
                merged_audios.append(audio)
        return merged_audios

    def merge_last_audio_if_too_short(self, audios: List[Audio], min_duration: float):
        if audios[-1].duration < min_duration:
            audios[-2] = audios[-2] + audios[-1]
            audios.pop()
        return audios

    def set_ids(self, audios: List[Audio], book_name: str, chapter: int):
        """Assigns an ID to each Audio in a list, following the format `<book_name>_<chapter>_<index>`. 
        Returns the list of Audios with the attributes `id` and `name` (with identical values)."""
        for index, audio in enumerate(audios):
            name = f"{book_name}_{chapter:02d}_f{index+1:06d}"
            audio.id = name
            audio.name = name
        return audios

    def fade(self, audios: List[Audio]):
        return [self.audio_fade_transformer.transform(audio) for audio in audios]


from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.transformer.TranscriptsSelectionTransformer import TranscriptsSelectionTransformer
from huiAudioCorpus.persistence.TranscriptsPersistence import TranscriptsPersistence
from huiAudioCorpus.persistence.AudioPersistence import AudioPersistence
from huiAudioCorpus.transformer.AudioSamplingRateTransformer import AudioSamplingRateTransformer
from tqdm.std import tqdm
import pandas as pd
import os

class Step9_GenerateCleanDataset:

    def __init__(self, save_path: str, info_file: str, audio_persistence: AudioPersistence, transcripts_persistence: TranscriptsPersistence,
                 audio_sampling_rate_transformer: AudioSamplingRateTransformer, transcripts_selection_transformer: TranscriptsSelectionTransformer, filter):
        self.audio_sampling_rate_transformer = audio_sampling_rate_transformer
        self.audio_persistence = audio_persistence
        self.transcripts_persistence = transcripts_persistence
        self.transcripts_selection_transformer = transcripts_selection_transformer
        self.save_path = save_path
        self.info_file = info_file
        self.filter = filter

    def run(self):
        done_marker = DoneMarker(self.save_path)
        result = done_marker.run(self.script, delete_folder=False)
        return result

    def script(self):
        df = pd.read_csv(self.info_file, sep='|', index_col=0)
        book = df.index[0].split("_")[0]
        speaker = df.loc[df.index[0], 'speaker']
        self.transcripts_persistence.load_path = os.path.join(self.transcripts_persistence.load_path, speaker, book)
        self.transcripts_persistence.save_path = os.path.join(self.transcripts_persistence.save_path, speaker, book)
        try:
            df = df.set_index('id')
        except:
            pass

        print('Audios before: ', df.shape[0])
        filtered_audios = self.filter(df)
        print('Audios after: ', filtered_audios.shape[0])
        audios_allowed = filtered_audios.index.tolist()

        self.copy_audio_files(audios_allowed)
        self.copy_and_filter_transcripts(audios_allowed)

    def copy_audio_files(self, audios_allowed):
        count_files = len(self.audio_persistence.get_ids())
        for audio in tqdm(self.audio_persistence.load_all(), total=count_files):
            if audio.name in audios_allowed:
                self.audio_persistence.save(audio)

    def copy_and_filter_transcripts(self, used_audio_file_names):
        for transcripts in tqdm(self.transcripts_persistence.load_all()):
            filtered_transcript = self.transcripts_selection_transformer.transform(transcripts, used_audio_file_names)
            self.transcripts_persistence.save(filtered_transcript)

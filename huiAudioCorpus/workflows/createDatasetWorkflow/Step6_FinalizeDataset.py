from huiAudioCorpus.transformer.TranscriptsSelectionTransformer import TranscriptsSelectionTransformer
import pandas as pd
from huiAudioCorpus.model.Audio import Audio
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.persistenz.AudioPersistenz import AudioPersistenz
from huiAudioCorpus.persistenz.TranscriptsPersistenz import TranscriptsPersistenz
from tqdm import tqdm

class Step6FinalizeDataset:

    def __init__(self, save_path: str, chapter_path: str, audio_persistenz: AudioPersistenz, transcripts_persistenz: TranscriptsPersistenz, transcripts_selection_transformer: TranscriptsSelectionTransformer):
        self.save_path = save_path
        self.audio_persistenz = audio_persistenz
        self.transcripts_persistenz = transcripts_persistenz
        self.chapter_path = chapter_path
        self.transcripts_selection_transformer = transcripts_selection_transformer
    
    def run(self):
        done_marker = DoneMarker(self.save_path)
        result = done_marker.run(self.script, delete_folder=False)
        return result

    def script(self):
        transcripts_iterator = list(self.transcripts_persistenz.load_all())
        transcripts = transcripts_iterator[0]
        transcripts_ids = [sentence.id for sentence in transcripts.sentences()]
        chapters = pd.read_csv(self.chapter_path)

        transcripts_selected_ids = {}

        ids = self.audio_persistenz.get_ids()
        audios = self.audio_persistenz.load_all()
        for audio in tqdm(audios, total=len(ids)):
            book, chapter, index = audio.id.rsplit('_', 2)
            reader = str(chapters.loc[int(chapter) - 1]['Reader']).replace(' ', '_')  # type:ignore
            if audio.id in transcripts_ids:
                path = reader + '/' + book
                if path in transcripts_selected_ids:
                    transcripts_selected_ids[path].append(audio.id)
                else:
                    transcripts_selected_ids[path] = [audio.id]
                audio.id = path + '/wavs/' + audio.id
                self.audio_persistenz.save(audio)

        for path, ids in transcripts_selected_ids.items():
            local_transcripts = self.transcripts_selection_transformer.transform(transcripts, ids)
            local_transcripts.id = path + '/metadata'
            self.transcripts_persistenz.save(local_transcripts)
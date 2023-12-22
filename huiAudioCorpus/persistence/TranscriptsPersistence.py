from pandas.core.frame import DataFrame
from huiAudioCorpus.model.Transcripts import Transcripts
from huiAudioCorpus.utils.FileListUtil import FileListUtil
from huiAudioCorpus.utils.PathUtil import PathUtil
import pandas as pd
import os

class TranscriptsPersistence:
    def __init__(self, load_path:str, save_path: str = None, file_extension:str = 'csv'):
        self.save_path = load_path if save_path is None else save_path
        self.load_path = load_path
        self.file_extension = file_extension
        self.file_list_util = FileListUtil()
        self.path_util = PathUtil()

    def get_ids(self):
        """Gets all transcript filenames without the file extension."""
        transcripts_files = self.file_list_util.get_files(self.load_path, self.file_extension)
        transcripts_files = [os.path.basename(file).split(".")[0] for file in transcripts_files]
        return transcripts_files
    
    def load(self, transcript_id: str):
        target_path = self.load_path +'/' +  transcript_id + '.' + self.file_extension
        df = pd.read_csv(target_path, sep='|', header=None) # type: ignore
        name = self.path_util.filename_without_extension(target_path)
        transcripts = Transcripts(df, transcript_id, name)
        return transcripts
    
    def save(self, transcripts: Transcripts):
        target_path = self.save_path +'/' +  transcripts.transcript_id + '.' + self.file_extension
        self.path_util.create_folder_for_file(target_path)
        trans = transcripts.transcripts
        trans.to_csv(target_path, sep='|', header=None, index=False) # type: ignore

    def load_all(self):
        ids = self.get_ids()
        for transcript_id in ids:
            yield self.load(transcript_id)

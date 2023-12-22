import os
from datetime import datetime
from os import unlink
from os.path import isfile
from huiAudioCorpus.enum.PipelineReturnEnum import PipelineReturnEnum
from huiAudioCorpus.utils.PathUtil import PathUtil

class DoneMarker:
    done_filename = '.done'
    
    def __init__(self, path: str):
        self.path = path
        self.done_file_path = path + '/' + self.done_filename
        self.path_util = PathUtil()

    def is_done(self):
        is_done = os.path.exists(self.done_file_path)
        return is_done

    def set_done(self):
        self.path_util.create_folder_for_file(self.done_file_path)
        with open(self.done_file_path, "w") as f:
            f.write(f'Done at:   {datetime.now()}')

    def remove(self):
        if isfile(self.done_file_path):
            unlink(self.done_file_path)

    def get_info(self):
        return 'Continue to the next step because of the done marker.'

    def run(self, script, delete_folder=True):
        if self.is_done():
            print(self.get_info())
            return PipelineReturnEnum.OkWithDoneMarker

        if delete_folder:
            self.path_util.delete_folder(self.path)

        script()

        self.set_done()
        return PipelineReturnEnum.Ok

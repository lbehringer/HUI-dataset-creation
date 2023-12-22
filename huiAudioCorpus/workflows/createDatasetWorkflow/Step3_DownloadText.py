
from huiAudioCorpus.persistence.GutenbergBookPersistence import GutenbergBookPersistence
from huiAudioCorpus.utils.DoneMarker import DoneMarker

class Step3DownloadText:

    def __init__(self, gutenberg_book_persistence: GutenbergBookPersistence, save_path: str):
        self.save_path = save_path
        self.gutenberg_book_persistence = gutenberg_book_persistence

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        self.gutenberg_book_persistence.save()


from huiAudioCorpus.persistenz.GutenbergBookPersistenz import GutenbergBookPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker

class Step3DownloadText:

    def __init__(self, gutenberg_book_persistenz: GutenbergBookPersistenz, save_path: str):
        self.save_path = save_path
        self.gutenberg_book_persistenz = gutenberg_book_persistenz

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        self.gutenberg_book_persistenz.save()

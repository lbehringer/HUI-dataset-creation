from huiAudioCorpus.persistenz.CredentialsPersistenz import CredentialsPersistenz
from huiAudioCorpus.utils.PathUtil import PathUtil
import pysftp

# This class is hard to test. Because the risk is not so high I decided not to test this class automatically. Pascal
class SecureFTP:  # pragma: no cover
    def __init__(self, path_util: PathUtil, server: str, credentials_persistenz: CredentialsPersistenz):
        cnopts = pysftp.CnOpts()
        credentials = credentials_persistenz.load(server)
        cnopts.hostkeys = None
        self.connection = pysftp.Connection(server, username=credentials.username, password=credentials.password,
                                            cnopts=cnopts)
        self.path_util = path_util

    def get_files(self, path: str):
        files = self.connection.listdir(path)
        return files

    def copy_file(self, source_path: str, target_path: str):
        source = self.connection.open(source_path, 'rb')
        self.path_util.copy_file_with_stream(source, self.get_size(source_path), target_path)  # type:ignore
        source.close()

    def get_size(self, source_path: str):
        stats = self.connection.stat(source_path)
        size = stats.st_size
        return size

import glob

class FileListUtil:
    def get_files(self, path: str, extension: str):
        search_path = path + '/**/*.' + extension
        files = glob.glob(search_path, recursive=True)
        return files

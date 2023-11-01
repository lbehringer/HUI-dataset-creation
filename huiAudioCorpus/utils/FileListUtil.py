import glob

class FileListUtil:
    def getFiles(self, path: str, extension: str):
        searchPath = path +  '/**/*.' + extension
        files = glob.glob(searchPath, recursive=True)
        return files
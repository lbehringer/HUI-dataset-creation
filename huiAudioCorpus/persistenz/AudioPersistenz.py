import librosa
import soundfile
from huiAudioCorpus.model.Audio import Audio
from nptyping import NDArray
from huiAudioCorpus.utils.FileListUtil import FileListUtil
from huiAudioCorpus.utils.PathUtil import PathUtil
from natsort import natsorted

class AudioPersistenz:
    def __init__(self, loadPath:str, savePath: str = None , fileExtension:str = 'wav'):
        self.savePath = loadPath if savePath is None else savePath
        self.loadPath = loadPath
        self.fileExtension = fileExtension
        self.fileListUtil = FileListUtil()
        self.pathUtil = PathUtil()

    def load(self, id: str, duration=None):
        """Load an audio file and return it as an Audio object"""
        audioTimeSeries: NDArray
        samplingRate: int
        targetPath = self.loadPath +'/' +  id + '.' + self.fileExtension
        name = self.pathUtil.filenameWithoutExtension(targetPath)
        audioTimeSeries, samplingRate = librosa.core.load(targetPath, sr=None, duration=duration) # type: ignore
        audio = Audio(audioTimeSeries, samplingRate, id, name)
        return audio

    def save(self, audio: Audio):
        """Save an Audio object to file"""
        targetPath = self.savePath + '/' + audio.id + '.wav'
        self.pathUtil.createFolderForFile(targetPath)
        audioTimeSeries = audio.timeSeries
        samplingRate = audio.samplingRate
        soundfile.write(targetPath, audioTimeSeries, samplingRate)

    def getNames(self):
        names = [self.pathUtil.filenameWithoutExtension(id) for id in self.getIds()]
        return names

    def getIds(self):
        audioFiles = self.fileListUtil.getFiles(self.loadPath, self.fileExtension)
        audioFiles = [file.replace(self.loadPath,'')[1:-len(self.fileExtension)-1] for file in audioFiles]
        audioFiles = natsorted(audioFiles)

        return audioFiles

    def loadAll(self, duration=None):
        ids = self.getIds()
        for id in ids:
            yield self.load(id, duration=duration)
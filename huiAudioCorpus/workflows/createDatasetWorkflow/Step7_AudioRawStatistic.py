from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.utils.PathUtil import PathUtil
import pandas as pd
import os


class Step7_AudioRawStatistic:
    def __init__(self, savePath: str, loadPath: str, pathUtil: PathUtil):
        self.savePath = savePath
        self.pathUtil = pathUtil
        self.loadPath = loadPath

    def run(self):
        doneMarker = DoneMarker(self.savePath)
        result = doneMarker.run(self.script, deleteFolder=False)
        return result

    def script(self):
        from huiAudioCorpus.dependencyInjection.DependencyInjection import DependencyInjection
        speakers = os.listdir(self.loadPath)
        audioInfos = []
        for speaker in speakers:
            if speaker == '.done':
                continue
            print('finalSummary: ' + speaker)
            loadPath = self.loadPath  + '/' + speaker
            savePath = self.savePath + '/' + speaker
            saveFile = savePath + '/overview.csv'
            self.pathUtil.createFolderForFile(saveFile)
            localDoneMarker = DoneMarker(savePath)
            if localDoneMarker.isDone():
                rawDataAudio = pd.read_csv(saveFile, sep='|' , index_col='id')
            else:
                diConfig = {
                    'audioPersistenz': {
                        'loadPath': loadPath,
                    }
                }
                rawDataAudio = DependencyInjection(diConfig).audioStatisticComponent.loadAudioFiles()
                rawDataAudio['speaker'] = speaker

                diConfig = {
                    'transcriptsPersistenz': {
                        'loadPath': loadPath,
                    }
                }
                rawDataText = DependencyInjection(diConfig).textStatisticComponent.loadTextFiles()
                rawData = rawDataAudio.merge(rawDataText, how='outer', on='id' )
                rawData.to_csv(saveFile , sep='|')

                localDoneMarker.setDone()

            audioInfos.append(rawDataAudio)

        audio = pd.concat(audioInfos)
        audio.to_csv(self.savePath  + '/overview.csv', sep='|')
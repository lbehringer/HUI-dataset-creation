from typing import Dict
from huiAudioCorpus.dependencyInjection.DependencyInjection import DependencyInjection
# import datasetWorkflow
import scripts.createDatasetConfig as createDatasetConfig
from huiAudioCorpus.utils.PathUtil import PathUtil
import os

pathUtil = PathUtil()
basePath = createDatasetConfig.__path__[0]  # type: ignore

# set external path where database should be created
externalPaths = ["/mnt/c/Users/lyone/Documents/_ComputationalLinguistics/HUI_accent/HUI-Audio-Corpus-German/database"
]

# dataBasePath = datasetWorkflow.__path__[0]  # type: ignore
for path in externalPaths:
    if os.path.exists(path):
        dataBasePath = path

def logStep(name):
    print('')
    print('')
    print('#######################################################')
    print(name)
    print('#######################################################')
    print('')

### load all configurations
bernd_1 = pathUtil.loadJson(
    basePath + '/Bernd_Ungerer_tausendUndEineNacht.json')
bernd_2 = pathUtil.loadJson(basePath + '/Bernd_Ungerer_other.json')
bernd = {**bernd_1, **bernd_2}
hokuspokus = pathUtil.loadJson(basePath + '/Hokuspokus.json')
redaer = pathUtil.loadJson(basePath + '/redaer.json')
friedrich = pathUtil.loadJson(basePath + '/Friedrich.json')
eva = pathUtil.loadJson(basePath + '/Eva.json')
karlsson = pathUtil.loadJson(basePath + '/Karlsson.json')
sonja = pathUtil.loadJson(basePath + '/Sonja.json')
christian_culp_latest = pathUtil.loadJson(basePath + '/Christian_Culp_latest.json')

allLibriboxIds = [author[key]['librivox_book_name'] for author in [
    bernd, hokuspokus, friedrich, eva, karlsson, redaer] for key in author]
duplicatIds = set([x for x in allLibriboxIds if allLibriboxIds.count(x) > 1])

if len(duplicatIds) > 0:
    raise Exception("Duplicate Librivox ids: " + str(duplicatIds))


# configure this object to only create a single speaker
# allConfigs = {**bernd, **hokuspokus, **friedrich, **eva, **karlsson, **sonja}
allConfigs = sonja
# allConfigs = friedrich
#allConfigs = redaer
allConfigs = christian_culp_latest

# this is needed for the statistic and split into others
specialSpeakers = ['Bernd_Ungerer', 'Eva_K', 'Friedrich', 'Hokuspokus', 'Karlsson']

workflowConfig = {
    'continueOnError': False,
    "hifi_qa": True,
    'prepareAudio': False,
    'prepareText': False,
    'transcriptText': False,
    'alignText': False,
    'finalize': False,
    'audioRawStatistic': False,
    'cleanStatistic': False,
    'fullStatistic': False,
    'generateClean': False
}


finalDatasetPath = dataBasePath + '/finalDataset'
finalDatasetPathClean = dataBasePath + '/finalDatasetClean'
step7Path = dataBasePath + '/rawStatistic'
setp8Path = dataBasePath + '/datasetStatistic'
setp8Path_clean = dataBasePath + '/datasetStatisticClean'


def cleanFilter(input):
    """Described in the HUI paper in section 4.2"""
    input = input[input['minSilenceDB'] < -50]
    input = input[input['silencePercent'] < 45]
    input = input[input['silencePercent'] > 10]
    return input

def runWorkflow(params: Dict, workflowConfig: Dict):
    print(params)
    bookBasePath = dataBasePath + '/books/'

    step1Path = bookBasePath + params['title'] + '/Step1_DownloadAudio'
    step1PathAudio = step1Path + '/audio'
    step1PathChapter = step1Path + '/chapter.csv'
    step2Path = bookBasePath + params['title'] + '/Step2_SplitAudio'
    step2_1_Path = bookBasePath + params['title'] + '/Step2_1_AudioStatistic'

    step2PathAudio = step2Path + '/audio'
    step3Path = bookBasePath + params['title'] + '/Step3_DownloadText'
    step3PathText = step3Path + '/text.txt'
    step3_1_Path = bookBasePath + params['title'] + '/Step3_1_PrepareText'
    step3_1_PathText = step3_1_Path + '/text.txt'

    step4Path = bookBasePath + params['title'] + '/Step4_TranscriptAudio'
    step5Path = bookBasePath + params['title'] + '/Step5_AlignText'
    step6Path = bookBasePath + params['title'] + '/Step6_FinalizeDataset'

    qa1_path = bookBasePath + params["title"] + "/QA1_HifiBandwidth"
    qa1_path_audio = os.path.join(qa1_path, "audio")

    qa2_path = bookBasePath + params["title"] + "/QA2_HifiSNR"
    qa2_path_audio = os.path.join(qa2_path, "audio")

    if workflowConfig["hifi_qa"]:
        logStep('Step1_DownloadAudio')
        config = {
            'audiosFromLibrivoxPersistenz': {
                'bookName': params['librivox_book_name'],
                'savePath': step1PathAudio + '/',
                'chapterPath': step1PathChapter
            },
            'step1_DownloadAudio': {
                'savePath': step1Path
            }
        }
        DependencyInjection(config).step1_DownloadAudio.run()

        logStep("QA1_HifiBandwidth")
        config = {
            "audio_persistenz": {
                "loadPath": step1PathAudio,
                "savePath": qa1_path_audio,
                "fileExtension": "mp3"
            },
            "audio_loudness_transformer": {
                "loudness": -20
            },
            "QA1_HifiBandwidth": {
                "save_path": qa1_path_audio,
                "book_name": params["title"],
                "seconds_to_analyze": 30,
                "bandwidth_hz_threshold": 13000
            },
        }
        DependencyInjection(config).QA1_HifiBandwidth.run()

        logStep("QA2_HifiSNR")
        config = {
            "audio_persistenz": {
                "loadPath": qa1_path_audio,
                "savePath": qa2_path_audio,
                "fileExtension": "mp3"
            },
            "audio_loudness_transformer": {
                "loudness": -20
            },
            "audio_sr_transformer": {
                "targetSamplingRate": 16000
            },            
            "QA2_HifiSNR": {
                "save_path": qa2_path_audio,
                'book_name': params['title'],
                "seconds_to_analyze": 30,
            },
        }
        DependencyInjection(config).QA2_HifiSNR.run()







############################################################################################
############################################################################################

    if workflowConfig['prepareAudio']:
        logStep('Step1_DowloadAudio')
        config = {
            'audiosFromLibrivoxPersistenz': {
                'bookName': params['librivox_book_name'],
                'savePath': step1PathAudio + '/',
                'chapterPath': step1PathChapter
            },
            'step1_DownloadAudio': {
                'savePath': step1Path
            }
        }
        DependencyInjection(config).step1_DownloadAudio.run()

        logStep('Step2_SplitAudio')
        config = {
            'audioSplitTransformer': {
                'minAudioDuration': 5,
                'maxAudioDuration': 40
            },
            'audioPersistenz': {
                'loadPath': step1PathAudio,
                'savePath': step2PathAudio,
                'fileExtension': 'mp3'
            },
            'audioLoudnessTransformer': {
                'loudness': -20
            },
            'step2_SplitAudio': {
                'bookName': params['title'],
                'savePath': step2Path,
                'remapSort': params['remapSort'] if 'remapSort' in params else None
            }
        }
        DependencyInjection(config).step2_SplitAudio.run()

        logStep('Step2_1_AudioStatistic')
        config = {
            'step2_1_AudioStatistic': {
                'savePath': step2_1_Path,
            },
            'audioPersistenz': {
                'loadPath': step2PathAudio
            },
            'plot': {
                'showDuration': 1,
                'savePath': step2_1_Path
            }
        }
        DependencyInjection(config).step2_1_AudioStatistic.run()

    if workflowConfig['prepareText']:
        logStep('Step3_DowloadText')
        config = {
            'GutenbergBookPersistenz': {
                'textId': params['gutenberg_id'],
                'savePath': step3PathText
            },
            'step3_DowloadText': {
                'savePath': step3Path
            }
        }
        DependencyInjection(config).step3_DowloadText.run()

        logStep('Step3_1_PrepareText')
        config = {
            'step3_1_PrepareText': {
                'savePath': step3_1_Path,
                'loadFile': step3PathText,
                'saveFile': step3_1_PathText,
                'textReplacement': params['text_replacement'],
                'startSentence': params['gutenberg_start'],
                'endSentence': params['gutenberg_end'],
                'moves': params['moves'] if 'moves' in params else [],
                'remove': params['remove'] if 'remove' in params else []
            }
        }
        DependencyInjection(config).step3_1_PrepareText.run()

    if workflowConfig['transcriptText']:
        logStep('Step4_TranscriptAudio')
        config = {
            'step4_TranscriptAudio': {
                'savePath': step4Path,
            },
            'audioPersistenz': {
                'loadPath': step2PathAudio
            },
            'transcriptsPersistenz': {
                'loadPath': step4Path,
            }
        }
        DependencyInjection(config).step4_TranscriptAudio.run()

    if workflowConfig['alignText']:
        logStep('Step5_AlignText')
        config = {
            'step5_AlignText': {
                'savePath': step5Path,
                'textToAlignPath': step3_1_PathText
            },
            'transcriptsPersistenz': {
                'loadPath': step4Path,
                'savePath': step5Path
            }
        }
        DependencyInjection(config).step5_AlignText.run()

    if workflowConfig['finalize']:
        logStep('Step6_FinalizeDataset')
        config = {
            'step6_FinalizeDataset': {
                'savePath': step6Path,
                'chapterPath': step1PathChapter
            },
            'audioPersistenz': {
                'loadPath': step2PathAudio,
                'savePath': finalDatasetPath
            },
            'transcriptsPersistenz': {
                'loadPath': step5Path,
                'savePath': finalDatasetPath
            }
        }
        DependencyInjection(config).step6_FinalizeDataset.run()


if __name__ == "__main__":
    summary = {}
    for configName in allConfigs:
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')
        logStep(configName)
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')

        config = allConfigs[configName]
        if workflowConfig['continueOnError']:
            try:
                runWorkflow(config, workflowConfig)
                summary[config['title']] = 'finished'
            except:
                summary[config['title']] = 'error'
        else:
            runWorkflow(config, workflowConfig)
    print(summary)

    if workflowConfig['audioRawStatistic']:
        logStep('audioRawStatistic')
        diConfig = {
            'step7_AudioRawStatistic': {
                'savePath': step7Path,
                'loadPath': finalDatasetPath
            }
        }
        DependencyInjection(diConfig).step7_AudioRawStatistic.run()

    if workflowConfig['fullStatistic']:
        logStep('fullStatistic')
        diConfig = {
            'step8_DatasetStatistic': {
                'savePath': setp8Path,
                'loadPath': step7Path + '/overview.csv',
                'specialSpeakers': specialSpeakers,
                'filter': None
            },
            'audioPersistenz': {
                'loadPath':''
            },
            'transcriptsPersistenz': {
                'loadPath':''
            },
            'plot': {
                'showDuration': 0
            }
        }
        DependencyInjection(diConfig).step8_DatasetStatistic.run()

    if workflowConfig['cleanStatistic']:
        logStep('cleanStatistic')
        diConfig = {
            'step8_DatasetStatistic': {
                'savePath': setp8Path_clean,
                'loadPath': step7Path + '/overview.csv',
                'specialSpeakers': specialSpeakers,
                'filter': cleanFilter
            },
            'audioPersistenz': {
                'loadPath':''
            },
            'transcriptsPersistenz': {
                'loadPath':''
            },
            'plot': {
                'showDuration': 0
            }
        }
        DependencyInjection(diConfig).step8_DatasetStatistic.run()

    if workflowConfig['generateClean']:
        logStep('generateClean')
        diConfig = {
            'step9_GenerateCleanDataset': {
                'savePath': finalDatasetPath,
                'infoFile': step7Path +'/overview.csv',
                'filter': cleanFilter
            },
            'transcriptsPersistenz': {
                'loadPath': finalDatasetPath,
                'savePath': finalDatasetPathClean
            },
            'audioPersistenz': {
                'loadPath': finalDatasetPath,
                'savePath': finalDatasetPathClean
            },
        }
        DependencyInjection(diConfig).step9_GenerateCleanDataset.run()
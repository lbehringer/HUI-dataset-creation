from typing import Dict
from huiAudioCorpus.dependencyInjection.DependencyInjection import DependencyInjection
# import dataset_workflow
import scripts.createDatasetConfig as createDatasetConfig
from huiAudioCorpus.utils.PathUtil import PathUtil
import os

path_util = PathUtil()
base_path = createDatasetConfig.__path__[0]  # type: ignore

# set external path where database should be created
external_paths = ["/mnt/c/Users/lyone/Documents/_ComputationalLinguistics/HUI_accent/HUI-Audio-Corpus-German/database"
]

# database_path = dataset_workflow.__path__[0]  # type: ignore
for path in external_paths:
    if os.path.exists(path):
        database_path = path

def log_step(name):
    print('')
    print('')
    print('#######################################################')
    print(name)
    print('#######################################################')
    print('')

### load all configurations
bernd_1 = path_util.load_json(
    base_path + '/Bernd_Ungerer_tausendUndEineNacht.json')
bernd_2 = path_util.load_json(base_path + '/Bernd_Ungerer_other.json')
bernd = {**bernd_1, **bernd_2}
hokuspokus = path_util.load_json(base_path + '/Hokuspokus.json')
redaer = path_util.load_json(base_path + '/redaer.json')
friedrich = path_util.load_json(base_path + '/Friedrich.json')
eva = path_util.load_json(base_path + '/Eva.json')
karlsson = path_util.load_json(base_path + '/Karlsson.json')
sonja = path_util.load_json(base_path + '/Sonja.json')
christian_culp_latest = path_util.load_json(base_path + '/Christian_Culp_latest.json')
twospeakers = path_util.load_json(base_path + "/twospeakers.json")
two_readers_latest = path_util.load_json(os.path.join(base_path, "two_readers_latest.json"))
overview_20231215_212809 = path_util.load_json(os.path.join(external_paths[0], "overview_20231215_212809", "readerLatestBook.json"))

all_librivox_ids = [author[key]['librivox_book_name'] for author in [
    bernd, hokuspokus, friedrich, eva, karlsson, redaer] for key in author]
duplicate_ids = set([x for x in all_librivox_ids if all_librivox_ids.count(x) > 1])

if len(duplicate_ids) > 0:
    raise Exception("Duplicate Librivox ids: " + str(duplicate_ids))

# configure this object to only create a single speaker
# all_configs = {**bernd, **hokuspokus, **friedrich, **eva, **karlsson, **sonja}
all_configs = sonja
# all_configs = friedrich
# all_configs = redaer
all_configs = christian_culp_latest
all_configs = two_readers_latest
all_configs = overview_20231215_212809

# this is needed for the statistic and split into others
special_speakers = ['Bernd_Ungerer', 'Eva_K', 'Friedrich', 'Hokuspokus', 'Karlsson']

workflow_config = {
    'continue_on_error': False,
    "hifi_qa": True,
    'prepare_audio': False,
    'prepare_text': False,
    'transcript_text': False,
    'align_text': False,
    'finalize': False,
    'audio_raw_statistic': False,
    'clean_statistic': False,
    'full_statistic': False,
    'generate_clean': False
}

final_dataset_path = database_path + '/finalDataset'
final_dataset_path_clean = database_path + '/finalDatasetClean'
step7_path = database_path + '/rawStatistic'
step8_path = database_path + '/datasetStatistic'
step8_path_clean = database_path + '/datasetStatisticClean'


def clean_filter(input_data):
    """Described in the HUI paper in section 4.2"""
    input_data = input_data[input_data['min_silence_db'] < -50]
    input_data = input_data[input_data['silence_percent'] < 45]
    input_data = input_data[input_data['silence_percent'] > 10]
    return input_data

def run_workflow(params: Dict, workflow_config: Dict):
    print(params)
    readers_base_path = os.path.join(database_path, "readers")
    step1_path = os.path.join(readers_base_path, params["reader"], "Step1_DownloadAudio")
    step1_path_audio = step1_path + '/audio'
    step1_path_chapter = step1_path + '/chapter.csv'
    step2_path = os.path.join(readers_base_path, params["reader"], "Step2_SplitAudio")
    step2_1_path = os.path.join(readers_base_path, params["reader"], "Step2_1_AudioStatistic")

    step2_path_audio = step2_path + '/audio'
    step3_path = os.path.join(readers_base_path, params["reader"], "Step3_DownloadText")
    step3_path_text = step3_path + '/text.txt'
    step3_1_path = os.path.join(readers_base_path, params["reader"], "Step3_1_PrepareText")
    step3_1_path_text = step3_1_path + '/text.txt'

    step4_path = os.path.join(readers_base_path, params["reader"], "Step4_TranscriptAudio")
    step5_path = os.path.join(readers_base_path, params["reader"], "Step5_AlignText")
    step6_path = os.path.join(readers_base_path, params["reader"], "Step6_FinalizeDataset")

    qa1_path = os.path.join(readers_base_path, params["reader"], "QA1_HifiBandwidth")
    qa1_path_audio = os.path.join(qa1_path, "audio")

    qa2_path = os.path.join(readers_base_path, params["reader"], "QA2_HifiSNR")
    qa2_path_audio = os.path.join(qa2_path, "audio")

    qa3_path = os.path.join(readers_base_path, params["reader"], "QA3_WVMOS")
    qa3_path_audio = os.path.join(qa3_path, "audio")

    if workflow_config["hifi_qa"]:
        log_step('Step1_DownloadAudio')
        config_step1 = {
            'audios_from_librivox_persistenz': {
                'book_name': params['librivox_book_name'],
                'solo_reading': params['solo_reading'],
                'sections': params['sections'],
                'save_path': step1_path_audio + '/',
                'chapter_path': step1_path_chapter,
                'max_chapters_per_reader': 2
            },
            'step1_download_audio': {
                'save_path': step1_path
            }
        }
        DependencyInjection(config_step1).step1_download_audio.run()

        log_step("QA1_HifiBandwidth")
        config_qa1 = {
            "audio_persistenz": {
                "load_path": step1_path_audio,
                "save_path": qa1_path_audio,
                "file_extension": "mp3"
            },
            "audio_loudness_transformer": {
                "loudness": -20
            },
            "qa1_hifi_bandwidth": {
                "save_path": qa1_path_audio,
                "book_name": params["title"],
                "seconds_to_analyze": 30,
                "analysis_offset": 30,
                "bandwidth_hz_threshold": 13000
            },
        }
        DependencyInjection(config_qa1).qa1_hifi_bandwidth.run()

        log_step("QA2_HifiSNR")
        config_qa2 = {
            "audio_persistenz": {
                "load_path": qa1_path_audio,
                "save_path": qa2_path_audio,
                "file_extension": "wav"
            },
            "audio_loudness_transformer": {
                "loudness": -20
            },
            "audio_sr_transformer": {
                "target_sampling_rate": 16000
            },
            "qa2_hifi_snr": {
                "save_path": qa2_path_audio,
                'book_name': params['title'],
                "seconds_to_analyze": 30,
            },
        }
        DependencyInjection(config_qa2).qa2_hifi_snr.run()

        log_step("QA3_WVMOS")
        config_qa3 = {
            "audio_persistenz": {
                "load_path": qa2_path_audio,
                "save_path": qa3_path_audio,
                "file_extension": "wav"
            },
            "qa3_wvmos": {
                "save_path": qa3_path_audio
            },
            "audio_sr_transformer": {
                "target_sampling_rate": 16000
            },
        }
        DependencyInjection(config_qa3).qa3_wvmos.run()




############################################################################################
############################################################################################
        

    if workflow_config['prepare_audio']:
        log_step('Step1_DownloadAudio')
        config_step1 = {
            'audios_from_librivox_persistenz': {
                'book_name': params['librivox_book_name'],
                'save_path': step1_path_audio + '/',
                'chapter_path': step1_path_chapter
            },
            'step1_download_audio': {
                'save_path': step1_path
            }
        }
        DependencyInjection(config_step1).step1_download_audio.run()

        log_step('Step2_SplitAudio')
        config_step2 = {
            'audio_split_transformer': {
                'min_audio_duration': 5,
                'max_audio_duration': 40
            },
            'audio_persistenz': {
                'load_path': step1_path_audio,
                'save_path': step2_path_audio,
                'file_extension': 'mp3'
            },
            'audio_loudness_transformer': {
                'loudness': -20
            },
            'step2_split_audio': {
                'book_name': params['title'],
                'save_path': step2_path,
                'remap_sort': params['remap_sort'] if 'remap_sort' in params else None
            }
        }
        DependencyInjection(config_step2).step2_split_audio.run()

        log_step('Step2_1_AudioStatistic')
        config_step2_1 = {
            'step2_1_audio_statistic': {
                'save_path': step2_1_path,
            },
            'audio_persistenz': {
                'load_path': step2_path_audio
            },
            'plot': {
                'show_duration': 1,
                'save_path': step2_1_path
            }
        }
        DependencyInjection(config_step2_1).step2_1_audio_statistic.run()

    if workflow_config['prepare_text']:
        log_step('Step3_DownloadText')
        config_step3 = {
            'gutenberg_book_persistenz': {
                'text_id': params['gutenberg_id'],
                'save_path': step3_path_text
            },
            'step3_download_text': {
                'save_path': step3_path
            }
        }
        DependencyInjection(config_step3).step3_download_text.run()

        log_step('Step3_1_PrepareText')
        config_step3_1 = {
            'step3_1_prepare_text': {
                'save_path': step3_1_path,
                'load_file': step3_path_text,
                'save_file': step3_1_path_text,
                'text_replacement': params['text_replacement'],
                'start_sentence': params['gutenberg_start'],
                'end_sentence': params['gutenberg_end'],
                'moves': params['moves'] if 'moves' in params else [],
                'remove': params['remove'] if 'remove' in params else []
            }
        }
        DependencyInjection(config_step3_1).step3_1_prepare_text.run()

    if workflow_config['transcript_text']:
        log_step('Step4_TranscriptAudio')
        config_step4 = {
            'step4_transcript_audio': {
                'save_path': step4_path,
            },
            'audio_persistenz': {
                'load_path': step2_path_audio
            },
            'transcripts_persistenz': {
                'load_path': step4_path,
            }
        }
        DependencyInjection(config_step4).step4_transcript_audio.run()

    if workflow_config['align_text']:
        log_step('Step5_AlignText')
        config_step5 = {
            'step5_align_text': {
                'save_path': step5_path,
                'text_to_align_path': step3_1_path_text
            },
            'transcripts_persistenz': {
                'load_path': step4_path,
                'save_path': step5_path
            }
        }
        DependencyInjection(config_step5).step5_align_text.run()

    if workflow_config['finalize']:
        log_step('Step6_FinalizeDataset')
        config_step6 = {
            'step6_finalize_dataset': {
                'save_path': step6_path,
                'chapter_path': step1_path_chapter
            },
            'audio_persistenz': {
                'load_path': step2_path_audio,
                'save_path': final_dataset_path
            },
            'transcripts_persistenz': {
                'load_path': step5_path,
                'save_path': final_dataset_path
            }
        }
        DependencyInjection(config_step6).step6_finalize_dataset.run()

if __name__ == "__main__":
    summary = {}
    for config_name in all_configs:
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')
        log_step(config_name)
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')
        print('+++++++++++++++++++++++++++++++++++++++++')

        config = all_configs[config_name]
        if workflow_config['continue_on_error']:
            try:
                run_workflow(config, workflow_config)
                summary[config['title']] = 'finished'
            except:
                summary[config['title']] = 'error'
        else:
            run_workflow(config, workflow_config)
    print(summary)

    if workflow_config['audio_raw_statistic']:
        log_step('audio_raw_statistic')
        di_config_step7 = {
            'step7_audio_raw_statistic': {
                'save_path': step7_path,
                'load_path': final_dataset_path
            }
        }
        DependencyInjection(di_config_step7).step7_audio_raw_statistic.run()

    if workflow_config['full_statistic']:
        log_step('full_statistic')
        di_config_step8 = {
            'step8_dataset_statistic': {
                'save_path': step8_path,
                'load_path': step7_path + '/overview.csv',
                'special_speakers': special_speakers,
                'filter': None
            },
            'audio_persistenz': {
                'load_path': ''
            },
            'transcripts_persistenz': {
                'load_path': ''
            },
            'plot': {
                'show_duration': 0
            }
        }
        DependencyInjection(di_config_step8).step8_dataset_statistic.run()

    if workflow_config['clean_statistic']:
        log_step('clean_statistic')
        di_config_clean_statistic = {
            'step8_dataset_statistic': {
                'save_path': step8_path_clean,
                'load_path': step7_path + '/overview.csv',
                'special_speakers': special_speakers,
                'filter': clean_filter
            },
            'audio_persistenz': {
                'load_path': ''
            },
            'transcripts_persistenz': {
                'load_path': ''
            },
            'plot': {
                'show_duration': 0
            }
        }
        DependencyInjection(di_config_clean_statistic).step8_dataset_statistic.run()

    if workflow_config['generate_clean']:
        log_step('generate_clean')
        di_config_step9 = {
            'step9_generate_clean_dataset': {
                'save_path': final_dataset_path,
                'info_file': step7_path + '/overview.csv',
                'filter': clean_filter
            },
            'transcripts_persistenz': {
                'load_path': final_dataset_path,
                'save_path': final_dataset_path_clean
            },
            'audio_persistenz': {
                'load_path': final_dataset_path,
                'save_path': final_dataset_path_clean
            },
        }
        DependencyInjection(di_config_step9).step9_generate_clean_dataset.run()

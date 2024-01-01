from typing import Dict
from huiAudioCorpus.dependencyInjection.DependencyInjection import DependencyInjection
# import datasetWorkflow
import scripts.createDatasetConfig as createDatasetConfig
from huiAudioCorpus.utils.PathUtil import PathUtil
import os


path_util = PathUtil()
base_path = createDatasetConfig.__path__[0]  # type: ignore

# set external path where database should be created
external_paths = ["/mnt/c/Users/lyone/Documents/_ComputationalLinguistics/HUI_accent/HUI-Audio-Corpus-German/database"]

# data_base_path = dataset_workflow.__path__[0]  # type: ignore
for path in external_paths:
    if os.path.exists(path):
        data_base_path = path

def log_step(name):
    print('')
    print('')
    print('#######################################################')
    print(name)
    print('#######################################################')
    print('')

# load all configurations
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
christian_culp = path_util.load_json(os.path.join(base_path, 'Christian_Culp_latest.json'))
christian_culp_one_section = path_util.load_json(os.path.join(base_path, 'Christian_Culp_one_section.json'))

all_libribox_ids = [author[key]['librivox_book_name'] for author in [
    bernd, hokuspokus, friedrich, eva, karlsson, redaer] for key in author]
duplicate_ids = set([x for x in all_libribox_ids if all_libribox_ids.count(x) > 1])

if len(duplicate_ids) > 0:
    raise Exception("Duplicate Librivox ids: " + str(duplicate_ids))

# configure this object to only create a single speaker
# all_configs = {**bernd, **hokuspokus, **friedrich, **eva, **karlsson, **sonja}
# all_configs = sonja
all_configs = friedrich
# all_configs = redaer
all_configs = christian_culp
# all_configs = christian_culp_one_section

# this is needed for the statistic and split into others
special_speakers = ['Bernd_Ungerer', 'Eva_K', 'Friedrich', 'Hokuspokus', 'Karlsson']

workflow_config = {
    'continue_on_error': False,
    'prepare_audio': True,
    'prepare_text': True,
    'transcript_text': True,
    'align_text': True,
    'finalize': True,
    'audio_raw_statistic': True,
    'clean_statistic': True,
    'full_statistic': True,
    'generate_clean': True
}

final_dataset_path = data_base_path + '/final_dataset'
final_dataset_path_clean = data_base_path + '/final_dataset_clean'
step7_path = data_base_path + '/raw_statistic'
step8_path = data_base_path + '/dataset_statistic'
step8_path_clean = data_base_path + '/dataset_statistic_clean'

def clean_filter(input):
    """Described in the HUI paper in section 4.2"""
    input = input[input['min_silence_db'] < -50]
    input = input[input['silence_percent'] < 45]
    input = input[input['silence_percent'] > 10]
    return input

def run_workflow(params: Dict, workflow_config: Dict):
    print(params)
    book_base_path = data_base_path + '/books/'

    step1_path = book_base_path + params['title'] + '/Step1_DownloadAudio'
    step1_path_audio  = step1_path + '/audio'
    step1_path_chapter = step1_path + '/chapter.csv'
    step2_path  = book_base_path + params['title'] + '/Step2_SplitAudio'
    step2_1_path  = book_base_path + params['title'] + '/Step2_1_AudioStatistic'

    step2_path_audio  = step2_path + '/audio'
    step3_path  = book_base_path + params['title'] + '/Step3_DownloadText'
    step3_path_text  = step3_path + '/text.txt'
    step3_1_path  = book_base_path + params['title'] + '/Step3_1_PrepareText'
    step3_1_path_text  = step3_1_path + '/text.txt'

    step4_path  = book_base_path + params['title'] + '/Step4_TranscriptAudio'
    step4_1_path = os.path.join(book_base_path + params['title'], "Step4_1_NormalizeTranscript")
    step5_path  = book_base_path + params['title'] + '/Step5_AlignText'
    step6_path  = book_base_path + params['title'] + '/Step6_FinalizeDataset'
        
    if workflow_config['prepare_audio']:
        log_step('step1_download_audio')
        config = {
            'audios_from_librivox_persistence': {
                'reader': params['reader'],
                'book_name': params['librivox_book_name'],
                'solo_reading': params['solo_reading'],
                'sections': params['sections'],
                'save_path': step1_path_audio + '/',
                'chapter_path': step1_path_chapter
            },
            'step1_download_audio': {
                'save_path': step1_path
            }
        }
        DependencyInjection(config).step1_download_audio.run()

        log_step('step2_split_audio')
        config = {
            'audio_split_transformer': {
                'min_audio_duration': 5,
                'max_audio_duration': 40
            },
            'audio_persistence': {
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
        DependencyInjection(config).step2_split_audio.run()

        log_step('step2_1_audio_statistic')
        config = {
            'step2_1_audio_statistic': {
                'save_path': step2_1_path,
            },
            'audio_persistence': {
                'load_path': step2_path_audio
            },
            'plot': {
                'show_duration': 1,
                'save_path': step2_1_path
            }
        }
        DependencyInjection(config).step2_1_audio_statistic.run()

    if workflow_config['prepare_text']:
        log_step('step3_download_text')
        config = {
            'gutenberg_book_persistence': {
                'text_id': params['gutenberg_id'],
                'save_path': step3_path_text
            },
            'step3_download_text': {
                'save_path': step3_path
            }
        }
        DependencyInjection(config).step3_download_text.run()

        log_step('step3_1_prepare_text')
        config = {
            'step3_1_prepare_text': {
                'save_path': step3_1_path,
                'load_file': step3_path_text,
                'save_file': step3_1_path_text,
                'text_replacement': params['text_replacement'],
                'start_sentence': params['gutenberg_start'],
                'end_sentence': params['gutenberg_end'],
                'language': params['language'],
                'moves': params['moves'] if 'moves' in params else [],
                'remove': params['remove'] if 'remove' in params else []
            }
        }
        DependencyInjection(config).step3_1_prepare_text.run()

    if workflow_config['transcript_text']:
        log_step('step4_transcript_audio')
        config = {
            'step4_transcript_audio': {
                'save_path': step4_path,
            },
            'audio_persistence': {
                'load_path': step2_path_audio
            },
            'audio_to_sentence_converter': {
                'language': params['language']
            },
            'transcripts_persistence': {
                'load_path': step4_path,
            }
        }
        DependencyInjection(config).step4_transcript_audio.run()

        log_step('step4_1_normalize_transcript')
        config = {
            "step4_1_normalize_transcript": {
                'save_path': step4_1_path,
                'text_replacement': params['text_replacement'],
                'language': params['language']
            },
            "transcripts_persistence": {
                "load_path": step4_path,
                "save_path": step4_1_path
            }
        }
        DependencyInjection(config).step4_1_normalize_transcript.run()

    if workflow_config['align_text']:
        log_step('step5_align_text')
        config = {
            'step5_align_text': {
                'save_path': step5_path,
                'text_to_align_path': step3_1_path_text
            },
            'transcripts_persistence': {
                'load_path': step4_1_path,
                'save_path': step5_path
            },
            'align_sentences_into_text_calculator': {
                'sections': params['sections']
            }
        }
        DependencyInjection(config).step5_align_text.run()

    if workflow_config['finalize']:
        log_step('step6_finalize_dataset')
        config = {
            'step6_finalize_dataset': {
                'save_path': step6_path,
                'chapter_path': step1_path_chapter
            },
            'audio_persistence': {
                'load_path': step2_path_audio,
                'save_path': final_dataset_path
            },
            'transcripts_persistence': {
                'load_path': step5_path,
                'save_path': final_dataset_path
            }
        }
        DependencyInjection(config).step6_finalize_dataset.run()

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
            except Exception:
                summary[config['title']] = 'error'
        else:
            run_workflow(config, workflow_config)
    print(summary)

    if workflow_config['audio_raw_statistic']:
        log_step('audio_raw_statistic')
        di_config = {
            'step7_audio_raw_statistic': {
                'save_path': step7_path,
                'load_path': final_dataset_path
            }
        }
        DependencyInjection(di_config).step7_audio_raw_statistic.run()

    if workflow_config['full_statistic']:
        log_step('full_statistic')
        di_config = {
            'step8_dataset_statistic': {
                'save_path': step8_path,
                'load_path': step7_path + '/overview.csv',
                'special_speakers': special_speakers,
                'filter': None
            },
            'audio_persistence': {
                'load_path': ''
            },
            'transcripts_persistence': {
                'load_path': ''
            },
            'plot': {
                'show_duration': 0
            }
        }
        DependencyInjection(di_config).step8_dataset_statistic.run()

    if workflow_config['clean_statistic']:
        log_step('clean_statistic')
        di_config = {
            'step8_dataset_statistic': {
                'save_path': step8_path_clean,
                'load_path': step7_path + '/overview.csv',
                'special_speakers': special_speakers,
                'filter': clean_filter
            },
            'audio_persistence': {
                'load_path': ''
            },
            'transcripts_persistence': {
                'load_path': ''
            },
            'plot': {
                'show_duration': 0
            }
        }
        DependencyInjection(di_config).step8_dataset_statistic.run()

    if workflow_config['generate_clean']:
        log_step('generate_clean')
        di_config = {
            'step9_generate_clean_dataset': {
                'save_path': final_dataset_path,
                'info_file': step7_path + '/overview.csv',
                'filter': clean_filter
            },
            'transcripts_persistence': {
                'load_path': final_dataset_path,
                'save_path': final_dataset_path_clean
            },
            'audio_persistence': {
                'load_path': final_dataset_path,
                'save_path': final_dataset_path_clean
            },
        }
        DependencyInjection(di_config).step9_generate_clean_dataset.run()

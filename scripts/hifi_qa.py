from typing import Dict
from huiAudioCorpus.dependencyInjection.DependencyInjection import DependencyInjection
# import dataset_workflow
import scripts.createDatasetConfig as createDatasetConfig
from huiAudioCorpus.utils.PathUtil import PathUtil
import os
import yaml
import time

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
#all_configs = two_readers_latest
#all_configs = overview_20231215_212809

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

    timestamp = time.strftime("%Y%m%d_%H%M%S")

    readers_base_path = os.path.join(database_path, "readers")
    config_save_path = os.path.join(readers_base_path, params["reader"], "config.yaml")
    step1_path = os.path.join(readers_base_path, params["reader"], "Step1_DownloadAudio")
    step1_path_audio = step1_path + '/audio'
    step1_path_chapter = step1_path + '/chapter.csv'
    step1_path_hifi_qa = step1_path + '/hifi_qa.csv'
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
    qa1_path_hifi_qa = os.path.join(qa1_path, "hifi_qa.csv")

    qa2_path = os.path.join(readers_base_path, params["reader"], "QA2_HifiSNR")
    qa2_path_audio = os.path.join(qa2_path, "audio")
    qa2_path_hifi_qa = os.path.join(qa2_path, "hifi_qa.csv")

    qa3_path = os.path.join(readers_base_path, params["reader"], "QA3_WVMOS")
    qa3_path_audio = os.path.join(qa3_path, "audio")
    qa3_path_hifi_qa = os.path.join(qa3_path, "hifi_qa.csv")

    qa_4_path = os.path.join(database_path, "hifi_qa_overview")
    hifi_qa_overview_filepath = os.path.join(qa_4_path, f"hifi_qa_overview_{timestamp}.csv")

    # hifi_qa_params
    hifi_qa_params = {
        "analysis_offset": 30,
        "seconds_to_analyze": 30,
        "loudness": -20,
        "bandwidth_hz_threshold": 13000,
        "min_db_threshold_wrt_peak": -60,
        "qa2_vad_min_speech_duration_ms": 250,
        "qa2_vad_min_silence_duration_ms": 150,
        "qa2_vad_window_size_samples": 512,
        "qa2_vad_speech_pad_ms": 30,            
        "qa2_vad_threshold": 0.5,
        "qa3_vad_min_speech_duration_ms": 250,
        "qa3_vad_min_silence_duration_ms": 500,
        "qa3_vad_window_size_samples": 512,
        "qa3_vad_speech_pad_ms": 50,            
        "qa3_vad_threshold": 0.5
        }
    with open(config_save_path, "w") as f:
        yaml.dump(hifi_qa_params, f)


    if workflow_config["hifi_qa"]:
        log_step('Step1_DownloadAudio')
        config_step1 = {
            'audios_from_librivox_persistence': {
                'reader': params['reader'],
                'book_name': params['librivox_book_name'],
                'solo_reading': params['solo_reading'],
                'sections': params['sections'],
                'save_path': step1_path_audio + '/',
                'chapter_path': step1_path_chapter,
                'hifi_qa_save_path': step1_path_hifi_qa,
                'max_chapters_per_reader': 2
            },
            'step1_download_audio': {
                'save_path': step1_path
            }
        }
        DependencyInjection(config_step1).step1_download_audio.run()

        log_step("QA1_HifiBandwidth")
        config_qa1 = {
            "audio_persistence": {
                "load_path": step1_path_audio,
                "save_path": qa1_path_audio,
                "file_extension": "mp3"
            },
            "audio_loudness_transformer": {
                "loudness": hifi_qa_params["loudness"]
            },
            "qa1_hifi_bandwidth": {
                "save_path": qa1_path,
                "book_name": params["title"],
                "seconds_to_analyze": hifi_qa_params["seconds_to_analyze"],
                "analysis_offset": hifi_qa_params["analysis_offset"],
                "bandwidth_hz_threshold": hifi_qa_params["bandwidth_hz_threshold"],
                "min_db_threshold_wrt_peak": hifi_qa_params["min_db_threshold_wrt_peak"],
                "hifi_qa_load_path": step1_path_hifi_qa,
                "hifi_qa_save_path": qa1_path_hifi_qa                
            },
        }
        DependencyInjection(config_qa1).qa1_hifi_bandwidth.run()

        log_step("QA2_HifiSNR")
        config_qa2 = {
            "audio_persistence": {
                "load_path": step1_path_audio,
                "save_path": qa2_path_audio,
                "file_extension": "mp3"
            },
            "audio_loudness_transformer": {
                "loudness": -20
            },
            "audio_sr_transformer": {
                "target_sampling_rate": 16000
            },
            "qa2_hifi_snr": {
                "save_path": qa2_path,
                'book_name': params['title'],
                "seconds_to_analyze": 30,
                "hifi_qa_load_path": qa1_path_hifi_qa,
                "hifi_qa_save_path": qa2_path_hifi_qa,
                "seconds_to_analyze": hifi_qa_params["seconds_to_analyze"],
                "analysis_offset": hifi_qa_params["analysis_offset"],                
                "vad_min_speech_duration_ms": hifi_qa_params["qa2_vad_min_speech_duration_ms"],
                "vad_min_silence_duration_ms": hifi_qa_params["qa2_vad_min_silence_duration_ms"],
                "vad_window_size_samples": hifi_qa_params["qa2_vad_window_size_samples"],
                "vad_speech_pad_ms": hifi_qa_params["qa2_vad_speech_pad_ms"],            
                "vad_threshold": hifi_qa_params["qa2_vad_threshold"]                
            },
        }
        DependencyInjection(config_qa2).qa2_hifi_snr.run()

        log_step("QA3_WVMOS")
        config_qa3 = {
            "audio_persistence": {
                "load_path": step1_path_audio,
                "save_path": qa3_path_audio,
                "file_extension": "mp3"
            },
            "qa3_wvmos": {
                "save_path": qa3_path,
                "hifi_qa_load_path": qa2_path_hifi_qa,
                "hifi_qa_save_path": qa3_path_hifi_qa,
                "seconds_to_analyze": hifi_qa_params["seconds_to_analyze"],
                "analysis_offset": hifi_qa_params["analysis_offset"],                
                "vad_min_speech_duration_ms": hifi_qa_params["qa3_vad_min_speech_duration_ms"],
                "vad_min_silence_duration_ms": hifi_qa_params["qa3_vad_min_silence_duration_ms"],
                "vad_window_size_samples": hifi_qa_params["qa3_vad_window_size_samples"],
                "vad_speech_pad_ms": hifi_qa_params["qa3_vad_speech_pad_ms"],            
                "vad_threshold": hifi_qa_params["qa3_vad_threshold"]                               
            },
            "audio_sr_transformer": {
                "target_sampling_rate": 16000
            },
        }
        DependencyInjection(config_qa3).qa3_wvmos.run()

        log_step("QA4_HifiQAOverview")
        config_qa4 = {
            "qa_4_hifi_qa_overview": {
                "save_path": qa_4_path,
                "hifi_qa_overview_filepath": hifi_qa_overview_filepath,
                "readers_dir": readers_base_path,
                "qa3_dir_basename": os.path.basename(qa3_path),
                "csv_basename": os.path.basename(qa3_path_hifi_qa)
            }
        }
        DependencyInjection(config_qa4).qa_4_hifi_qa_overview.run()



############################################################################################
############################################################################################
        

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

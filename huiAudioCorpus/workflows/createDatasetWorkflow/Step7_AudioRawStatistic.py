from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.utils.PathUtil import PathUtil
import pandas as pd
import os


class Step7_AudioRawStatistic:
    def __init__(self, save_path: str, load_path: str, path_util: PathUtil):
        self.save_path = save_path
        self.path_util = path_util
        self.load_path = load_path

    def run(self):
        done_marker = DoneMarker(self.save_path)
        result = done_marker.run(self.script, delete_folder=False)
        return result

    def script(self):
        from huiAudioCorpus.dependencyInjection.DependencyInjection import DependencyInjection
        speakers = os.listdir(self.load_path)
        audio_infos = []

        for speaker in speakers:
            if speaker == '.done':
                continue
            print('final_summary: ' + speaker)
            save_path = os.path.join(self.save_path, speaker)
            save_file = os.path.join(save_path, 'overview.csv')
            self.path_util.create_folder_for_file(save_file)
            local_done_marker = DoneMarker(save_path)

            if local_done_marker.is_done():
                raw_data_audio = pd.read_csv(save_file, sep='|', index_col='id')
            else:
                books = os.listdir(os.path.join(self.load_path, speaker))
                for book in books:
                    load_path = os.path.join(self.load_path, speaker, book)            
                    di_config_audio = {
                        'audio_persistence': {
                            'load_path': load_path,
                        }
                    }
                    raw_data_audio = DependencyInjection(di_config_audio).audio_statistic_component.load_audio_files()
                    raw_data_audio['speaker'] = speaker

                    di_config_text = {
                        'transcripts_persistence': {
                            'load_path': load_path,
                        }
                    }
                    raw_data_text = DependencyInjection(di_config_text).text_statistic_component.load_text_files()
                    raw_data = raw_data_audio.merge(raw_data_text, how='outer', on='id')
                    raw_data.to_csv(save_file, sep='|')

                local_done_marker.set_done()

            audio_infos.append(raw_data_audio)

        audio = pd.concat(audio_infos)
        audio.to_csv(os.path.join(self.save_path, 'overview.csv'), sep='|')
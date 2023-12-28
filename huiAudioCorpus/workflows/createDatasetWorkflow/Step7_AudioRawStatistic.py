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
        all_speaker_overview_dfs = []

        # create speaker-specific overviews
        for speaker in speakers:
            if speaker == '.done':
                continue
            print('final_summary: ' + speaker)
            save_path = os.path.join(self.save_path, speaker)
            save_file = os.path.join(save_path, 'overview.csv')
            self.path_util.create_folder_for_file(save_file)
            local_done_marker = DoneMarker(save_path)

            if local_done_marker.is_done():
                raw_data = pd.read_csv(save_file, sep='|', index_col='id')
            else:
                books = os.listdir(os.path.join(self.load_path, speaker))
                for book in books:
                    load_path = os.path.join(self.load_path, speaker, book)

                    # get audio info
                    di_config_audio = {
                        'audio_persistence': {
                            'load_path': load_path,
                        }
                    }
                    raw_data_audio = DependencyInjection(di_config_audio).audio_statistic_component.load_audio_files()
                    raw_data_audio['speaker'] = speaker

                    # get text info
                    di_config_text = {
                        'transcripts_persistence': {
                            'load_path': load_path,
                        }
                    }
                    raw_data_text = DependencyInjection(di_config_text).text_statistic_component.load_text_files()

                    # merge audio and text info with corresponding IDs
                    raw_data = raw_data_audio.merge(raw_data_text, how='outer', on='id')

                    # write speaker-specific overview to csv file
                    raw_data.to_csv(save_file, sep='|')

                local_done_marker.set_done()

            all_speaker_overview_dfs.append(raw_data)

        # create all-speaker overview file
        all_speakers_overview = pd.concat(all_speaker_overview_dfs)
        all_speakers_overview.to_csv(os.path.join(self.save_path, 'overview.csv'), sep='|')
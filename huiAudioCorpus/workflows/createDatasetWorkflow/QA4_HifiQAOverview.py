from huiAudioCorpus.utils.DoneMarker import DoneMarker
import pandas as pd
import os
import glob

class QA4_HifiQAOverview:
    def __init__(
            self, 
            save_path: str,
            hifi_qa_overview_filepath: str,
            readers_dir: str,
            qa3_dir_basename: str,
            csv_basename: str):
        self.save_path = save_path
        self.hifi_qa_overview_filepath = hifi_qa_overview_filepath
        self.readers_dir = readers_dir
        self.qa3_dir_basename = qa3_dir_basename
        self.csv_basename = csv_basename


    def run(self):
        return DoneMarker(self.save_path).run(self.script, delete_folder=False)
    
    def script(self):
        reader_specific_files = glob.glob(os.path.join(self.readers_dir, "*", self.qa3_dir_basename, self.csv_basename))
        reader_specific_dfs = [pd.read_csv(f, sep="|") for f in reader_specific_files]
        overview_df = pd.read_csv(reader_specific_files[0], sep="|")
        # for f in reader_specific_files[1:]:
        #     reader_specific_df = pd.read_csv(f, sep="|")
        #     overview_df = overview_df.merge(reader_specific_df, how="outer", on="id")
        overview_df = pd.concat(reader_specific_dfs)
        os.makedirs(self.save_path, exist_ok=True)
        overview_df.to_csv(self.hifi_qa_overview_filepath, sep="|", index=False)

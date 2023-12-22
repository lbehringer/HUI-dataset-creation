from io import BufferedReader
import json
from pathlib import Path
import os
import huiAudioCorpus.testOutput as test_output
from tqdm import tqdm
import requests
from zipfile import ZipFile


class PathUtil:
    def filename_without_extension(self, path: str):
        filename = self.filename_with_extension(path)
        filename_without_extension = os.path.splitext(filename)[0]
        return filename_without_extension

    def filename_with_extension(self, path: str):
        filename = Path(path).name
        return filename

    def create_folder_for_file(self, file):
        Path(file).parent.mkdir(parents=True, exist_ok=True)

    def delete_folder(self, folder):
        if os.path.isdir(folder):
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                else:
                    self.delete_folder(file_path)
            os.rmdir(folder)

    def get_test_data_folder(self, test_file: str):
        test_folder = test_output.__path__[0] + '/' + self.filename_without_extension(test_file)
        return test_folder

    def copy_file_with_stream(self, input_stream: BufferedReader, input_size: int, output_file):
        self.create_folder_for_file(output_file)
        buffer_size = 1024 * 1024 * 2

        with tqdm(total=input_size, unit='iB', unit_scale=True, unit_divisor=1024) as pbar:
            with open(output_file, 'wb') as dest:
                while True:
                    copy_buffer = input_stream.read(buffer_size)
                    if not copy_buffer:
                        break
                    size = dest.write(copy_buffer)
                    pbar.update(size)

    def copy_file_from_url(self, url: str, output_file):
        self.create_folder_for_file(output_file)
        buffer_size = 1024 * 10

        resp = requests.get(url, stream=True)
        input_size = resp.headers.get('content-length')
        if input_size is not None:
            input_size = int(input_size)
        else:
            input_size = None

        input_size = int()  # type: ignore
        with tqdm(total=input_size, unit='iB', unit_scale=True, unit_divisor=1024, desc=url) as pbar:
            with open(output_file, 'wb') as dest:
                for data in resp.iter_content(chunk_size=buffer_size):
                    size = dest.write(data)
                    pbar.update(size)

    def copy_file(self, input_file: str, output_file: str):
        size = os.path.getsize(input_file)
        with open(input_file, 'rb', ) as source:
            self.copy_file_with_stream(source, size, output_file)

    def unzip(self, input_zip: str, output_folder: str):
        with ZipFile(input_zip, 'r') as zip_reference:
            for file in zip_reference.namelist():
                if not os.path.isfile(os.path.join(output_folder, file)):
                    print('start unzipping because file ', file, 'does not exist.')
                    self.delete_folder(output_folder)
                    zip_reference.extractall(output_folder)
                    break

    def save_json(self, filename: str, json_content):
        string = json.dumps(json_content, indent=4, ensure_ascii=False)
        self.create_folder_for_file(filename)
        with open(filename, 'w', encoding='utf8') as file:
            file.write(string)

    def load_json(self, filename: str):
        with open(filename, encoding='utf8') as json_file:
            data = json.load(json_file)
        return data

    def write_file(self, text: str, filename: str):
        self.create_folder_for_file(filename)
        with open(filename, 'w', encoding='utf8') as f:
            f.write(text)

    def load_file(self, filename: str):
        with open(filename, 'r', encoding='utf8') as f:
            input_text = f.read()
        return input_text

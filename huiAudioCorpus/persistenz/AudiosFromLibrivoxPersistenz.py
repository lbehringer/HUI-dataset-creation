import bs4 as bs
import pandas as pd
from huiAudioCorpus.utils.PathUtil import PathUtil
import requests
import json
from tqdm import tqdm
from joblib import Parallel, delayed
from urllib.parse import quote
from typing import Union

class AudiosFromLibrivoxPersistence:

    def __init__ (self, book_name: str, save_path: str, chapter_path: str, url: str = 'https://librivox.org/', solo_reading: bool = None, sections: Union[list, str] = None, max_chapters_per_reader=None):
        self.book_name = book_name
        self.url = url
        self.save_path = save_path
        self.solo_reading = solo_reading
        self.sections = sections
        self.chapter_path = chapter_path
        self.path_util = PathUtil()
        self.limit_per_iteration = 1000
        self.number_of_iterations = 20
        self.max_chapters_per_reader = max_chapters_per_reader
        self.minimum_upload_timestamp = 1702162800  # 10 December 2023 

    def save(self):
        chapters, chapter_download_links = self.get_chapters(self.book_name, get_download_links=True)
        Parallel(n_jobs=1)(delayed(self.path_util.copy_file_from_url)(link ,self.save_path + '/' + link.split('/')[-1]) for link in chapter_download_links[:self.max_chapters_per_reader])
        chapters.to_csv(self.chapter_path)
        

    def get_chapters(self, book_name: str, get_download_links):
        search_url = self.get_search_url(book_name, self.url)
        response = self.load_search_book(search_url)
        chapter_url = self.extract_chapter_url(response)
        chapter_download_links = self.get_chapter_links(chapter_url) if get_download_links else None
        chapters = pd.read_html(chapter_url)
        return chapters[0], chapter_download_links

    def get_catalog_date(self, search_url: str):
        search_result = requests.get(search_url)
        search_result.encoding = "UTF-8"
        soup = bs.BeautifulSoup(search_result.text, 'html.parser')
        production_details = soup.find("dl", class_="product-details")
        catalog_date = production_details.find_all("dd")[2].text
        return int(catalog_date.replace("-", ""))

    def load_search_book(self, url: str):
        search_result = requests.get(url)
        return search_result.text

    def get_search_url(self, book_name: str, url: str):
        search_url = url + 'api/feed/audiobooks/?format=json&title=' + quote(book_name)
        return search_url

    def extract_chapter_url(self, response: str):
        json_input = json.loads(response)['books']
        book = json_input[0]
        url_zip_file = book['url_librivox']
        return url_zip_file

    def extract_zip_url(self, response: str):
        json_input = json.loads(response)['books']
        book = json_input[0]
        url_zip_file = book['url_zip_file']
        return url_zip_file

    def get_chapter_links(self, url: str):
        search_result = requests.get(url)
        search_result.encoding = "UTF-8"
        soup = bs.BeautifulSoup(search_result.text, 'html.parser')
        parsed_table = soup.find_all('table')[0] 
        data = [[td.a['href'] if td.find('a') else 
                ''.join(td.stripped_strings)
                for td in row.find_all('td')]
                for row in parsed_table.find_all('tr')]
        if self.solo_reading:
            download_links = [chapter[1] for chapter in data if len(chapter) > 0]
        else:
            download_links = [chapter[1] for idx, chapter in enumerate(data) if idx in set(self.sections) and len(chapter) > 0]
        return download_links

    def get_ids(self, request_url=None):
        books = []
        if request_url:
            print("Using custom request URL for metadata retrieval.")
            page = requests.get(request_url)
            page.encoding = "UTF-8"
            try:
                result = json.loads(page.text)
                if 'books' in result:
                    for idx, book in enumerate(result['books']):
                        result['books'][idx]['catalog_date'] = self.get_catalog_date(book['url_librivox'])
                    books.extend(result['books'])
                else:
                    print(result)
                    print("Stopping download of overview metadata.")
            except ValueError as e:
                print(f"Error: {e}")
                print("Stopping download of overview metadata.")
        else:
            limit = self.limit_per_iteration
            minimum_upload_timestamp = self.minimum_upload_timestamp
            for i in tqdm(range(self.number_of_iterations), desc=f"Performing retrieval in max. {self.number_of_iterations} iterations"):
                request_url = f'https://librivox.org/api/feed/audiobooks/?limit={limit}&offset={i*limit}&since={minimum_upload_timestamp}&format=json'
                page = requests.get(request_url)
                page.encoding = "UTF-8"
                try:
                    result = json.loads(page.text)
                    if 'books' in result:
                        for idx, book in enumerate(result['books']):
                            result['books'][idx]['catalog_date'] = self.get_catalog_date(book['url_librivox'])
                        books.extend(result['books'])
                    else:
                        print(result)
                        print("Stopping download of overview metadata.")
                        break
                except ValueError as e:
                    print(f"Error in iteration {i}: {e}")
                    print("Stopping download of overview metadata.")
                    break
        return books

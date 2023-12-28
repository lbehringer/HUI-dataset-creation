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

    def __init__ (self, 
                  reader: str,
                  book_name: str, 
                  save_path: str, 
                  chapter_path: str, 
                  hifi_qa_path: str = None,
                  url: str = 'https://librivox.org/', 
                  solo_reading: bool = None, 
                  sections: Union[list, str] = None, 
                  max_chapters_per_reader=None):
        self.reader = reader
        self.book_name = book_name
        self.url = url
        self.save_path = save_path
        self.solo_reading = solo_reading
        self.sections = sections
        self.chapter_path = chapter_path
        self.hifi_qa_path = hifi_qa_path
        self.path_util = PathUtil()
        self.limit_per_iteration = 1000
        self.number_of_iterations = 20
        self.max_chapters_per_reader = max_chapters_per_reader
        self.minimum_upload_timestamp = 1702162800  # 10 December 2023 

    def save(self):
        chapters, download_link_dict = self.get_chapters(self.book_name, get_download_links=True)
        
        Parallel(n_jobs=1)(delayed(self.path_util.copy_file_from_url) \
                           (download_link_dict[key], self.save_path + '/' + download_link_dict[key].split('/')[-1]) for key in sorted(download_link_dict)[:self.max_chapters_per_reader])
        chapters.to_csv(self.chapter_path)

        # save hifi_qa_stats
        hifi_qa_stat_dict = {key: [download_link_dict[key].split("/")[-1].split(".")[0], self.book_name, key, self.reader] for key in sorted(download_link_dict)[:self.max_chapters_per_reader]}
        hifi_qa_df = pd.DataFrame.from_dict(hifi_qa_stat_dict, orient='index', columns=['id', 'book_name', 'section', 'reader'])
        hifi_qa_df.to_csv(self.hifi_qa_path, sep="|", index=False)
        
    def get_chapters(self, book_name: str, get_download_links):
        search_url = self.get_search_url(book_name, self.url)
        response = self.load_search_book(search_url)
        chapter_url = self.extract_chapter_url(response)
        chapter_download_link_dict = self.get_chapter_links(chapter_url) if get_download_links else None
        chapters = pd.read_html(chapter_url)
        return chapters[0], chapter_download_link_dict

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
        # retrieve the chapter-download table for the current book
        soup = bs.BeautifulSoup(search_result.text, 'html.parser')
        parsed_table = soup.find_all('table')[0] 
        # get 5 elements per row: low-quality chapter link, high-quality chapter link, author link, source text link, reader link
        data = [[td.a['href'] if td.find('a') else 
                ''.join(td.stripped_strings)
                for td in row.find_all('td')]
                for row in parsed_table.find_all('tr')]
        # for each row, the section number is the key, and index 1 (the high-quality chapter link) is the value
        # row 0 is an empty list, so we ignore it
        if self.solo_reading:
            download_link_dict = {idx: chapter[1] for idx, chapter in enumerate(data) if len(chapter) > 0}
        else:
            download_link_dict = {idx: chapter[1] for idx, chapter in enumerate(data) if idx in set(self.sections) and len(chapter) > 0}
        return download_link_dict

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

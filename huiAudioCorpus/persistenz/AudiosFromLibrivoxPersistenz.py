import bs4 as bs
import pandas as pd
from huiAudioCorpus.utils.PathUtil import PathUtil
import requests
import json
from tqdm import tqdm
from joblib import Parallel, delayed
from urllib.parse import quote
from typing import Union

class AudiosFromLibrivoxPersistenz:

    def __init__ (self, bookName: str, savePath: str, chapterPath: str, url: str = 'https://librivox.org/', solo_reading: bool = None, sections: Union[list, str] = None, max_chapters_per_reader=None):
        self.bookName = bookName
        self.url = url
        self.savePath = savePath
        self.solo_reading = solo_reading
        self.sections = sections
        self.chapterPath = chapterPath
        self.pathUtil = PathUtil()
        self.limitPerIteration = 1000
        self.numberOfIterations = 20
        self.max_chapters_per_reader = max_chapters_per_reader
        # self.minimumUploadTimestamp = 1625672626 # 7 July 2021 (which is the date of the last change in the HUI repo)
        # self.minimumUploadTimestamp = 1672527600 # 1 January 2023
        # self.minimumUploadTimestamp = 1698184800 # 25 October 2023
        # self.minimumUploadTimestamp = 1699225200 # 6 November 2023
        self.minimumUploadTimestamp = 1702162800 # 10 December 2023 

    def save(self):
        chapters, chapterDownloadLinks = self.getChapters(self.bookName, get_download_links=True)
        Parallel(n_jobs=1)(delayed(self.pathUtil.copyFileFromUrl)(link ,self.savePath+ '/' + link.split('/')[-1]) for link in chapterDownloadLinks[:self.max_chapters_per_reader])
        chapters.to_csv(self.chapterPath)
        

    def getChapters(self, bookName:str, get_download_links):
        """Get all chapters from a book.
        Return a tuple of a DataFrame """
        searchUrl = self.getSearchUrl(bookName, self.url)
        response = self.loadSearchBook(searchUrl)
        chapterUrl = self.extractChapterUrl(response)
        chapterDownloadLinks = self.getChapterLinks(chapterUrl) if get_download_links else None
        chapters = pd.read_html(chapterUrl)
        return chapters[0], chapterDownloadLinks

    def get_catalog_date(self, search_url: str):
        """Returns catalog date as YYYYMMDD formatted int"""

        search_result = requests.get(search_url)
        search_result.encoding = "UTF-8"
        soup = bs.BeautifulSoup(search_result.text, 'html.parser')
        # get the production details in the sidebar
        production_details = soup.find("dl", class_="product-details")
        # catalog date is the third dd element in the production details
        catalog_date = production_details.find_all("dd")[2].text
        return int(catalog_date.replace("-", ""))

    def loadSearchBook(self, url:str ):
        searchResult = requests.get(url)
        return searchResult.text

    def getSearchUrl(self, bookName: str, url:str):
        searchUrl = url + 'api/feed/audiobooks/?format=json&title=' + quote(bookName)
        return searchUrl

    def extractChapterUrl(self, response: str):
        jsonInput = json.loads(response)['books']
        book = jsonInput[0]
        urlZipFile = book['url_librivox']
        return urlZipFile

    def extractZipUrl(self, response: str):
        jsonInput = json.loads(response)['books']
        book = jsonInput[0]
        urlZipFile = book['url_zip_file']
        return urlZipFile

    def getChapterLinks(self, url: str):
        searchResult = requests.get(url)
        searchResult.encoding = "UTF-8"
        soup = bs.BeautifulSoup(searchResult.text, 'html.parser')
        parsed_table = soup.find_all('table')[0] 
        data = [[td.a['href'] if td.find('a') else 
                ''.join(td.stripped_strings)
                for td in row.find_all('td')]
                for row in parsed_table.find_all('tr')]
        if self.solo_reading:
            downloadLinks = [chapter[1] for chapter in data if len(chapter)>0]
        # if book is not solo_reading, download only chapters read by the specified reader
        # index 0 contains empty chapter, so we skip it
        else:
            downloadLinks = [chapter[1] for idx, chapter in enumerate(data) if idx in set(self.sections) and len(chapter)>0]
        return downloadLinks


    def getIds(self, requestUrl=None):
        """Retrieves raw metadata of book IDs (either with a specified or a default requestUrl) 
        and returns a corresponding dictionary."""
        books = []
        if requestUrl:
            print("Using custom request URL for metadata retrieval.")
            page = requests.get(requestUrl)
            page.encoding = "UTF-8"
            try:
                result = json.loads(page.text)
                if 'books' in result:
                    # add catalog date
                    for idx, book in enumerate(result['books']):
                        result['books'][idx]['catalog_date'] = self.get_catalog_date(book['url_librivox'])
                    # result['books'][0]['catalog_date'] = self.get_catalog_date(result['books'][0]['url_librivox'])
                    books.extend(result['books'])
                else:
                    print(result)
                    print("Stopping download of overview metadata.")
            except ValueError as e: # includes JSONDecodeError
                print(f"Error: {e}")
                print("Stopping download of overview metadata.")
        else:
            limit = self.limitPerIteration
            minimum_upload_timestamp = self.minimumUploadTimestamp  # we only want to retrieve books released AFTER this date
            for i in tqdm(range(self.numberOfIterations), desc=f"Performing retrieval in max. {self.numberOfIterations} iterations"):
                requestUrl = f'https://librivox.org/api/feed/audiobooks/?limit={limit}&offset={i*limit}&since={minimum_upload_timestamp}&format=json'
                page = requests.get(requestUrl)
                page.encoding = "UTF-8"
                try:
                    result = json.loads(page.text)
                    if 'books' in result:
                        # add catalog date
                        for idx, book in enumerate(result['books']):
                            result['books'][idx]['catalog_date'] = self.get_catalog_date(book['url_librivox'])
                        # result['books'][0]['catalog_date'] = self.get_catalog_date(result['books'][0]['url_librivox'])
                        # if "catalog_date" not in result["books"][0]:
                        #     raise KeyError
                        books.extend(result['books'])
                
                    else:
                        print(result)
                        print("Stopping download of overview metadata.")
                        break
                except ValueError as e: # includes JSONDecodeError
                    print(f"Error in iteration {i}: {e}")
                    print("Stopping download of overview metadata.")
                    break
        return books
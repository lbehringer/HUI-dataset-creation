import bs4 as bs
import pandas as pd
from huiAudioCorpus.utils.PathUtil import PathUtil
import requests
import json
from tqdm import tqdm
from joblib import Parallel, delayed
from urllib.parse import quote

class AudiosFromLibrivoxPersistenz:

    def __init__ (self, bookName: str, savePath: str, chapterPath: str, url:str = 'https://librivox.org/'):
        self.bookName = bookName
        self.url = url
        self.savePath = savePath
        self.chapterPath = chapterPath
        self.pathUtil = PathUtil()
        self.limitPerIteration = 1000
        self.numberOfIterations = 20
        self.minimumUploadTimestamp = 1625672626 # 7 July 2021 (which is the date of the last change in the HUI repo)
        # self.minimumUploadTimestamp = 1672527600 # 1 January 2023
        # self.minimumUploadTimestamp = 1698184800 # 25 October 2023

    def save(self):
        chapters, chapterDownloadLinks = self.getChapter(self.bookName, get_download_links=True)
        Parallel(n_jobs=4)(delayed(self.pathUtil.copyFileFromUrl)(link ,self.savePath+ '/' + link.split('/')[-1]) for link in chapterDownloadLinks)
        chapters.to_csv(self.chapterPath)
        

    def getChapter(self, bookName:str, get_download_links):
        searchUrl = self.getSearchUrl(bookName, self.url)
        response = self.loadSearchBook(searchUrl)
        chapterUrl = self.extractChapterUrl(response)
        chapterDownloadLinks = self.getChapterLinks(chapterUrl) if get_download_links else None
        chapters = pd.read_html(chapterUrl)
        return chapters[0], chapterDownloadLinks

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
        downloadLinks = [chapter[1] for chapter in data if len(chapter)>0]
        return downloadLinks


    def getIds(self, requestUrl=None):
        """Retrieves raw metadata of book IDs (either with a specified or a default requestUrl) 
        and returns a corresponding dictionary."""
        books = []
        if requestUrl:
            print("Using custom request URL for retrieval.")
            print(requestUrl)
            page = requests.get(requestUrl)
            print(page)
            page.encoding = "UTF-8"
            try:
                result = json.loads(page.text)
                print(result)
                if 'books' in result:
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
            for i in tqdm(range(self.numberOfIterations)):
                requestUrl = f'https://librivox.org/api/feed/audiobooks/?limit={limit}&offset={i*limit}&since={minimum_upload_timestamp}&format=json'
                page = requests.get(requestUrl)
                page.encoding = "UTF-8"
                try:
                    result = json.loads(page.text)
                    if 'books' in result:
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
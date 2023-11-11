

from huiAudioCorpus.utils.PathUtil import PathUtil
from huiAudioCorpus.persistenz.AudiosFromLibrivoxPersistenz import AudiosFromLibrivoxPersistenz
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from tqdm import tqdm
import os

class Step0_Overview:

    def __init__(self, audiosFromLibrivoxPersistenz: AudiosFromLibrivoxPersistenz, savePath: str, pathUtil: PathUtil, requestUrl: str = None):
        self.savePath = savePath
        self.audiosFromLibrivoxPersistenz = audiosFromLibrivoxPersistenz
        self.pathUtil = pathUtil
        self.requestUrl = requestUrl

    def run(self):
        return DoneMarker(self.savePath).run(self.script, deleteFolder=False)
    
    def script(self):
        booksLibrivox = self.downloadOverviewLibrivox(requestUrl=self.requestUrl)
        usableBooks = self.downloadChapters(booksLibrivox)
        speakerOverview_gb = self.generateSpeakerOverview(usableBooks["gutenberg_books"], gutenberg=True)
        speakerOverview_non_gb = self.generateSpeakerOverview(usableBooks["non_gutenberg_books"], gutenberg=False)
        speakerShort_gb = self.generateSpeakerShort(speakerOverview_gb, gutenberg=True)
        speakerShort_non_gb = self.generateSpeakerShort(speakerOverview_non_gb, gutenberg=False)
        speaker_template_gb = self.generateSpeakerTemplate(usableBooks["gutenberg_books"], gutenberg=True)
        speaker_template_non_gb = self.generateSpeakerTemplate(usableBooks["non_gutenberg_books"], gutenberg=False)
        self.generate_reader_template_latest_book(speaker_template_gb, gutenberg=True)
        self.generate_reader_template_latest_book(speaker_template_non_gb, gutenberg=False)

        total_hours_gutenberg = sum([book['time'] for book in usableBooks["gutenberg_books"]])/60/60
        total_hours_others = sum([book['time'] for book in usableBooks["non_gutenberg_books"]])/60/60
        print('Usable books (Gutenberg):', len(usableBooks["gutenberg_books"]))
        print(f'Total hours (Gutenberg): {total_hours_gutenberg}')
        print(f'Total hours (others): {total_hours_others}')
        print(f'Total hours (combined): {total_hours_gutenberg + total_hours_others}')

        num_gb_speakers = len(speakerShort_gb)
        print('Count of Speakers (Gutenberg):', num_gb_speakers)
        if num_gb_speakers > 0:
            print('bestSpeaker (longest recording duration) (Gutenberg):', speakerShort_gb[0])

        num_non_gp_speakers = len(speakerShort_non_gb)
        print('Count of Speakers (others):', num_non_gp_speakers)
        if num_non_gp_speakers > 0:
            print('bestSpeaker (longest recording duration) (others):', speakerShort_non_gb[0])        

    def generate_reader_template_latest_book(self, speaker_template: dict, gutenberg=True):
        """Generates a dict containing only the most recently added book by each speaker and where the corresponding text can be found.
        Writes the dictionary to a JSON file and returns the dict."""
        non_gb_suffix = "" if gutenberg else "_non_gutenberg"
        latest_books_path = os.path.join(self.savePath, f'readerLatestBook{non_gb_suffix}.json')
        latest_books = {}
        for reader, books in speaker_template.items():
            latest_catalog_date = 0
            latest_book = None
            for book, details in books.items():
                catalog_date = details.get("catalog_date")
                if catalog_date > latest_catalog_date:
                    latest_catalog_date = catalog_date
                    latest_book = book
            latest_books[reader] = speaker_template[reader][latest_book]
        self.pathUtil.saveJson(latest_books_path, latest_books)            
        return latest_books

    def downloadOverviewLibrivox(self, requestUrl=None):
        """Downloads Librivox metadata and generates a corresponding dictionary. 
        Writes the dictionary to a JSON file and returns the dict."""
        librivoxPath = self.savePath + '/booksLibrivox.json'
        if not os.path.isfile(librivoxPath):
            print('Download Overview from Librivox')
            booksLibrivox  = self.audiosFromLibrivoxPersistenz.getIds(requestUrl=requestUrl)
            self.pathUtil.saveJson(librivoxPath, booksLibrivox)

        booksLibrivox = self.pathUtil.loadJson(librivoxPath)
        return booksLibrivox

    def downloadChapters(self, booksLibrivox, only_use_gutenberg_books=False):
        """Downloads chapter-wise metadata of usable books and generates a corresponding dictionary. 
        Differentiates between book texts hosted by gutenberg.org/projekt-gutenberg.org and other websites.
        Writes the dictionary to a JSON file and returns the dict."""

        gutenberg_books_path = self.savePath + '/usable_gutenberg_books.json'
        non_gutenberg_books_path = self.savePath + '/usable_non_gutenberg_books.json'
        files_already_exist = os.path.isfile(gutenberg_books_path) or \
            (not only_use_gutenberg_books and os.path.isfile(non_gutenberg_books_path))
        
        gutenberg_books = []
        non_gutenberg_books = []

        if not files_already_exist:
            print('Download Chapters from Librivox')

            for book in booksLibrivox:
                if self.isBookUseable(book):
                    book_metadata = {'time': book['totaltimesecs'], 'title':book['title'], 'url': book['url_text_source'], 'catalog_date': book['catalog_date']}
                    # retrieve books with gutenberg-hosted texts
                    if self.text_hosted_by_gutenberg(book):
                        gutenberg_books.append(book_metadata)
                    # if specified, also retrieve books with texts hosted by other websites
                    elif not only_use_gutenberg_books:
                        non_gutenberg_books.append(book_metadata)

            print(f"Retrieved {len(gutenberg_books)} books with Gutenberg-hosted texts.")
            print(f"Retrieved {len(non_gutenberg_books)} books with texts from other hosts.")

            # write gutenberg-hosted books to file
            print("Processing books with Gutenberg-hosted texts.")
            for book in tqdm(gutenberg_books):
                chapters, _ = self.audiosFromLibrivoxPersistenz.getChapter(book['title'], get_download_links=False)
                book['chapters'] = []
                for _, chapter in chapters.iterrows():
                    book['chapters'].append({
                        'title': chapter['Chapter'],
                        'reader': chapter['Reader'],
                        'time': convertToSeconds(chapter['Time'])
                    })
            self.pathUtil.saveJson(gutenberg_books_path, gutenberg_books)

            # if specified, write non-gutenberg-hosted books to file
            if not only_use_gutenberg_books:
                print("Processing books with texts from other hosts.")
                for book in tqdm(non_gutenberg_books):
                    chapters, _ = self.audiosFromLibrivoxPersistenz.getChapter(book['title'], get_download_links=False)
                    book['chapters'] = []
                    for _, chapter in chapters.iterrows():
                        book['chapters'].append({
                            'title': chapter['Chapter'],
                            'reader': chapter['Reader'],
                            'time': convertToSeconds(chapter['Time'])
                        })
                self.pathUtil.saveJson(non_gutenberg_books_path, non_gutenberg_books)

        else:
            print("Loading existing files.")
            if not os.path.isfile(gutenberg_books_path):
                gutenberg_books = self.pathUtil.loadJson(gutenberg_books_path)
            if not only_use_gutenberg_books and not os.path.isfile(non_gutenberg_books_path):
                non_gutenberg_books = self.pathUtil.loadJson(gutenberg_books_path)
                
        return {"gutenberg_books": gutenberg_books, "non_gutenberg_books": non_gutenberg_books}


    def text_hosted_by_gutenberg(self, book):
        """Determines whether or not the text of a book is hosted by a Gutenberg website.
        Returns a boolean."""
        if 'www.projekt-gutenberg.org' in book['url_text_source']:
            return True
        if 'www.gutenberg.org/' in book['url_text_source']:
            return True
        return False

    def isBookUseable(self, book):
        """Determines whether or not a book is usable for dataset creation.
        Returns a boolean."""
        if book['totaltimesecs']<=0:
            return False
        if book['language'] != "English":
            return False
        return True
    
    def generateSpeakerTemplate(self, usableBooks, gutenberg=True):
        """Generates a dict containing the books read by each speaker and where the corresponding text can be found.
        Writes the dictionary to a JSON file and returns the dict."""

        print("Generate speaker template")
        filename = "readerTemplate.json" if gutenberg else "readerTemplate_non_gutenberg.json"
        readerPath = os.path.join(self.savePath, filename)
        if not os.path.isfile(readerPath):
            reader = {}
            for book in usableBooks:
                bookTitle = book['title']
                catalog_date = book['catalog_date']
                for chapter in book['chapters']:
                    if chapter['reader'] not in reader:
                        reader[chapter['reader']] = {}

                    title = ''.join([i for i in bookTitle.lower().replace(' ','_') if (i in 'abcdefghijklmonpqrstuvwxyz_' or i.isnumeric())])

                    # Gutenberg-hosted books
                    if gutenberg:
                        gutenbergId = book['url'].replace('https://','').replace('http://','').replace('www.', '').replace('projekt-gutenberg.org/', '')
                        if gutenbergId.startswith('gutenberg.org/'):
                            if "/cache/epub/" in gutenbergId:
                                gutenbergId = int(gutenbergId.split("/")[3])
                            elif "/files/" in gutenbergId:
                                gutenbergId = int(gutenbergId.split("/")[2])
                            else:
                                gutenbergId = int(gutenbergId.replace('gutenberg.org/ebooks/', '').replace('gutenberg.org/etext/', '').rstrip("/"))

                    # non-Gutenberg-hosted books
                    else:
                        gutenbergId = book["url"].replace("https://", "").replace("http://", "")

                    reader[chapter['reader']][title] = {
                        'title': title,
                        'librivox_book_name': bookTitle,
                        'gutenberg_id': gutenbergId,
                        'catalog_date': catalog_date,
                        'gutenberg_start': '',
                        'gutenberg_end': '',
                        'text_replacement':{}
                    }



            self.pathUtil.saveJson(readerPath, reader)
        reader = self.pathUtil.loadJson(readerPath)
        return reader

    def generateSpeakerOverview(self, usableBooks, gutenberg=True):
        """Generates a dictionary containing chapter-wise recording time (in SECONDS) of each speaker.
        Writes the dictionary to a JSON file and returns the dict."""        
        filename = "readerLong.json" if gutenberg else "readerLong_non_gutenberg.json"
        readerPath = os.path.join(self.savePath, filename)
        if not os.path.isfile(readerPath):
            readers = {}
            for book in usableBooks:
                bookTitle = book['title']
                for chapter in book['chapters']:
                    if chapter['reader'] not in readers:
                        readers[chapter['reader']] = []

                    readers[chapter['reader']].append({
                        'title': chapter['title'],
                        'time': chapter['time'],
                        'book': bookTitle
                    })
            self.pathUtil.saveJson(readerPath, readers)
        readers = self.pathUtil.loadJson(readerPath)
        return readers

    def generateSpeakerShort(self, speakerOverview, gutenberg=True):
        """Generates a dictionary containing the rounded recording time (in HOURS) of each speaker, sorted from longest to shortest time.
        Writes the dict to a JSON file and returns the dict."""
        filename = "readerShort.json" if gutenberg else "readerShort_non_gutenberg.json"
        readerPath = os.path.join(self.savePath, filename)
        
        if not os.path.isfile(readerPath):
            readers = []
            for speaker in speakerOverview:
                readers.append({
                    'name': speaker,
                    'time': round(sum([chapter['time'] for chapter in speakerOverview[speaker]])/60/60,1) # time in hours
                })
            readers.sort(key=lambda x: x['time'], reverse=True)
            self.pathUtil.saveJson(readerPath, readers)
        readers = self.pathUtil.loadJson(readerPath)
        return readers


def convertToSeconds(timeString: str):
    ftr = [3600,60,1]
    duration = sum([a*b for a,b in zip(ftr, map(int,timeString.split(':')))])
    return duration
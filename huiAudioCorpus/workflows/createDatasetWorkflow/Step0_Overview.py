from huiAudioCorpus.utils.PathUtil import PathUtil
from huiAudioCorpus.persistence.AudiosFromLibrivoxPersistence import AudiosFromLibrivoxPersistence
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from tqdm import tqdm
import os

class Step0_Overview:

    def __init__(self, audios_from_librivox_persistence: AudiosFromLibrivoxPersistence, save_path: str, path_util: PathUtil, request_url: str = None, language: str ="English"):
        self.save_path = save_path
        self.audios_from_librivox_persistence = audios_from_librivox_persistence
        self.path_util = path_util
        self.request_url = request_url
        self.language = language

    def run(self):
        return DoneMarker(self.save_path).run(self.script, delete_folder=False)
    
    def script(self):
        books_librivox = self.download_overview_librivox(request_url=self.request_url)
        usable_books = self.download_chapters(books_librivox)
        speaker_overview_gb = self.generate_speaker_overview(usable_books["gutenberg_books"], gutenberg=True)
        speaker_overview_non_gb = self.generate_speaker_overview(usable_books["non_gutenberg_books"], gutenberg=False)
        speaker_short_gb = self.generate_speaker_short(speaker_overview_gb, gutenberg=True)
        speaker_short_non_gb = self.generate_speaker_short(speaker_overview_non_gb, gutenberg=False)
        speaker_template_gb = self.generate_speaker_template(usable_books["gutenberg_books"], gutenberg=True)
        speaker_template_non_gb = self.generate_speaker_template(usable_books["non_gutenberg_books"], gutenberg=False)
        self.generate_reader_template_latest_book(speaker_template_gb, gutenberg=True)
        self.generate_reader_template_latest_book(speaker_template_non_gb, gutenberg=False)

        total_hours_gutenberg = sum([book['time'] for book in usable_books["gutenberg_books"]])/60/60
        total_hours_others = sum([book['time'] for book in usable_books["non_gutenberg_books"]])/60/60
        print('Usable books (Gutenberg):', len(usable_books["gutenberg_books"]))
        print(f'Total hours (Gutenberg): {total_hours_gutenberg}')
        print(f'Total hours (others): {total_hours_others}')
        print(f'Total hours (combined): {total_hours_gutenberg + total_hours_others}')

        num_gb_speakers = len(speaker_short_gb)
        print('Count of Speakers (Gutenberg):', num_gb_speakers)
        if num_gb_speakers > 0:
            print('bestSpeaker (longest recording duration) (Gutenberg):', speaker_short_gb[0])

        num_non_gp_speakers = len(speaker_short_non_gb)
        print('Count of Speakers (others):', num_non_gp_speakers)
        if num_non_gp_speakers > 0:
            print('bestSpeaker (longest recording duration) (others):', speaker_short_non_gb[0])        

    def generate_reader_template_latest_book(self, speaker_template: dict, gutenberg=True):
        """Generates a dict containing only the most recently added book by each speaker and where the corresponding text can be found.
        Writes the dictionary to a JSON file and returns the dict."""
        non_gb_suffix = "" if gutenberg else "_non_gutenberg"
        latest_books_path = os.path.join(self.save_path, f'readerLatestBook{non_gb_suffix}.json')
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
        self.path_util.save_json(latest_books_path, latest_books)            
        return latest_books

    def download_overview_librivox(self, request_url=None):
        """Downloads Librivox metadata and generates a corresponding dictionary. 
        Writes the dictionary to a JSON file and returns the dict."""
        librivox_path = self.save_path + '/booksLibrivox.json'
        if not os.path.isfile(librivox_path):
            print('Download Overview from Librivox')
            books_librivox  = self.audios_from_librivox_persistence.get_ids(request_url=request_url)
            self.path_util.save_json(librivox_path, books_librivox)

        books_librivox = self.path_util.load_json(librivox_path)
        return books_librivox

    def download_chapters(self, books_librivox, only_use_gutenberg_books=False):
        """Downloads chapter-wise metadata of usable books and generates a corresponding dictionary. 
        Differentiates between book texts hosted by gutenberg.org/projekt-gutenberg.org and other websites.
        Writes the dictionary to a JSON file and returns the dict."""

        gutenberg_books_path = self.save_path + '/usable_gutenberg_books.json'
        non_gutenberg_books_path = self.save_path + '/usable_non_gutenberg_books.json'
        files_already_exist = os.path.isfile(gutenberg_books_path) or \
            (not only_use_gutenberg_books and os.path.isfile(non_gutenberg_books_path))
        
        gutenberg_books = []
        non_gutenberg_books = []

        if not files_already_exist:
            print('Download Chapters from Librivox')

            for book in books_librivox:
                if self.is_book_useable(book):
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
                chapters, _ = self.audios_from_librivox_persistence.get_chapters(book['title'], get_download_links=False)
                book['chapters'] = []
                for idx, chapter in chapters.iterrows():
                    book['chapters'].append({
                        'section': idx+1,
                        'title': chapter['Chapter'],
                        'reader': chapter['Reader'],
                        'time': convert_to_seconds(chapter['Time'])
                    })
            self.path_util.save_json(gutenberg_books_path, gutenberg_books)

            # if specified, write non-gutenberg-hosted books to file
            if not only_use_gutenberg_books:
                print("Processing books with texts from other hosts.")
                for book in tqdm(non_gutenberg_books):
                    chapters, _ = self.audios_from_librivox_persistence.get_chapters(book['title'], get_download_links=False)
                    book['chapters'] = []
                    for idx, chapter in chapters.iterrows():
                        book['chapters'].append({
                            'section': idx+1,
                            'title': chapter['Chapter'],
                            'reader': chapter['Reader'],
                            'time': convert_to_seconds(chapter['Time'])
                        })
                self.path_util.save_json(non_gutenberg_books_path, non_gutenberg_books)

        else:
            print("Loading existing files.")
            if not os.path.isfile(gutenberg_books_path):
                gutenberg_books = self.path_util.load_json(gutenberg_books_path)
            if not only_use_gutenberg_books and not os.path.isfile(non_gutenberg_books_path):
                non_gutenberg_books = self.path_util.load_json(gutenberg_books_path)
                
        return {"gutenberg_books": gutenberg_books, "non_gutenberg_books": non_gutenberg_books}


    def text_hosted_by_gutenberg(self, book):
        """Determines whether or not the text of a book is hosted by a Gutenberg website.
        Returns a boolean."""
        if 'www.projekt-gutenberg.org' in book['url_text_source']:
            return True
        if 'www.gutenberg.org/' in book['url_text_source']:
            return True
        return False

    def is_book_useable(self, book):
        """Determines whether or not a book is usable for dataset creation.
        Returns a boolean."""
        if book['totaltimesecs']<=0:
            return False
        if book['language'] != self.language:
            return False
        return True
    
    def generate_speaker_template(self, usable_books, gutenberg=True):
        """Generates a dict containing the books read by each speaker and where the corresponding text can be found.
        Writes the dictionary to a JSON file and returns the dict."""

        print("Generate speaker template")
        filename = "readerTemplate.json" if gutenberg else "readerTemplate_non_gutenberg.json"
        reader_path = os.path.join(self.save_path, filename)
        if not os.path.isfile(reader_path):
            reader = {}
            for book in usable_books:
                book_title = book['title']
                catalog_date = book['catalog_date']
                # determine if book is solo or collaborative reading
                readers = set([chapter['reader'] for chapter in book['chapters']])
                solo_reading = len(readers) == 1
                for chapter in book['chapters']:
                    if chapter['reader'] not in reader:
                        reader[chapter['reader']] = {}

                    title = ''.join([i for i in book_title.lower().replace(' ','_') if (i in 'abcdefghijklmonpqrstuvwxyz_' or i.isnumeric())])

                    # Gutenberg-hosted books
                    if gutenberg:
                        gutenberg_id = book['url'].replace('https://','').replace('http://','').replace('www.', '').replace('projekt-gutenberg.org/', '')
                        if gutenberg_id.startswith('gutenberg.org/'):
                            if "/cache/epub/" in gutenberg_id:
                                gutenberg_id = int(gutenberg_id.split("/")[3])
                            elif "/files/" in gutenberg_id:
                                gutenberg_id = int(gutenberg_id.split("/")[2])
                            else:
                                gutenberg_id = int(gutenberg_id.replace('gutenberg.org/ebooks/', '').replace('gutenberg.org/etext/', '').rstrip("/"))

                    # non-Gutenberg-hosted books
                    else:
                        gutenberg_id = book["url"].replace("https://", "").replace("http://", "")

                    if title not in reader[chapter['reader']]:
                        reader[chapter['reader']][title] = {
                            'solo_reading': solo_reading,
                            'sections': 'all' if solo_reading else [chapter['section']],
                            'reader': chapter['reader'],
                            'title': title,
                            'librivox_book_name': book_title,
                            'gutenberg_id': gutenberg_id,
                            'catalog_date': catalog_date,
                            'gutenberg_start': '',
                            'gutenberg_end': '',
                            'text_replacement':{}
                        }
                    else:
                        if not solo_reading:
                            reader[chapter['reader']][title]['sections'].append(chapter['section'])



            self.path_util.save_json(reader_path, reader)
        reader = self.path_util.load_json(reader_path)
        return reader

    def generate_speaker_overview(self, usable_books, gutenberg=True):
        """Generates a dictionary containing chapter-wise recording time (in SECONDS) of each speaker.
        Writes the dictionary to a JSON file and returns the dict."""        
        filename = "readerLong.json" if gutenberg else "readerLong_non_gutenberg.json"
        reader_path = os.path.join(self.save_path, filename)
        if not os.path.isfile(reader_path):
            readers = {}
            for book in usable_books:
                book_title = book['title']
                for chapter in book['chapters']:
                    if chapter['reader'] not in readers:
                        readers[chapter['reader']] = []

                    readers[chapter['reader']].append({
                        'title': chapter['title'],
                        'time': chapter['time'],
                        'book': book_title
                    })
            self.path_util.save_json(reader_path, readers)
        readers = self.path_util.load_json(reader_path)
        return readers

    def generate_speaker_short(self, speaker_overview, gutenberg=True):
        """Generates a dictionary containing the rounded recording time (in HOURS) of each speaker, sorted from longest to shortest time.
        Writes the dict to a JSON file and returns the dict."""
        filename = "readerShort.json" if gutenberg else "readerShort_non_gutenberg.json"
        reader_path = os.path.join(self.save_path, filename)
        
        if not os.path.isfile(reader_path):
            readers = []
            for speaker in speaker_overview:
                readers.append({
                    'name': speaker,
                    'time': round(sum([chapter['time'] for chapter in speaker_overview[speaker]])/60/60,1) # time in hours
                })
            readers.sort(key=lambda x: x['time'], reverse=True)
            self.path_util.save_json(reader_path, readers)
        readers = self.path_util.load_json(reader_path)
        return readers


def convert_to_seconds(time_string: str):
    ftr = [3600,60,1]
    duration = sum([a*b for a,b in zip(ftr, map(int,time_string.split(':')))])
    return duration

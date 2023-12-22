from typing import List, Union
import re
from bs4 import BeautifulSoup
import requests
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers
from huiAudioCorpus.utils.PathUtil import PathUtil

class GutenbergBookPersistence:

    def __init__(self, text_id: Union[str, int], save_path: str):
        self.text_id = text_id
        self.save_path = save_path
        self.path_util = PathUtil()
        self.gutenberg_projekt_download = GutenbergProjektDownload()
        self.gutenberg_download = GutenbergDownload()

    def save(self):
        if isinstance(self.text_id, str):
            text = self.gutenberg_projekt_download.download_text(self.text_id)
        else:
            text = self.gutenberg_download.download_text(self.text_id)
        self.path_util.write_file(text, self.save_path)


class GutenbergProjektDownload:

    def __init__(self):
        self.base_link = 'https://www.projekt-gutenberg.org/'

    def get_ids(self):
        works_link = self.base_link + "info/texte/allworka.html"
        works = requests.get(works_link)
        works.encoding = "UTF-8"
        works_soup = BeautifulSoup(works.text, "html.parser")

        books = []
        last_name = ''
        first_name = ''
        elements = works_soup.find("dl")
        for element in elements:
            if element.name == 'dt':
                current_author = element.text
                if ', ' in current_author:
                    names = current_author.split(', ')
                    first_name = names[-1]
                    last_name = names[0]
                else:
                    last_name = current_author
                    first_name = ''

            if element.name == 'dd':
                link = element.find("a")
                if link is not None:
                    book_id = link["href"][5:]
                    book_name = link.text
                    books.append({
                        'name': book_name,
                        'first_name': first_name,
                        'last_name': last_name,
                        'id': book_id
                    })
        return books

    def download_text(self, text_id: str):
        link = self.base_link + text_id
        full_text = ''
        while link is not None:
            paragraphs, link = self.download_page(link)
            prepared_paragraph = self.prepare_paragraph(paragraphs)
            full_text += prepared_paragraph
        return full_text

    def download_page(self, link: str):
        page = requests.get(link)
        page.encoding = "UTF-8"
        page_soup = BeautifulSoup(page.text, "html.parser")
        paragraphs = page_soup.find('p').find_all("p")
        if len(paragraphs) == 0:
            paragraphs = page_soup.find_all("p", class_=None)

        next_link = None
        if len(page_soup.find_all("a", text=re.compile("weiter\s*>>"))) > 0:
            direct_link = page_soup.find("a", text=re.compile("weiter\s*>>"))["href"]
            next_link = page.url.split("/")
            next_link.pop()
            next_link.append(direct_link)
            next_link = "/".join(next_link)
        return paragraphs, next_link

    def prepare_paragraph(self, paragraphs: List):
        extracted_paragraphs = ''
        for paragraph in paragraphs:
            if paragraph.text:
                for footnote in paragraph.select('span'):
                    footnote.extract()

                if len(paragraph.text) > 0 and len(paragraph.contents) == 1:
                    extracted_paragraph = re.sub(r" +", r" ",
                                                 paragraph.text.replace("\t", " ").replace("\n", " "))
                    extracted_paragraphs += extracted_paragraph.strip() + "\n"
        return extracted_paragraphs


class GutenbergDownload:

    def download_text(self, text_id: int):
        text = strip_headers(
            load_etext(text_id, mirror='http://www.mirrorservice.org/sites/ftp.ibiblio.org/pub/docs/books/gutenberg/')
        ).strip()
        return text

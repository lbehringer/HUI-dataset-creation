
from huiAudioCorpus.utils.PathUtil import PathUtil
from typing import Dict, List
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.calculator.TextNormalizer import TextNormalizer

import re
import json
from nemo_text_processing.text_normalization import Normalizer

class Step3_1_PrepareText:

    def __init__(self, save_path: str, load_file: str, save_file: str, start_sentence: str, end_sentence: str, text_replacement: Dict[str, str], text_normalizer: TextNormalizer, moves: List[Dict[str, str]], remove: List[Dict[str, str]], language: str = "en"):
        self.save_path = save_path
        self.text_normalizer = text_normalizer
        self.load_file = load_file
        self.save_file = save_file
        # order replacements in reverse length order to match longest replacement strings first (e.g. "xii" before "xi")
        self.text_replacement = {k: text_replacement[k] for k in sorted(text_replacement, key=len, reverse=True)}
        self.path_util = PathUtil()
        self.start_sentence = start_sentence
        self.end_sentence = end_sentence
        self.moves = moves
        self.removes = remove
        self.language = language

    def run(self):
        return DoneMarker(self.save_path).run(self.script)
    
    def script(self):
        input_text = self.path_util.load_file(self.load_file)
        cut_text = self.cut_text(input_text, self.start_sentence, self.end_sentence)
        removed_text = self.remove(cut_text, self.removes)
        if self.language == "en" or "de":
            # TODO: Figure out why Nemo Normalizer doesn't work in French
            lines = removed_text.split("\n")
            nemo_norm = Normalizer(
                input_case="cased", 
                lang=self.language,
                )
            normalized = nemo_norm.normalize_list(lines)
            normalized = "\n".join(normalized)
            moved_text = self.move(normalized, self.moves)
        else:
            replaced_text = self.replace(removed_text, self.text_replacement)
            moved_text = self.move(replaced_text, self.moves)
        self.path_util.write_file(moved_text, self.save_file)

    def move(self, text: str, moves: List[Dict[str, str]]):
        for move in moves:
            start = move['start']
            end = move['end']
            after = move['after']
            text_to_move = text.partition(start)[-1].partition(end)[0] + end
            text_without_move = text.replace(text_to_move, "")
            first, separator, last = text_without_move.partition(after)
            final_text = first + separator + text_to_move + last
            text = final_text
        return text

    def remove(self, text: str, removes: List[Dict[str, str]]):
        # Iterate through the list of dictionaries containing removal information
        for remove in removes:
            text_to_remove = ""
            text_to_remove_old = None
            start = remove['start']
            end = remove['end']
            
            # Continue the loop until the removed text stops changing
            while text_to_remove != text_to_remove_old:
                # Construct the text to be removed by extracting content between 'start' and 'end'
                text_to_remove = start + text.partition(start)[-1].partition(end)[0] + end
                
                # Remove the constructed text from the original text
                text = text.replace(text_to_remove, "")
                
                # Update the old value for comparison in the next iteration
                text_to_remove_old = text_to_remove
                
                # Print the removed text for debugging purposes (you may remove this line in production)
                print(text_to_remove)
        
        # Return the modified text after all removals
        return text


    def cut_text(self, text: str, start_sentence: str, end_sentence: str):
        # Check if start_sentence is empty
        if start_sentence == "":
            without_first = text
        else:
            # If start_sentence is not empty, extract text after the start_sentence
            without_first = start_sentence + text.split(start_sentence, 1)[1]

        # Check if end_sentence is empty
        if end_sentence == "":
            without_end = without_first
        else:
            # If end_sentence is not empty, extract text before the end_sentence
            without_end = without_first.split(end_sentence, 1)[0] + end_sentence

        # Remove leading and trailing whitespaces
        stripped = without_end.strip()

        # Replace carriage return ('\r') characters with an empty string
        prepared = stripped.replace('\r', '')

        # Return the prepared text
        return prepared

    def replace(self, text: str, text_replacement: Dict[str, str]):
        before_replacement = {
            '\xa0': ' '
        }
        base_replacement = {
            '...': '.',
            '«': ' ',
            '»': ' ',
            '--': ' ',
            #"'": '',
            '"': ' ',
            '_': ' ',
            #'-': ' ',
            ':': ':',
            '’': ' ',
            '‘': ' ',
            '<': ' ',
            '>': ' ',
            '›': ' ',
            '‹': ' ',
            '^': ' ',
            '|': ' ',
            '+': 'plus',
            # replace em-dash by dash
            '–': '-',

            # omit punctuation replacments
            # '-': ' ',
            # ';': ',',
            #'(': ' ',
            #')': ' ',

            # omit French char replacements
            # 'é': 'e',
            # 'ê': 'e',
            # 'è': 'e',
            # 'à': 'a',
            # 'á': 'a',
            # 'œ': 'oe',
            # 'Ç': 'C',
            # 'ç': 'c

        }
        
        # Replacement rules for abbreviations
        abbreviations = {
            ' H. v.': ' Herr von ',
            '†': ' gestorben ',
            ' v.': ' von ',
            '§': ' Paragraph ',
            ' geb.': ' geboren ',
            ' u.': ' und ',
            '&': ' und ',
            ' o.': ' oder ',
            ' Nr.': ' Nummer ',
            ' Pf.': ' Pfennig ',
            ' Mk.': ' Mark ',
            " Sr. Exz.": " seiner exzellenz ",
            " Kgl.": " königlich ",
            " Dr.": ' Doktor ',
            ' Abb.': ' Abbildung ',
            ' Abh.': ' Abhandlung ',
            ' Abk.': ' Abkürzung ',
            ' allg.': ' allgemein ',
            ' bes.': ' besonders ',
            ' bzw.': ' beziehungsweise ',
            ' geb.': ' geboren ',
            ' gegr.': ' gegründet ',
            ' jmd.': ' jemand ',
            ' o. Ä.': ' oder Ähnliches ',
            ' u. a.': ' unter anderem ',
            ' o.Ä.': ' oder Ähnliches ',
            ' u.a.': ' unter anderem ',
            ' ugs.': ' umgangssprachlich ',
            ' urspr.': ' ursprünglich ',
            ' usw.': '  und so weiter',
            ' u. s. w.': ' und so weiter ',
            ' u.s.w.': ' und so weiter ',
            ' zz.': ' zurzeit ',
            ' dt.': '  deutsch',
            ' ev.': ' evangelisch ',
            ' Jh.': ' Jahrhundert ',
            ' kath.': ' katholisch ',
            ' lat.': ' lateinisch ',
            ' luth.': ' lutherisch ',
            ' Myth.': ' Mythologie ',
            ' natsoz.': ' nationalsozialistisch ',
            ' n.Chr.': ' nach Christus ',
            ' n. Chr.': ' nach Christus ',
            ' relig.': ' religiös ',
            ' v. Chr.': ' vor Christus ',
            ' v.Chr.': ' vor Christus ',
            ' Med.': ' Medizin ',
            ' Mio.': ' Millionen ',
            ' d.h.': ' das heißt ',
            ' d. h.': ' das heißt ',
            ' Abb.': ' Abbildung ',
            ' f.': ' folgende ',
            ' ff.': ' folgende ',
            ' ggf.': ' gegebenfalls ',
            ' i. Allg.': ' im Allgemeinen ',
            ' i. d. R.': ' in der Regel ',
            ' i.Allg.': ' im Allgemeinen ',
            ' i.d.R.': ' in der Regel ',
            ' lt.': ' laut ',
            ' m.': ' mit ',
            ' od.': ' oder ',
            ' s. o.': ' siehe oben ',
            ' s. u.': ' siehe unten ',
            ' s.o.': ' siehe oben ',
            ' s.u.': ' siehe unten ',
            ' Std.': ' Stunde ',
            ' tägl.': ' täglich ',
            ' Tsd.': ' Tausend ',
            ' tsd.': ' tausend ',
            ' v.': ' von ',
            ' z. B.': ' zum Beispiel ',
            ' z.B.': ' zum Beispiel ',
            ' Z. B.': ' zum Beispiel ',
            ' Z.B.': ' zum Beispiel ',
            ' Bsp.': ' Beispiel ',
            ' bzgl.': ' bezüglich ',
            ' ca.': ' circa ',
            ' dgl.': ' dergleichen ',
            ' etc.': ' et cetera ',
            ' evtl.': ' eventuell ',
            ' z.T.': ' zum Teil ',
            ' z. T.': ' zum Teil ',
            ' zit.': ' zitiert ',
            ' zzgl.': ' zuzüglich ',
            ' H. ': ' Herr ',
            ' N. N.': ' so und so ',
            ' N.N.': ' so und so ',
            ' u.s.f.': ' und so fort',
            ' u. s. f.': ' und so fort',
            ' von Ew.': ' von euerer ',
            ' Se.': ' seine ',
            ' St.': ' Sankt ',
            ' inkl.': ' inklusive ',
            'U.S.A.': ' U S A ',
            ' d. J': 'des Jahres ',
            'G.m.b.H.': ' GmbH ',
            ' Mr.': ' Mister ',
            '°': ' Grad ',
            ' m. E.': ' meines Erachtens ',
            ' m.E.': ' meines Erachtens ',
            ' Ew.': ' Eure ',
            ' a.O.': ' an der Oder ',
            ' d.': ' der ',
            ' Ev.': ' Evangelium ',
            ' Sr.': ' seiner ',
            ' hl.': ' heilige ',
            ' Hr.': ' Herr ',
            'd.i.': ' das ist ',
            ' Aufl.': ' Auflage ',
            "A. d. Üb.":" Anmerkung der Übersetzerin ",
            " gest.": " gestorben "
    

            
        }
        for input, target in before_replacement.items():
            text = text.replace(input, target)
        for input, target in text_replacement.items():
            text = text.replace(input, target)
        for input, target in base_replacement.items():
            text = text.replace(input, target)

        self.path_util.write_file(text, self.save_file)

        ############################################################################
        ###  Check for remaining numbers, abbreviations, and unwanted characters ###
        ############################################################################

        remaining_numbers = [s for s in text.split() if bool(re.search(r'\d', s))]
        if len(remaining_numbers) > 0:
            print('there are remaining numbers inside the text')
            print(remaining_numbers)
            replacements = {}
            for text in remaining_numbers:
                replacements[text] = self.text_normalizer.normalize(text)
            replacements = dict(sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True))
            print(json.dumps(replacements, indent=4, ensure_ascii=False))

            raise Exception('there are remaining numbers inside the text')

        remaining_abbreviations = [ab for ab in abbreviations.keys() if ab in text]
        if len(remaining_abbreviations) > 0:
            print('there are remaining abbreviations inside the text')
            print(remaining_abbreviations)
            replacements = {key: value for (key, value) in abbreviations.items() if key in remaining_abbreviations}
            replacements = dict(sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True))
            print(json.dumps(replacements, indent=4, ensure_ascii=False))
            raise Exception('there are remaining abbreviations inside the text')

        a_to_z = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        possible_abbreviations = [' ' + char + '.' for char in 'abcdefghijklmnopqrstuvwxyz' if ' ' + char + '.' in text] + [' ' + char + char2 + '.' for char in a_to_z for char2 in a_to_z if ' ' + char + char2 + '.' in text]
        # shortWords = [' Co.', ' go.', ' Da.',' na.',' ab.', ' an.', ' da.', ' du.', ' er.', ' es.', ' ja.', ' so.', ' um.', ' zu.', ' Ja.', ' Ad.', ' je.', ' Es.', ' ob.', ' is.', ' tu.', ' Hm.', ' So.', ' wo.', ' ha.', ' he.', ' Du.', ' du.', ' Nu.', ' in.']
        # add French short words
        short_words = [' y.', ' ci.', ' en.', ' et.', ' il.', ' la.', ' nu.', ' on.', ' pu.', ' un.', ' va.', ' vu.', ' Co.', ' go.', ' Da.',' na.',' ab.', ' an.', ' da.', ' du.', ' er.', ' es.', ' ja.', ' so.', ' um.', ' zu.', ' Ja.', ' Ad.', ' je.', ' Es.', ' ob.', ' is.', ' tu.', ' Hm.', ' So.', ' wo.', ' ha.', ' he.', ' Du.', ' du.', ' Nu.', ' in.']
        possible_abbreviations = [ab for ab in possible_abbreviations if ab not in short_words]
        if len(possible_abbreviations) > 0:
            print('there are remaining possible abbreviations inside the text')
            print(possible_abbreviations)
            raise Exception('there are remaining possible abbreviations inside the text')
        
        #allowedChars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ äöüßÖÄÜ .,;?!:" \n'
        # allow French chars
                    # 'é': 'e',
            # 'ê': 'e',
            # 'è': 'e',
            # 'à': 'a',
            # 'á': 'a',
            # 'œ': 'oe',
            # 'Ç': 'C'

        allowed_chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ äöüßÖÄÜ .,;?!: ;-() éêèàâáíîìóôòúûùœÇçÁÂÀÉÊÈÔëïæ" \n\''
        remaining_not_allowed_chars = [char for char in text if char not in allowed_chars]
        if len(remaining_not_allowed_chars) > 0:
            print('there are remaining unallowed chars inside the text')
            print(remaining_not_allowed_chars)
            raise Exception('there are remaining unallowed chars inside the text')
        return text

from huiAudioCorpus.utils.PathUtil import PathUtil
from typing import Dict, List
from huiAudioCorpus.utils.DoneMarker import DoneMarker
from huiAudioCorpus.calculator.TextNormalizer import TextNormalizer

import re
import json
from nemo_text_processing.text_normalization import Normalizer

class Step3_1_PrepareText:

    def __init__(self, savePath: str, loadFile: str, saveFile: str, startSentence: str, endSentence: str, textReplacement: Dict[str,str],  textNormalizer: TextNormalizer, moves: List[Dict[str, str]], remove: List[Dict[str, str]], language: str = "en"):
        self.savePath = savePath
        self.textNormalizer = textNormalizer
        self.loadFile = loadFile
        self.saveFile = saveFile
        # order replacements in reverse length order to match longest replacement strings first (e.g. "xii" before "xi")
        self.textReplacement = {k: textReplacement[k] for k in sorted(textReplacement, key=len, reverse=True)}
        self.pathUtil = PathUtil()
        self.startSentence = startSentence
        self.endSentence = endSentence
        self.moves = moves
        self.removes = remove
        self.language = language

    def run(self):
        return DoneMarker(self.savePath).run(self.script)
    
    def script(self):
        inputText = self.pathUtil.loadFile(self.loadFile)
        cuttedText = self.cutText(inputText, self.startSentence , self.endSentence)
        removedText = self.remove(cuttedText, self.removes)
        # TODO: Figure out why Nemo Normalizer doesn't work in French
        # lines = removedText.split("\n")
        # nemo_norm = Normalizer(
        #     input_case="cased", 
        #     lang=self.language,
        #     )
        # normalized = nemo_norm.normalize_list(lines)
        # normalized = "\n".join(normalized)
        # movedText = self.move(normalized, self.moves)
        replacedText = self.replace(removedText, self.textReplacement)
        movedText = self.move(replacedText, self.moves)
        self.pathUtil.writeFile(movedText, self.saveFile)

    def move(self, text: str, moves: List[Dict[str, str]]):
        for move in moves:
            start = move['start']
            end = move['end']
            after = move['after']
            textToMove = text.partition(start)[-1].partition(end)[0] + end
            textWithoutMove = text.replace(textToMove, "")
            first, seperator, last = textWithoutMove.partition(after)
            finalText = first + seperator + textToMove + last
            text = finalText
        return text

    def remove(self, text: str, removes: List[Dict[str, str]]):
        # Iterate through the list of dictionaries containing removal information
        for remove in removes:
            textToRemove = ""
            textToRemove_old = None
            start = remove['start']
            end = remove['end']
            
            # Continue the loop until the removed text stops changing
            while textToRemove != textToRemove_old:
                # Construct the text to be removed by extracting content between 'start' and 'end'
                textToRemove = start + text.partition(start)[-1].partition(end)[0] + end
                
                # Remove the constructed text from the original text
                text = text.replace(textToRemove, "")
                
                # Update the old value for comparison in the next iteration
                textToRemove_old = textToRemove
                
                # Print the removed text for debugging purposes (you may remove this line in production)
                print(textToRemove)
        
        # Return the modified text after all removals
        return text


    def cutText(self, text: str, startSentence: str, endSentence: str):
        # Check if startSentence is empty
        if startSentence == "":
            withoutFirst = text
        else:
            # If startSentence is not empty, extract text after the startSentence
            withoutFirst = startSentence + text.split(startSentence, 1)[1]

        # Check if endSentence is empty
        if endSentence == "":
            withoutEnd = withoutFirst
        else:
            # If endSentence is not empty, extract text before the endSentence
            withoutEnd = withoutFirst.split(endSentence, 1)[0] + endSentence

        # Remove leading and trailing whitespaces
        stripped = withoutEnd.strip()

        # Replace carriage return ('\r') characters with an empty string
        prepared = stripped.replace('\r', '')

        # Return the prepared text
        return prepared

    def replace(self, text: str, textReplacement: Dict[str,str]):
        beforeReplacement = {
            '\xa0': ' '
        }
        baseReplacement =  {
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
        for input, target in beforeReplacement.items():
            text = text.replace(input,target)
        for input, target in textReplacement.items():
            text = text.replace(input,target)
        for input, target in baseReplacement.items():
            text = text.replace(input,target)

        self.pathUtil.writeFile(text, self.saveFile)

        ############################################################################
        ###  Check for remaining numbers, abbreviations, and unwanted characters ###
        ############################################################################

        remainingNumbers = [s for s in text.split() if bool(re.search(r'\d', s))]
        if len(remainingNumbers)>0:
            print('there are remaining number inside the text')
            print(remainingNumbers)
            replacements = {}
            for text in remainingNumbers:
                replacements[text] = self.textNormalizer.normalize(text)
            replacements = dict(sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True))
            print(json.dumps(replacements, indent=4, ensure_ascii=False))

            raise Exception('there are remaining number inside the text')

        remainingAbbreviations = [ab for ab in abbreviations.keys() if ab in text]
        if len(remainingAbbreviations)>0:
            print('there are remaining abbreviations inside the text')
            print(remainingAbbreviations)
            replacements = {key: value for (key,value) in abbreviations.items() if key in remainingAbbreviations}
            replacements = dict(sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True))
            print(json.dumps(replacements, indent=4, ensure_ascii=False))
            raise Exception('there are remaining abbreviations inside the text')

        aToZ = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        possibleAbbreviations = [' '+char+'.' for char in 'abcdefghijklmnopqrstuvwxyz' if ' '+char+'.' in text] + [' '+char+char2+'.' for char in aToZ for char2 in aToZ if ' '+char+char2+'.' in text]
        # shortWords = [' Co.', ' go.', ' Da.',' na.',' ab.', ' an.', ' da.', ' du.', ' er.', ' es.', ' ja.', ' so.', ' um.', ' zu.', ' Ja.', ' Ad.', ' je.', ' Es.', ' ob.', ' is.', ' tu.', ' Hm.', ' So.', ' wo.', ' ha.', ' he.', ' Du.', ' du.', ' Nu.', ' in.']
        # add French short words
        shortWords = [' y.', ' ci.', ' en.', ' et.', ' il.', ' la.', ' nu.', ' on.', ' pu.', ' un.', ' va.', ' vu.', ' Co.', ' go.', ' Da.',' na.',' ab.', ' an.', ' da.', ' du.', ' er.', ' es.', ' ja.', ' so.', ' um.', ' zu.', ' Ja.', ' Ad.', ' je.', ' Es.', ' ob.', ' is.', ' tu.', ' Hm.', ' So.', ' wo.', ' ha.', ' he.', ' Du.', ' du.', ' Nu.', ' in.']
        possibleAbbreviations = [ab for ab in possibleAbbreviations if ab not in shortWords]
        if len(possibleAbbreviations)>0:
            print('there are remaining possible abberviations inside the text')
            print(possibleAbbreviations)
            raise Exception('there are remaining possible abberviations inside the text')
        
        #allowedChars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ äöüßÖÄÜ .,;?!:" \n'
        # allow French chars
                    # 'é': 'e',
            # 'ê': 'e',
            # 'è': 'e',
            # 'à': 'a',
            # 'á': 'a',
            # 'œ': 'oe',
            # 'Ç': 'C'

        allowedChars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ äöüßÖÄÜ .,;?!: ;-() éêèàâáíîìóôòúûùœÇçÁÂÀÉÊÈÔëïæ" \n\''
        remaininNotAllowedChars = [char for char in text if char not in allowedChars]
        if len(remaininNotAllowedChars)>0:
            print('there are remaining unallowed chars inside the text')
            print(remaininNotAllowedChars)
            raise Exception('there are remaining unallowed chars inside the text')
        return text
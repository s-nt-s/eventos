from core.goodreads import GR
from core.util import trim
import re
import logging


logger = logging.getLogger(__name__)


def _match_title_author(text: str, *rgx: str):
    for r in rgx:
        x = re.search(r, text)
        if x is None:
            continue
        m = x.groupdict()
        tt = trim(m['title'])
        at = trim(m.get('author'))
        if tt is None:
            continue
        if at in (
            'Retórica',
            'Madrid',
            'Carabanchel',
            'Vizcaya',
        ):
            continue
        if re.search(
            (at or ''),
            r'\beditorial\b',
            flags=re.I
        ):
            at=None
        return {
            'title': tt,
            'author': at
        }


def _is(s: str, rg: str):
    if s is None:
        return False
    m = re.search(rg, s, flags=re.I)
    return m is not None


class BookFinder:
  
    def find(self, title_author: str):
        if title_author is None:
            return None

        m = _match_title_author(
            title_author,
            r"^'?(?P<title>[^']+)'?\.? [pP]resentaci[oó]n y conversaci[oó]n con (?P<author>[A-Z][^']+)$",
            r"^(?P<author>[A-Z].+) presenta: '?(?P<title>[^']+)'?$",
            r"^'?(?P<title>[^']+)'?\s*,?\s*(?:escrito por|de) (?P<author>[A-Z][^']+)$",
            r".*[pP]resentaci[oó]n del libro '?(?P<title>[^']+)'?.*",
        )
        if m is None:
            return self.__find(title_author)

        tt = m['title']
        at = m.get('author')

        logger.debug(f"title={tt} author={at} <-- {title_author}")

        url = self.__find(tt, at)
        if url is not None:
            return url

        books = GR.search(tt, at)
        if books:
            return books[0].url

    def __find(self, title: str, autor: str | None = None):
        for k, r in {
            "https://gestiona3.madrid.org/biblio_publicas/cgi-bin/abnetopac?TITN=1517923": r"\bUn verano kurdo\b",
            "https://gestiona3.madrid.org/biblio_publicas/cgi-bin/abnetopac?TITN=2254206": r"\bCuando el mundo duerme\b",
            "https://gestiona3.madrid.org/biblio_publicas/cgi-bin/abnetopac?TITN=2205945": r"\bMuerte accidental de un anarquista\b",
            "https://gestiona3.madrid.org/biblio_publicas/cgi-bin/abnetopac?TITN=2087546": r"\bLa mala costumbre\b",
            "https://gestiona3.madrid.org/biblio_publicas/cgi-bin/abnetopac?TITN=267016": r"\bEl anti-?Edipo\b",
            "https://madrid.ebiblio.es/resources/699f20282753bd15e883053d": r"\b(Redes vacías|Tecnología catastrófica y fin de la democracia)\b",
            "https://www.sigloxxieditores.com/libro/el-capital-obra-completa_17971/": r"\bEl Capital\b",
        }.items():
            if _is(title, r):
                return k


BF = BookFinder()


if __name__ == "__main__":
    import sys
    from core.log import config_log
    config_log("log/book.log", log_level=logging.DEBUG)
    print(BF.find(" ".join(sys.argv[1:])))
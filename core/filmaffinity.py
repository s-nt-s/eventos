from core.web import buildSoup
from bs4 import Tag
import re
import cloudscraper
import logging
from urllib.parse import quote


logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")

FM_SCRAPER = cloudscraper.create_scraper()


class FilmAffinityError(ValueError):
    pass


def _get_soup(url: str):
    soup = buildSoup(url, FM_SCRAPER.get(url).text)
    title_none = "not title found"
    txt = get_text(soup.select_one("title")) or title_none
    if txt.lower() in (title_none, "too many request", ):
        raise FilmAffinityError(txt)
    return soup


def get_text(n: Tag | None) -> str | None:
    if not isinstance(n, Tag):
        return None
    txt = re_sp.sub(" ", n.get_text()).strip()
    if len(txt) == 0:
        return None
    return txt


class FilmAffinityApi:
    ACTIVE = True

    @staticmethod
    def search(year: int, *titles: str):
        if not FilmAffinityApi.ACTIVE or not isinstance(year, int) or len(titles)==0:
            return None
        try:
            ids: set[int] = set()
            for title in titles:
                url = "https://www.filmaffinity.com/es/search.php?stype=title&em=1&stext="+quote(title)
                soup = _get_soup(url)
                link = soup.select_one('link[rel="alternate"][hreflang="es"][href]')
                _id_ = FilmAffinityApi.__extract_id_from_link(link)
                if _id_:
                    yr = FilmAffinityApi.__get_year(soup)
                    if yr == year:
                        ids.add(_id_)
                for div in soup.select("div.searchres div.card-body"):
                    span = get_text(div.select_one("span.mc-year"))
                    if span is None or int(span) != year:
                        continue
                    link = div.select_one("a[href]")
                    _id_ = FilmAffinityApi.__extract_id_from_link(link)
                    if _id_:
                        ids.add(_id_)
            if len(ids) == 1:
                return ids.pop()
        except FilmAffinityError as e:
            logger.critical(f"Error fetching film {year} {titles}: {e}")
            FilmAffinityApi.ACTIVE = False
            return None

    @staticmethod
    def __get_year(soup: Tag) -> str:
        y = get_text(soup.select_one("dd[itemprop='datePublished'], span[itemprop='datePublished']"))
        if y and y.isdecimal():
            return int(y)

    @staticmethod
    def __extract_id_from_link(a: Tag):
        if a is None:
            return None
        href = a.attrs.get("href")
        if not isinstance(href, str):
            return None
        m = re.search(r"/film(\d+)\.html$", href)
        if m is None:
            return None
        return int(m.group(1))

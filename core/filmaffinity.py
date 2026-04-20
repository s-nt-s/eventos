from core.web import buildSoup
from bs4 import Tag
import re
import cloudscraper
import logging
from urllib.parse import quote
from functools import cache


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


def _gap_year(y: int):
    return (None, y-1, y, y+1)


class FilmAffinityApi:
    ACTIVE = True

    @staticmethod
    @cache
    def search(year: int, *titles: str):
        if not isinstance(year, int):
            year = None
        if len(titles) == 0:
            return None
        if not FilmAffinityApi.ACTIVE:
            return None
        for k, y, t in (
            (132739, 2025, "Sorda"),
            (411856, 1963, "El verdugo"),
            (513636, 1962, "Matar a un ruiseñor"),
            (126406, 2024, "Una cabeza en la pared"),
            (999902, 1996, "El perro del hortelano"),
            (963150, 2025, "La furia"),
            (957271, 2025, "Decorado"),
            (309861, 2025, "La cena"),
            (206795, 2005, "Last Days"),
            (252377, 1977, "Informe general"),
            (684913, 2015, "Informe general II. El nuevo rapto de Europa"),
            (985323, 2010, "Seguir siendo"),
            (778097, 1975, "Welfare"),
            (207758, 2019, "Ema"),
            (235758, 2025, "Karla"),
            (397329, 2025, "El canto de las manos"),
            (295517, 2025, "La lucha"),
            (435198, 2026, "Tres mujeres"),
            (154374, 2025, "La buena hija"),
            (502795, 2024, "La semilla de la higuera sagrada"),
            (968717, 2025, "Mit hasan in gaza"),
            (435869, 1953, "Bienvenido Mr. Marshall"),
            (669924, 1927, "El gato y el canario"),
            (591219, 2026, "El canto de las mariposas"),
            (963958, 2024, "Jugar con fuego"),
            (750980, 2024 ,"On falling"),
            (821116, 2025, "Madrid, ext"),
            (795317, 2017, "La familia"),
            (304206, 2025, "Océano con David Attenborough"),
            (392601, 2026, "Crías"),
        ):
            if t in titles and year in _gap_year(y):
                return k
        if len(titles) > 1 and year is None:
            return None
        try:
            ids: set[int] = set()
            for title in titles:
                url = "https://www.filmaffinity.com/es/search.php?stype=title&em=1&stext="+quote(title)
                soup = _get_soup(url)
                link = soup.select_one('link[rel="alternate"][hreflang="es"][href]')
                _id_ = FilmAffinityApi.__extract_id_from_link(link)
                if _id_:
                    if year is None and len(titles) == 1:
                        logger.debug(f"FilmAffinityApi.search = {_id_} = {titles[0]}")
                        return _id_
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
            logger.debug(f"FilmAffinityApi.search = {tuple(sorted(ids))} = {year} + {titles}")
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

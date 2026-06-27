from core.web import Web, get_text
from core.cache import TupleCache
import logging
from core.event import Event, Cinema, Session, Category
from core.place import Places
import re
from bs4 import Tag
from core.util import to_uuid
from datetime import date
from core.md import MD
from typing import NamedTuple, Optional
from collections import defaultdict
from portal.base import Base

logger = logging.getLogger(__name__)

months = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')
TODAY = date.today()
RE_SESSION = re.compile(r"(\d+)(?:\s+de)?\s+(" + "|".join(months) + r")\S+\s+a\s+las?\s+([\d:]+)", flags=re.I)


class Movie(NamedTuple):
    title: str
    original: Optional[str] = None
    year: Optional[int] = None
    director: tuple[str, ...] = tuple()

    def get_title_year(self):
        if self.year is None:
            return self.title
        return f"{self.title} ({self.year})"


def to_title_year(t: str, y: int):
    if t is None:
        return None
    if y is None:
        return t
    return f"{t} ({y})"


class Dore(Base):
    URL = "https://entradasfilmoteca.sacatuentrada.es/es/busqueda?precio_desde=0&precio_hasta=5&pagina="
    PRICE = 3

    def __init__(self, cache: str|bool = True):
        super().__init__(cache)
        self.__w = Web()

    def __iter_divs(self):
        page = 0
        while True:
            page = page + 1
            self.__w.get(f"{Dore.URL}{page}")
            divs = self.__w.soup.select("#contenedor-productos div.productos > div")
            if len(divs) == 0:
                break
            for div in divs:
                more_info = div.find("a", string=re.compile(r"^\s*\+\s+INFO\s*$"))
                if more_info is None:
                    continue
                url_info = more_info.attrs["href"]
                if not div.find("span", string=re.compile(r"^\s*Agotado\s*$", flags=re.I)):
                    yield url_info, div

    def _get_events(self):
        events: set[Event] = set()
        for url_info, div in self.__iter_divs():
            events.add(self.__div_to_event(url_info, div))
        events = self.__clean_events(events)
        return tuple(events)

    def __clean_events(self, all_events: set[Event]):
        data: dict[str, set[Event]] = defaultdict(set)
        for e in all_events:
            e = e.fix()
            data[e.name].add(e)
        vnts: set[Event] = set()
        for arr in map(sorted, data.values()):
            if len(arr) == 1:
                vnts.add(arr[0])
                continue
            sessions: set[Session] = set()
            for e in arr:
                for s in e.sessions:
                    sessions.add(s.merge(url=(s.url or e.url)))
            vnts.add(e.merge(sessions=tuple(sorted(sessions))))
        events: set[Event] = set()
        for e in vnts:
            surl = set(s.url for s in e.sessions if s.url)
            if len(surl) > 0:
                if len(surl) > 1:
                    e = e.merge(url='')
                else:
                    e = e.merge(
                        url=surl.pop(),
                        sessions=tuple(sorted(s.merge(url=None) for s in e.sessions))
                    )
            events.add(e)
        return tuple(sorted(events))

    def __movies_from_div(self, div: Tag):
        movies: list[Movie] = []
        director: list[str] = []
        for d in re.split(r", | y ", get_text(div.select_one("h3.subtitulo")) or ''):
            d = re.sub(r"[\s,\.]+$", "", d)
            if d not in ("", "VV.AA") and d not in director:
                director.append(d)

        h2 = get_text(div.select_one("h2"))
        h2 = h2.rstrip(" .,")

        def _iter():
            ok = False
            for r in (
                r"([^\(\)]+)\(([^\(\)]+\s*,\s*)?((?:19|20)\d{2})?\)",
                r"([^\(\)]+) \(([^\(\)]+)\) \(((?:19|20)\d{2})?\)"
            ):
                for title, original, year in re.findall(r, h2):
                    title = re.sub(r"^y?\s+", "", title.strip())
                    original = re.sub(r"^\s+|\s*,$", "", original.strip())
                    if len(title) == 0:
                        continue
                    ok = True
                    yield title, original, int(year) if len(year) else None
            if ok is False:
                years = set(map(int, re.findall(r"[,\(]\s*((?:20|19)\d{2})\s*\)", h2)))
                yield h2, None, years.pop() if years else None,

        for title, original, year in _iter():
            m = Movie(
                title=title,
                original=original if original else None,
                year=year,
                director=tuple(director)
            )
            if m not in movies:
                movies.append(m)

        return movies

    def __div_to_event(self, url: str, div: Tag):
        movies = self.__movies_from_div(div)
        aka: list[str] = []
        names: list[str] = []
        years: set[int] = set()
        directors: set[tuple[str, ...]] = set()
        for m in movies:
            title = to_title_year(m.title, m.year)
            if title not in names:
                names.append(title)
            for t in (m.title, m.original):
                t = to_title_year(t, m.year)
                if t and t not in aka:
                    aka.append(t)
            if m.year:
                years.add(m.year)
            if m.director:
                directors.add(m.director)

        ev = Cinema(
            id='fm'+to_uuid(url),
            url=url,
            name=" + ".join(names),
            aka=tuple(aka),
            year=years.pop() if len(years) == 1 else None,
            director=directors.pop() if len(directors) == 1 else None,
            category=Category.CINEMA,
            img=div.select_one("img").attrs["data-src"],
            place=Places.DORE.value,
            sessions=self.__find_sessions(div),
            price=Dore.PRICE,
            duration=None #self.__find_duration(txt),
        )
        return ev

    def __find_duration(self, txt: str):
        if txt is None:
            return None
        m = re.search(r"Total sesión: (\d+)['’]", txt, re.I)
        if m:
            return int(m.group(1))
        duration = tuple(map(int, re.findall(r"(\d+)['’]", txt)))
        if len(duration) == 0:
            return None
        return sum(duration)

    def __find_sessions(self, div: Tag):
        txt = MD.convert(div.select_one("div.descripcion"))
        if txt is None:
            return tuple()
        sessions: set[Session] = set()
        for d, m, hm in RE_SESSION.findall(txt):
            d = int(d)
            m = months.index(m.lower())+1
            hm = tuple(map(int, re.findall(r"\d+", hm)))
            h = hm[0]
            mm = hm[1] if len(hm) == 2 else 0
            y = TODAY.year
            if m < TODAY.month:
                y = y + 1
            sessions.add(Session(
                date=f"{y}-{m:02d}-{d:02d} {h:02d}:{mm:02d}"
            ))
        return tuple(sorted(sessions))


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/dore.log", log_level=(logging.DEBUG))
    Dore().get_events()

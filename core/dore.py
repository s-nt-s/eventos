from .web import Web, refind, get_text
from .cache import TupleCache
from typing import Set, Dict, Union
from functools import cached_property, cache
import logging
from .event import Event, Cinema, Place, Session, Category, FieldNotFound, FieldUnknown, CategoryUnknown
import re
from bs4 import Tag
from core.util import to_uuid
from datetime import date

logger = logging.getLogger(__name__)

months = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')
TODAY = date.today()
RE_SESSION = re.compile(r"(\d+)(?:\s+de)?\s+(" + "|".join(months) + r")\S+\s+a\s+las?\s+([\d:]+)", flags=re.I)


class Dore(Web):
    URL = "https://entradasfilmoteca.sacatuentrada.es/es/busqueda?precio_desde=0&precio_hasta=5&pagina="
    PRICE = 3
    PLACE = Place(
        name="Cine Doré",
        address="C. de Santa Isabel, 3, Centro, 28012 Madrid",
        latlon="40.411950735826316,-3.699066276358703"
    )

    def __iter_divs(self):
        page = 0
        while True:
            page = page + 1
            self.get(f"{Dore.URL}{page}")
            divs = self.soup.select("#contenedor-productos div.productos > div")
            if len(divs) == 0:
                break
            for div in divs:
                more_info = div.find("a", string=re.compile(r"^\s*\+\s+INFO\s*$"))
                if more_info is None:
                    continue
                url_info = more_info.attrs["href"]
                if not div.find("span", string=re.compile(r"^\s*Agotado\s*$", flags=re.I)):
                    yield url_info, div

    @property
    @TupleCache("rec/dore.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url_info, div in self.__iter_divs():
            events.add(self.__div_to_event(url_info, div))
        events = self.__clean_events(events)
        return tuple(events)

    def __clean_events(self, all_events: Set[Event]):
        data: Dict[str, Set[Event]] = {}
        for e in all_events:
            if e.title not in data:
                data[e.title] = set()
            data[e.title].add(e)
        vnts: Set[Event] = set()
        for arr in map(sorted, data.values()):
            if len(arr) == 1:
                vnts.add(arr[0])
                continue
            sessions: Set[Session] = set()
            for e in arr:
                for s in e.sessions:
                    sessions.add(s.merge(url=(s.url or e.url)))
            vnts.add(e.merge(sessions=tuple(sorted(sessions))))
        events: Set[Event] = set()
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

    def __div_to_event(self, url: str, div: Tag):
        name = get_text(div.select_one("h2"))
        name = name.rstrip(" .,")
        m = re.match(r"^([^\(\)]+)\s+(\([^\(\)]*(\d{4})\))$", name)
        year = None
        aka: list[str] = []
        if m:
            name, ori, year = m.groups()
            aka.append(name)
            ori = re.sub(r"^\(|[, ]*"+year+"\)$", "", ori).strip()
            if ori:
                aka.append(ori)
            year = int(year)
        else:
            aka.append(name)

        director = get_text(div.select_one("h3.subtitulo"))
        ev = Cinema(
            id='fm'+to_uuid(url),
            url=url,
            name=name,
            category=Category.CINEMA,
            img=div.select_one("img").attrs["data-src"],
            place=Dore.PLACE,
            sessions=self.__find_sessions(div),
            price=Dore.PRICE,
            aka=tuple(aka),
            year=year,
            director=(director, ) if director else tuple(),
            duration=None #self.__find_duration(txt),
        )
        return ev

    def __find_duration(self, txt: str):
        if txt is None:
            return None
        m = re.search(r"Total sesión: (\d+)['’]", txt, re.IGNORECASE)
        if m:
            return int(m.group(1))
        duration = tuple(map(int, re.findall(r"(\d+)['’]", txt)))
        if len(duration) == 0:
            return None
        return sum(duration)

    def __find_sessions(self, div: Tag):
        txt = get_text(div.select_one("div.descripcion"))
        if txt is None:
            return tuple()
        sessions: Set[Session] = set()
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
    from .log import config_log
    config_log("log/dore.log", log_level=(logging.DEBUG))
    print(Dore().events)

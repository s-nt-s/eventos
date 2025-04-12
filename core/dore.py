from .web import Web, refind, get_text
from .cache import TupleCache
from typing import Set, Dict, Union
from functools import cached_property, cache
import logging
from .event import Event, Place, Session, Category, FieldNotFound, FieldUnknown, CategoryUnknown
import re
import time
from bs4 import Tag

logger = logging.getLogger(__name__)

months = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')


def get_img(n: Union[None, Tag]):
    if n is None:
        return None
    src = n.attrs.get("src")
    if src in (None, 'https://entradasfilmoteca.gob.es//Contenido/ImagenesEspectaculos/00_4659/Lou-n'):
        return None
    return src


class Dore(Web):
    URL = "https://entradasfilmoteca.gob.es/"
    PRICE = 3

    def get(self, url, auth=None, parser="lxml", **kvargs):
        kys = ('__EVENTTARGET', '__EVENTARGUMENT')
        if len(kvargs) and len(set(kys).difference(kvargs.keys())) == 0:
            msg = str(url) + '?' + "&".join(map(lambda k: f"{k}={kvargs[k]}", kys))
            logger.debug(msg)
        else:
            logger.debug(url)
        return super().get(url, auth, parser, **kvargs)

    @cached_property
    def calendar(self):
        ids: Set[int] = set()
        self.get(Dore.URL)
        while True:
            cal = self.soup.select_one("#CalendarioBusqueda")
            days = refind(cal, "td a", r"\d+")
            if len(days) == 0:
                return tuple(sorted(ids))
            y = refind(cal, "td", r".* de \d+$")[0]
            y = get_text(y).split()[-1]
            for a in days:
                d, _, m = a.attrs["title"].strip().split()
                m = months.index(m.lower()[:3]) + 1
                ids.add((int(y), int(m), int(d)))
            nxt = refind(cal, "a", r">")
            if len(nxt) < 1:
                return tuple(sorted(ids))
            action, data = self.prepare_submit("#ctl01")
            data['__EVENTTARGET'] = "ctl00$CalendarioBusqueda"
            data['__EVENTARGUMENT'] = nxt[0].attrs['href'].split("'")[-2]
            data['ctl00$TBusqueda'] = ""
            self.get(action, **data)

    @cache
    def get_links(self):
        urls: Set[str] = set()
        self.get(Dore.URL)
        for y, m, d in self.calendar:
            self.get(f"https://entradasfilmoteca.gob.es/Busqueda.aspx?fecha={d:02d}/{m:02d}/{y}%200:00:00")
            for a in self.soup.select("div.thumPelicula a.linkPelicula"):
                urls.add(a.attrs["href"])
        return tuple(sorted(urls))

    @property
    @TupleCache("rec/dore.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url in self.get_links():
            events.add(self.__url_to_event(url))
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

    def __url_to_event(self, url):
        self.get(url)
        ficha = self.select_one("#textoFicha")
        leyenda = self.soup.select_one("#textoFicha #leyenda")
        if leyenda:
            leyenda.extract()
        txt = get_text(ficha)
        duration = self.__find_duration(txt)
        if duration is None:
            logger.warning(str(FieldNotFound("duration (#textoFicha)", self.url)))
            duration = 120
        img = self.soup.select_one("div.item.active img")
        return Event(
            id='fm'+url.split("=")[-1],
            url=url,
            name=get_text(self.soup.select_one("div.row h1")),
            category=self.__find_category(),
            img=get_img(img),
            place=self.__find_place(),
            sessions=self.__find_sessions(),
            duration=duration,
            price=Dore.PRICE
        )

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

    def __find_sessions(self):
        sessions: Set[Session] = set()
        for tr in self.soup.select("#ContentPlaceHolderMain_grvEventos tr"):
            tds = tuple(map(get_text, tr.findAll("td")))
            d, m, y = map(int, tds[1].split("/"))
            h, mm = map(int, tds[2].split(":"))
            sessions.add(Session(
                date=f"{y}-{m:02d}-{d:02d} {h:02d}:{mm:02d}"
            ))
        return tuple(sorted(sessions))

    def __find_category(self):
        n = self.select_one("#ContentPlaceHolderMain_h2Categoria")
        for br in n.findAll("br"):
            br.replace_with("\n\n")
        txt = get_text(n).split()[0].lower()
        if txt == "cine":
            return Category.CINEMA
        logger.critical(str(CategoryUnknown(self.url, txt)))
        return Category.UNKNOWN

    def __find_place(self):
        place = get_text(self.select_one("#lateralFicha h4")).lower()
        if place == "cine doré":
            return Place(
                name="Cine Doré",
                address="C. de Santa Isabel, 3, Centro, 28012 Madrid"
            )
        raise FieldUnknown(self.url, "place", place)


if __name__ == "__main__":
    from .log import config_log
    config_log("log/dore.log", log_level=(logging.DEBUG))
    print(Dore().events)

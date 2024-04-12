from .web import Web, refind, get_text
from .cache import TupleCache
from typing import Set, Dict, List
from functools import cached_property, cache
import logging
from .event import Event, Place, Session, Category
import re
from bs4 import Tag
from datetime import datetime, timedelta
from .util import plain_text
import json


logger = logging.getLogger(__name__)
NOW = datetime.now()

class CasaAmericaException(Exception):
    pass


class CasaAmerica(Web):
    URL = "https://www.casamerica.es/agenda"

    def get(self, url, auth=None, parser="lxml", **kvargs):
        if url == self.url:
            return self.soup
        logger.debug(url)
        return super().get(url, auth, parser, **kvargs)

    @cached_property
    def calendar(self):
        urls: List[str] = []
        url = CasaAmerica.URL + "/" + NOW.strftime("%Y%m")
        while url:
            self.get(url)
            if self.soup.select_one("div.view-grouping h2.dia") is None:
                return tuple(urls)
            if url not in urls:
                urls.append(url)
            url = None
            cal = self.soup.select_one('nav.paginador-agenda a.page-link[rel="next"]')
            if cal is not None:
                url = cal.attrs["href"]
        return tuple(urls)

    @property
    @TupleCache("rec/casaamerica.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url in self.calendar:
            events = events.union(self.__url_to_events(url))
        return self.__clean_events(events)

    def __clean_events(self, all_events: Set[Event]):
        if None in all_events:
            all_events.remove(None)
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

    def __url_to_events(self, url: str):
        self.get(url)
        date = None
        ym = url.rstrip("/").split("/")[-1]
        y = int(ym[:4])
        m = int(ym[4:])
        for n in self.soup.select("div.view-content h2.dia, div.view-content li.row"):
            txt = get_text(n)
            if txt is None:
                continue
            if n.name == "h2":
                d = int(txt.split()[0])
                date = f"{y}-{m:02d}-{d:02d}"
                continue
            yield self.__url_to_event(date, n)

    def __url_to_event(self, date: str, info: Tag):
        h = info.find("p", string=re.compile(r"^\s*Horario\s*:\s+\d\d:\d\d\s*$"))
        if h is None:
            raise CasaAmericaException("NOT FOUND p[text=Horario: HH:MM]")
        a = info.select_one("h3.titulo a")
        if a is None:
            raise CasaAmericaException("NOT FOUND h3.titulo")
        hm = get_text(h).split()[-1]
        url = a.attrs["href"]
        js = self.__find_json(url)
        if self.__is_block(url):
            return None
        return Event(
            id="am"+js['path']['currentPath'].split("/")[-1],
            url=url,
            name=get_text(a),
            img=self.__find_img(url),
            price=self.__find_price(url),
            category=self.__find_category(info),
            duration=self.__find_duration(url),
            sessions=tuple((Session(
                url=url,
                date=date+" "+hm
            ),)),
            place=Place(
                name="La casa America",
                address="Plaza Cibeles, s/n, Salamanca, 28014 Madrid"
            )
        )

    @cache
    def __is_block(self, url: str):
        self.get(url)
        txt = plain_text(self.soup.find("title"))
        if txt is None:
            raise CasaAmericaException("NOT FOUND title")
        if txt.lower().startswith("acceso denegado"):
            logger.warn("ACCESS DENIED "+url)
            return True
        return False


    @cache
    def __find_img(self, url: str):
        self.get(url)
        return self.select_one("figure.imagen img").attrs["src"]

    @cache
    def __find_json(self, url: str):
        self.get(url)
        js = get_text(self.select_one('script[type="application/json"]'))
        return json.loads(js)

    @cache
    def __find_price(self, url: str):
        self.get(url)
        prices = set()
        for p in map(get_text, self.soup.select("article p")):
            if p is None or "General:" not in p:
                continue
            prices = prices.union(map(float, re.findall(r"([\d,.]+)€", p)))
        if len(prices) == 0:
            return 0
        return max(prices)

    @cache
    def __find_duration(self, url: str):
        self.get(url)
        durations = set()
        for p in map(get_text, self.soup.select("article p")):
            if p is not None:
                durations = durations.union(map(int, re.findall(r"(\d+)['’]", p)))
        if len(durations) == 0:
            logger.warn("NO DURATION in "+url)
            return 0
        return sum(durations)

    def __find_category(self, info: Tag):
        c = info.select_one("p.categoria")
        if c is None:
            raise CasaAmericaException("NOT FOUND p.categoria in " + self.url)
        txt = plain_text(c).lower()
        if txt == "cine":
            return Category.CINEMA
        if txt == "exposiciones":
            return Category.EXPO
        if txt == "teatro":
            return Category.THEATER
        if txt == "musica":
            return Category.CONCERT
        if txt in ("social", "literatura", "politica", "sociedad", "ciencia tecnologia", "economia", "arte", "historia"):
            return Category.OTHERS
        raise CasaAmericaException("Unknown category: " + txt)


if __name__ == "__main__":
    from .log import config_log
    config_log("log/casaamerica.log", log_level=(logging.DEBUG))
    print(CasaAmerica().events)

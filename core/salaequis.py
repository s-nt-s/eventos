from .web import Web, get_text
from functools import cache
from bs4 import Tag
from typing import Set, Dict, List
from .cache import TupleCache
import logging
from .event import Event, Session, Place, Category, FieldNotFound
import logging
import re
from .util import plain_text

logger = logging.getLogger(__name__)


class SalaEquis(Web):
    TAQUILLA = "https://salaequis.es/taquilla/"
    ENCUENTROS = "https://salaequis.es/encuentros/"

    def get(self, url, auth=None, parser="lxml", **kwargs):
        logger.debug(url)
        return super().get(url, auth, parser, **kwargs)

    @cache
    def get_encuentros(self):
        data: Dict[str, List[Tag]] = {}
        soup = self.get_soup(SalaEquis.ENCUENTROS)
        for i in soup.select("div.info"):
            txt = plain_text(i.select_one("div.title h2"))
            if txt is None:
                continue
            if txt not in data:
                data[txt] = []
            data[txt].append(i)
        return data

    @cache
    def get_links(self):
        links: Set[str] = set()
        self.get(SalaEquis.TAQUILLA)
        for a in self.soup.select("div.buy a"):
            links.add(a.attrs["href"])
        return tuple(sorted(links))

    @property
    @TupleCache("rec/salaequis.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url in self.get_links():
            events.add(self.__url_to_event(url))
        if None in events:
            events.remove(None)
        return tuple(sorted(events))

    def __url_to_event(self, url):
        self.get(url)
        if self.soup.find("a", string="Comprar"):
            return None
        div = self.soup.find("div", attrs={"id": re.compile("^product-\d+$")})
        if div is None:
            raise FieldNotFound("product-\\d+", self.url)
        id = "se"+div.attrs["id"].split("-")[-1]
        name = get_text(self.select_one("h1.product_title")).title()
        sessions = self.__find_session(name)
        if len(sessions) == 0:
            logger.debug(f"{id}[name={name}] no have sessions")
            return None
        event = Event(
            id=id,
            url=url,
            name=name,
            img=self.select_one("#productImage img").attrs["src"],
            price=0,
            category=Category.CINEMA,
            place=Place(
                name="Sala Equis",
                address="C. del Duque de Alba, 4, Centro, 28012 Madrid, España",
                latlon="40.412126715926796,-3.7059047815506396"
            ),
            duration=self.__find_duration(),
            sessions=sessions
        )
        return event
    
    def __find_duration(self):
        duration = set()
        for txt in map(get_text, self.soup.select("div.shortDescription p")):
            duration = duration.union(map(int, re.findall(r"(\d+)\s*min\b", txt)))
        if len(duration) == 0:
            raise FieldNotFound("div.shortDescription p[\\d+ min]", self.url)
        return sum(duration)
    
    def __find_session(self, name: str):
        tags = self.__find_encuentro(name)
        if tags is None or len(tags) == 0:
            return tuple()
        sessions: Set[Session] = set()
        for tag in tags:
            dmy = get_text(tag.select_one("div.day"))
            hm = get_text(tag.select_one("div.hour"))
            if None in (dmy, hm):
                continue
            d, m, y = map(int, dmy.split("/"))
            h, mm = map(int, hm.split(":"))
            sessions.add(Session(
                date=f"{y}-{m:02d}-{d:02d} {h:02d}:{mm:02d}"
            ))
        return tuple(sorted(sessions))
    
    def __find_encuentro(self, name: str):
        name = plain_text(name)
        if name is None:
            return None
        encuentros = self.get_encuentros()
        if name in encuentros:
            return encuentros[name]
        for k, v in encuentros.items():
            if k.endswith(" "+name):
                return v

if __name__ == "__main__":
    from .log import config_log
    config_log("log/salaequis.log", log_level=(logging.DEBUG))
    print(SalaEquis().events)
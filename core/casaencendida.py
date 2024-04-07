from .web import Web, get_text
from typing import Set, Dict, List
from functools import cache
from .cache import TupleCache
import logging
from .event import Event, Session, Place, Category
import re
import json

logger = logging.getLogger(__name__)

months = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')


class CasaEncendidaException(Exception):
    pass


class CasaEncendida(Web):
    URL = "https://www.lacasaencendida.es/actividades?t[0]=activity_"
    ACTIVITY = (2, 3)
    PLACE = Place(
        name="La Casa Encendida",
        address="Rda. de Valencia, 2, Centro, 28012 Madrid"
    )

    def get(self, url, auth=None, parser="lxml", **kvargs):
        logger.debug(url)
        return super().get(url, auth, parser, **kvargs)

    @cache
    def get_links(self):
        urls: Set[str] = set()
        for a in CasaEncendida.ACTIVITY:
            urls = urls.union(self.__get_links(CasaEncendida.URL+str(a)))
        return tuple(sorted(urls))

    def __get_links(self, url_cat):
        urls: Set[str] = set()
        page = 0
        while True:
            page = page + 1
            self.get(url_cat+f"&page={page}")
            links = self.soup.select("div.results-list a.results-list__link")
            for a in links:
                urls.add(a.attrs["href"])
            if len(links) == 0:
                return tuple(sorted(urls))

    @property
    @TupleCache("rec/casaencendida.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url in self.get_links():
            events.add(self.__url_to_event(url))
        return tuple(sorted(events))

    def __get_json(self, url) -> List[Dict]:
        self.get(url)
        n = self.select_one('script[type="application/ld+json"]')
        return json.loads(get_text(n))

    def __url_to_event(self, url):
        info = self.__get_json(url)
        return Event(
            id="ce"+info['identifier'].split("-")[-1],
            url=url,
            name=info[0]['name'],
            category=self.__find_category(),
            img=info[0]['image'],
            place=CasaEncendida.PLACE,
            sessions=self.__find_sessions(info),
            price=self.__find_price(info)
        )

    def __find_sessions(self, info: List[Dict]):
        if len(info) == 1:
            return tuple((Session(
                url=self.url,
                date=info[0]["startDate"][:16].replace("T", " ")
            ), ))
        sessions: Set[Session] = set()
        for i in info[1:]:
            sessions.add(Session(
                url=i['location']['url'],
                date=i["startDate"][:16].replace("T", " ")
            ))
        return tuple(sorted(sessions))

    def __find_price(self, info: List[Dict]):
        prices = set({0, })
        for i in info:
            if not i.get("offers"):
                continue
            for o in i["offers"]:
                prices.add(float(o["price"]))
        return max(prices)

    def __find_category(self):
        tags = set()
        for tag in map(get_text, self.soup.select("div.tags")):
            for t in re.split(r",?\s+", tag):
                tags.add(t.lower())
        if "cine" in tags:
            return Category.CINEMA
        if "conciertos" in tags:
            return Category.CONCERT
        raise CasaEncendidaException("Unknown category: " + ", ".join(sorted(tags)))


if __name__ == "__main__":
    from .log import config_log
    config_log("log/casaencendida.log", log_level=(logging.DEBUG))
    print(CasaEncendida().events)

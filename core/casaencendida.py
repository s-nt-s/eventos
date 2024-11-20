from .web import Web, get_text
from typing import Set, Dict, List
from functools import cache
from .cache import TupleCache
import logging
from .event import Event, Session, Place, Category, FieldUnknown
import re
import json
from datetime import datetime
from .filemanager import FM

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
        js = json.loads(get_text(n))
        return js

    def __url_to_event(self, url):
        info = self.__get_json(url)
        self.__validate_info_event(info)
        idevent = info[0]['identifier'].split("-")[-1]
        FM.dump(f"rec/casaencendida/{idevent}.json", info)
        return Event(
            id="ce"+idevent,
            url=url,
            name=info[0]['name'],
            category=self.__find_category(info),
            img=info[0]['image'],
            place=CasaEncendida.PLACE,
            sessions=self.__find_sessions(info),
            price=self.__find_price(info),
            duration=self.__find_duration(info)
        )

    def __validate_info_event(self, info: List):
        if not isinstance(info, list):
            raise CasaEncendidaException("MUST TO BE A LIST: "+str(info))
        if len(info) == 0:
            raise CasaEncendidaException("MUST TO BE A LIST NOT EMPTY: "+str(info))
        for i in info:
            if not isinstance(i, dict):
                raise CasaEncendidaException("MUST TO BE A LIS OF DICTs: "+str(info))
        identifier = info[0].get('identifier')
        if not isinstance(identifier, str):
            raise CasaEncendidaException("MUST TO BE A LIS OF DICTs with a identifier: "+str(info))
        idevent = identifier.split("-")[-1]
        if not idevent.isdigit():
            raise CasaEncendidaException("MUST TO BE A LIS OF DICTs with a int identifier: "+str(info))
        return True

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

    def __find_category(self, info: List[Dict]):
        tags = set()
        for tag in map(get_text, self.soup.select("div.tags, div.item-detail__info__tags a")):
            for t in re.split(r"\s*[,/]\s+", tag):
                tags.add(t.replace("#", "").strip().lower())
        if "en familia" in tags:
            return Category.CHILDISH
        if tags.intersection(("cine", "audiovisuales")):
            return Category.CINEMA
        if tags.intersection(("conciertos", "música")):
            return Category.MUSIC
        name:str = info[0]['name'].lower()
        if "concierto" in name:
            return Category.MUSIC
        desc = get_text(self.soup.select_one("div.item-detail__info__content")) or ""
        if "canciones" in desc:
            return Category.MUSIC
        if "film" in name.split():
            return Category.CINEMA
        raise FieldUnknown(f"category in {self.url}", ", ".join(sorted(tags)))

    def __find_duration(self, info: List[Dict]):
        def to_date(s: str):
            if s is not None:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
        under24: Set[int] = set()
        over24: Set[int] = set()
        for i in info:
            startDate = to_date(i.get('startDate'))
            endDate = to_date(i.get('endDate'))
            if startDate and endDate:
                d = round((endDate - startDate).total_seconds() / 60)
                if d < 0:
                    continue
                if d < (24*60):
                    under24.add(d)
                else:
                    over24.add(d)
        if len(under24.union(over24)) == 0:
            raise FileNotFoundError("duration", info)
        if under24:
            return max(under24)
        return max(over24)


if __name__ == "__main__":
    from .log import config_log
    config_log("log/casaencendida.log", log_level=(logging.DEBUG))
    print(CasaEncendida().events)

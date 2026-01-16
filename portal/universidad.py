from core.ics import IcsReader, IcsEventWrapper
from functools import cached_property
from core.event import Event, Place, Session, Category
from core.util import re_or, re_and
import requests
import re
from bs4 import BeautifulSoup
from collections import defaultdict
from types import MappingProxyType
from functools import cache
from core.web import buildSoup, get_text
import feedparser
import logging
from typing import Callable
from requests import Session as ReqSession
from bs4 import XMLParsedAsHTMLWarning
from core.util import find_euros, get_obj
from core.cache import HashTupleCache
from datetime import datetime
import pytz
import urllib3
import warnings
import json
from typing import NamedTuple

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

urllib3.disable_warnings()
logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")

NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))


class Info(NamedTuple):
    ldj: dict
    sym: dict
    pog: dict

    def get_img(self):
        if self.pog:
            return self.pog.get('image')

    def get_price(self):
        if self.ldj:
            offers = self.ldj.get('offers')
            if isinstance(offers, list) and len(offers) > 0:
                prices: set[float] = set()
                for o in offers:
                    prices.add(float(o['price']))
                if len(prices):
                    return max(prices)

    def get_categories(self):
        val: set[str] = set()
        if isinstance(self.sym, dict):
            arr = []
            for k in ('categories', 'tags'):
                v = self.sym.get(k)
                if isinstance(v, list):
                    arr.extend(v)
            for c in arr:
                if isinstance(c, dict):
                    v = c.get("name")
                    if isinstance(v, str):
                        v = re_sp.sub(" ", v).strip()
                        if len(v):
                            val.add(v)
        if val:
            return tuple(val)

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        return Info(**obj)


def load_kml_soup(url: str, verify_ssl=True):
    r = requests.get(url, verify=verify_ssl)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "xml")
    return soup


def clean_place_name(name: str) -> str:
    if name is None:
        return None
    name = re_sp.sub(" ", name).strip()
    if len(name) == 0:
        return None
    if re_and(name, "URJC", "Quintana", flags=re.I):
        return "URJC Quintana"
    if re_and(name, "Carlos III", "Puerta (de )?Toledo", flags=re.I):
        return "UC3 Puerta Toledo"
    if re_and(name, "ateneo (de )?Madrid", flags=re.I):
        return "Ateneo Madrid"
    return name


class Universidad:
    def __init__(
        self,
        ics: str,
        verify_ssl=True,
        isOkPlace: Callable[[Place | tuple[float, float] | str], bool] = None
    ):
        self.__ics_url = ics
        self.__isOkPlace = isOkPlace or (lambda *_: True)
        self.__ics = IcsReader(ics, verify_ssl=verify_ssl)
        self.__kml_url = re.sub(
            r"/ics/location/(.+)/(.+)\.ics$",
            r"/kml/get/\2.kml",
            ics
        )
        self.__kml_soup = load_kml_soup(self.__kml_url, verify_ssl=verify_ssl)
        self.__rss_url = ics.replace("/ics/", "/rss/").replace(".ics", ".rss")
        self.__rss = feedparser.parse(self.__rss_url)
        self.__verify_ssl = verify_ssl
        self.__s = ReqSession()

    @cache
    def __get(self, url: str):
        r = self.__s.get(url, verify=self.__verify_ssl)
        r.raise_for_status()
        return buildSoup(url, r.content)

    @HashTupleCache("rec/universidad/{}.json", builder=Info.build)
    def __get_info(self, url: str):
        ldj, sym = None, None
        soup = self.__get(url)
        txt = get_text(soup.select_one("script[type='application/ld+json']"))
        if txt:
            ldj = json.loads(txt)
        for script in map(get_text, soup.select("script")):
            m = re.match(
                re.escape("var SYM = $.extend(SYM || {}, {data:") + r"(.+)}\);.*",
                script or ""
            )
            if m:
                sym = json.loads(m.group(1))
        pog = {}
        for k in ('image', ):
            og_node = soup.select_one(f"meta[property='og:{k}']")
            if og_node:
                v = og_node.get("content")
                if v:
                    v = re_sp.sub(r" ", v).strip()
                    if len(v):
                        pog[k] = v
        return Info(
            ldj=ldj,
            sym=sym,
            pog=pog
        )

    @cache
    def __get_description(self, url: str, name: str) -> str:
        for i in self.__rss.entries:
            if i.link in (url, url + ".html"):
                return buildSoup(url, i.description)
        for p in self.__kml_soup.select("Placemark:has(name):has(description)"):
            n = p.find("name").text.strip()
            if n != name:
                continue
            c = p.find("description").text.strip()
            if len(c) == 0:
                return buildSoup(url, c)

    @cache
    def __find_coordinates(self, name: str):
        if name is None or len(name.strip()) == 0:
            return None
        coord: set[tuple[float, float]] = set()
        for p in self.__kml_soup.select("Placemark:has(name):has(coordinates)"):
            n = p.find("name").text.strip()
            if n != name:
                continue
            c = p.find("coordinates").text.strip()
            if len(c) == 0:
                continue
            lon, lat = tuple(map(float, c.split(",")))[:2]
            lat = round(lat, 6)
            lon = round(lon, 6)
            coord.add((lat, lon))
        if len(coord) == 1:
            lat, lon = coord.pop()
            return f"{lat},{lon}"

    def __get_locations(self):
        loc: dict[str, set[str]] = defaultdict(set)
        for e in self.__ics.events:
            latlon = self.__find_coordinates(e.SUMMARY)
            if latlon and e.LOCATION:
                loc[e.LOCATION].add(latlon)
        rt: dict[str, str] = {}
        for k, v in loc.items():
            if len(v) == 1:
                rt[k] = v.pop()
        return MappingProxyType(rt)

    @cached_property
    def events(self):
        logger.info("Buscando eventos en universidades")
        loc_latlon = self.__get_locations()
        events: set[Event] = set()
        for e in self.__ics.events:
            if e.DTSTART <= NOW:
                continue
            latlon = self.__find_coordinates(e.SUMMARY)
            if latlon is None:
                latlon = loc_latlon.get(e.LOCATION)
            place = Place(
                name=clean_place_name(e.LOCATION),
                address=e.LOCATION,
                latlon=latlon
            ).normalize()
            if not self.__isOkPlace(place):
                continue
            link = self.__find_url(e)
            if link is None:
                logger.warning(f"Evento sin URL {e}")
                continue
            category = self.__find_category(link, e)
            img = self.__find_img(link, e)
            price = self.__find_price(link, e)
            event = Event(
                id=e.UID,
                url=link,
                name=e.SUMMARY,
                duration=e.duration or 60,
                img=img,
                price=price,
                publish=e.str_publish,
                category=category,
                place=place,
                sessions=(
                    Session(
                        date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                    ),
                ),
            )
            events.add(event)
        evs = tuple(sorted(events))
        logger.info(f"Buscando eventos en universidades = {len(evs)}")
        return evs

    def __find_category(self, link: str, e: IcsEventWrapper) -> Category:
        if re_or(e.SUMMARY, r"Actividad formativa de Doctorado", flags=re.I):
            return Category.NO_EVENT
        if re_or(e.SUMMARY, r"UN REGRESO DE CINE", flags=re.I):
            return Category.CINEMA
        if re_or(e.SUMMARY, "Presentaci[óo]n de la asociaci[óo]n", flags=re.I):
            return Category.CONFERENCE
        description = get_text(self.__get_description(link, e.SUMMARY))
        if re_or(description, r"Encuentro con", flags=re.I):
            return Category.CONFERENCE
        if re_or(description, "obra esc[eé]nica", flags=re.I):
            return Category.THEATER
        info = self.__get_info(link)
        if info:
            cat = info.get_categories() or tuple()
            for c in cat:
                if re_or(c, "teatro"):
                    return Category.THEATER
        return Category.UNKNOWN

    def __find_url(self, e: IcsEventWrapper):
        for url in (e.URL, e.DESCRIPTION):
            if isinstance(url, str) and url.startswith("http"):
                return url

    def __find_img(self, link: str, e: IcsEventWrapper):
        for img in (e.ATTACH,):
            if isinstance(img, str) and img.startswith("http"):
                return img
        info = self.__get_info(link)
        if info:
            return info.get_img()

    def __find_price(self, link: str, e: IcsEventWrapper) -> float | int:
        description = get_text(self.__get_description(link, e.SUMMARY))
        prc = find_euros(description)
        if prc is not None:
            return prc
        info = self.__get_info(link)
        if info:
            prc = info.get_price()
            if prc is not None:
                return prc
        return 0

    @classmethod
    def get_events(
        cls,
        *urls: str,
        verify_ssl=True,
        isOkPlace: Callable[[Place | tuple[float, float] | str], bool] = None
    ):
        events: set[Event] = set()
        for url in urls:
            events.update(cls(url, verify_ssl=verify_ssl, isOkPlace=isOkPlace).events)
        return tuple(sorted(events))


if __name__ == "__main__":
    # https://eventos.uc3m.es/kml.html
    # https://eventos.ucm.es/kml.html
    # https://eventos.uam.es/kml.html
    # https://eventos.urjc.es/kml.html
    evs = Universidad.get_events(
        "https://eventos.uc3m.es/ics/location/espana/lo-1.ics",
        "https://eventos.ucm.es/ics/location/espana/lo-1.ics",
        "https://eventos.uam.es/ics/location/espana/lo-1.ics",
        "https://eventos.urjc.es/ics/location/espana/lo-1.ics",
        verify_ssl=False
    )
    for event in evs:
        continue
        print(event)

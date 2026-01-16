from core.ics import IcsReader, IcsEventWrapper
from functools import cached_property
from core.event import Event, Place, Session, Category
from core.util import re_or
from fastkml import kml
import requests
import re
from bs4 import BeautifulSoup
from collections import defaultdict
from types import MappingProxyType
from functools import cache
from core.web import WEB, buildSoup
import feedparser
import logging

import urllib3

urllib3.disable_warnings()
logger = logging.getLogger(__name__)


def load_kml(url: str, verify_ssl=True) -> kml.KML:
    r = requests.get(url, verify=verify_ssl)
    r.raise_for_status()
    k = kml.KML()
    k.from_string(r.text.encode("utf-8"))
    return k


def load_kml_soup(url: str, verify_ssl=True):
    r = requests.get(url, verify=verify_ssl)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "xml")
    return soup


class Universidad:
    def __init__(self, ics: str, verify_ssl=True):
        self.__ics_url = ics
        self.__ics = IcsReader(ics, verify_ssl=verify_ssl)
        self.__kml_url = re.sub(
            r"/ics/location/(.+)/(.+)\.ics$",
            r"/kml/get/\2.kml",
            ics
        )
        #self.__kml = load_kml(self.__kml_url, verify_ssl=verify_ssl)
        self.__kml_soup = load_kml_soup(self.__kml_url, verify_ssl=verify_ssl)
        self.__rss_url = ics.replace("/ics/", "/rss/").replace(".ics", ".rss")
        self.__rss = feedparser.parse(self.__rss_url)

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
                return c

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
        loc_latlon = self.__get_locations()
        events: set[Event] = set()
        for e in self.__ics.events:
            latlon = self.__find_coordinates(e.SUMMARY)
            if latlon is None:
                latlon = loc_latlon.get(e.LOCATION)
            link = self.__find_url(e)
            if link is None:
                logger.warning(f"Evento sin URL {e}")
                continue
            category = self.__find_category(e)
            description = self.__get_description(link, e.SUMMARY)
            event = Event(
                id=e.UID,
                url=link,
                name=e.SUMMARY,
                duration=e.duration or 60,
                img=self.__find_img(e),
                price=0,
                publish=e.str_publish,
                category=category,
                place=Place(
                    name=e.LOCATION,
                    address=e.LOCATION,
                    latlon=latlon
                ),
                sessions=(
                    Session(
                        date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                    ),
                ),
            )
            events.add(event)
        evs = tuple(sorted(events))
        return evs

    def __find_category(self, e: IcsEventWrapper) -> Category:
        if re_or(e.SUMMARY, r"Actividad formativa de Doctorado", flags=re.I):
            return Category.NO_EVENT
        if re_or(e.SUMMARY, r" UN REGRESO DE CINE", flags=re.I):
            return Category.CINEMA
        return Category.UNKNOWN

    def __find_url(self, e: IcsEventWrapper):
        for url in (e.URL, e.DESCRIPTION):
            if isinstance(url, str) and url.startswith("http"):
                return url

    def __find_img(self, e: IcsEventWrapper):
        for img in (e.ATTACH,):
            if isinstance(img, str) and img.startswith("http"):
                return img
        return None

    @classmethod
    def get_events(cls, *urls: str, verify_ssl=True):
        events: set[Event] = set()
        for url in urls:
            events.update(cls(url, verify_ssl=verify_ssl).events)
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

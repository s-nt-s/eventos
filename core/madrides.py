from .web import Web
from bs4 import Tag, BeautifulSoup
import re
from typing import NamedTuple, Tuple, Set, Dict, List
import json
from functools import cached_property, cache
from urllib.parse import urlencode
from .event import Event, Session, Place, Category, FieldNotFound
from .util import plain_text
from ics import Calendar
from datetime import timedelta
import logging
from .cache import TupleCache
from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


def get_query(url: str):
    purl = urlparse(url)
    qr = parse_qs(purl.query)
    return {k: v[0] for k, v in qr.items()}


def safe_get_text(n: Tag):
    if isinstance(n, Tag):
        return get_text(n)


def get_text(n: Tag):
    t = n.get_text()
    t = re_sp.sub(" ", t)
    t = re.sub(r'[“”]', '"', t)
    return t.strip()


def clean_lugar(s: str):
    s = re.sub(r"^Biblioteca Pública( Municipal)?", "Biblioteca", s)
    s = re.sub(r"\s+\(.*?\)\s*$", "", s)
    return s


def get_href(n: Tag):
    if n is None:
        return None
    if n.name == "a":
        return n.attrs.get("href")
    return get_href(n.find("a"))

class MadridEs:
    AGENDA = "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/?vgnextfmt=default&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD"
    TAXONOMIA = "https://www.madrid.es/ContentPublisher/jsp/apl/includes/XMLAutocompletarTaxonomias.jsp?taxonomy=/contenido/actividades&idioma=es&onlyFirstLevel=true"

    def __init__(self):
        self.w = Web()

    def get(self, url, *args, **kwargs) -> BeautifulSoup:
        if self.w.url != url:
            logger.debug(url)
            return self.w.get(url, *args, **kwargs)
        return self.w.soup

    @property
    @TupleCache("rec/madrides.json", builder=Event.build)
    def events(self):
        evts: Set[Event] = set()
        for action, data in self.iter_submit():
            evts = evts.union(self.__get_events(action, data))
        arr: List[Event] = []
        for e in sorted(evts):
            img = self.get(e.url).select_one("div.image-content img")
            if img:
                e = e.merge(img=img.attrs["src"])
            arr.append(e)
        return tuple(arr)

    def __get_events(self, action, data=None):
        evts: Set[Event] = set()
        for div in self.__get_soup_events(action, data):
            cat = self.__find_category(div)
            if cat is None:
                continue
            lg = div.select_one("a.event-location")
            if lg is None:
                continue
            cal = self.__get_cal(div)
            if cal is None:
                continue
            durations: Set[int] = set()
            sessions: Set[Session] = set()
            for event in cal.events:
                start = event.begin
                end = event.end
                durations.add(int((end - start).seconds / 60))
                sessions.add(Session(
                    date=start.strftime("%Y-%m-%d %H:%M")
                ))
            if len(durations) == 0:
                continue
            a = div.select_one("a.event-link")
            lk = a.attrs["href"]
            qr = get_query(lk)
            ev = Event(
                id="ms"+qr["vgnextoid"],
                url=lk,
                name=get_text(a),
                img=None,
                price=0,
                category=cat,
                place=Place(
                    name=clean_lugar(lg.attrs["data-name"]),
                    address=lg.attrs["data-direction"],
                    latlon=lg.attrs["data-latitude"]+","+lg.attrs["data-longitude"]
                ),
                duration=max(durations),
                sessions=tuple(sorted(sessions))
            )
            evts.add(ev)
        nxt = get_href(self.w.soup.select_one("li.next a"))
        if nxt:
            evts = evts.union(self.__get_events(nxt))
        return evts

    def __get_cal(self, div: Tag):
        cal = div.select_one("p.event-date a")
        if cal is None:
            return None
        url = cal.attrs["href"]
        logger.debug(url)
        r = self.w._get(url)
        return Calendar(r.text)

    def __find_category(self, div: Tag):
        lg = div.select_one("a.event-location")
        if lg and re.search(r"\btiteres\b", plain_text(lg.attrs["data-name"])):
            return Category.PUPPETRY
        tp = safe_get_text(div.select_one("p.event-type")) or ""
        name = get_text(div.select_one("a.event-link"))
        tp_name = plain_text((tp+" "+name).strip())
        if re.search(r"\b(monologos?)\b", tp_name):
            return Category.OTHERS
        if re.search(r"\b(cine|proyeccion(es)?|cortometraje)\b", tp_name):
            return Category.CINEMA
        if re.search(r"\b(musica|conciertos?)\b", tp_name):
            return Category.MUSIC
        if re.search(r"\b(teatro)\b", tp_name):
            return Category.THEATER
        if re.search(r"\b(exposicion)\b", tp_name):
            return Category.EXPO

    def __get_soup_events(self, action, data=None):
        if data:
            action = action + '?' + urlencode(data)
        soup = self.get(action)
        arr = soup.select("#listSearchResults ul.events-results li div.event-info")
        logger.debug(f"{len(arr)} en {action}")
        return arr

    def iter_submit(self):
        self.get(MadridEs.AGENDA)
        action, data = self.w.prepare_submit("#generico1", enviar="buscar")
        for k in ("gratuita", "movilidad"):
            if k in data:
                del data[k]
        data['gratuita'] = "1"
        data["tipo"] = "-1"
        data["distrito"] = "-1"
        data["usuario"] = "-1"

        data = {k: v for k, v in data.items() if v is not None}
        aux = dict(data)

        def do_filter(**kwargs):
            return bool(len(self.__get_soup_events(action, {**aux, **kwargs})))

        def my_filter(k, arr, **kwargs):
            return tuple(filter(lambda v: do_filter(**{**kwargs, **{k: v}}), arr))

        for dis in my_filter("distrito", self.centro.keys()):
            data["distrito"] = dis
            yield action, data


    @cached_property
    def centro(self):
        data: Dict[str, str] = {}
        for k, v in self.distritos.items():
            if not re.search(r"arganzuela|centro|moncloa|chamberi|retiro|salamaca", plain_text(v)):
                continue
            data[k]=v
        return data

    @cached_property
    def gente(self):
        data: Dict[str, str] = {}
        for k, v in self.distritos.items():
            if re.search(r"vehiculos|contribuyente|drogodependientes", plain_text(v)):
                continue
            data[k]=v
        return data
    
    @cached_property
    def distritos(self):
        return self.__get_options("#distrito")

    @cached_property
    def usuarios(self):
        return self.__get_options("#usuario")

    def __get_options(self, slc):
        data: Dict[str, str] = {}
        soup = self.get(MadridEs.AGENDA)
        for o in soup.select(slc+" option"):
            k = o.attrs["value"]
            v = re_sp.sub(" ", o.get_text()).strip()
            if k != "-1":
                data[k] = v
        return data

    @cached_property
    def tipos(self):
        data: Dict[str, str] = {}
        soup = self.get(MadridEs.TAXONOMIA, parser="xml")
        for n in soup.findAll('item'):
            value = n.find('value').string.strip()
            text = re_sp.sub(" ", n.find('text').string).strip()
            data[value] = text
        return data


if __name__ == "__main__":
    from .log import config_log
    config_log("log/madrides.log", log_level=(logging.DEBUG))
    print(MadridEs().events)
    #m.get_events()
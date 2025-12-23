from requests.sessions import Session
import logging
from typing import NamedTuple, Optional
from collections import defaultdict
from core.dictwraper import DictWraper
from datetime import datetime, date
from requests.exceptions import JSONDecodeError
from functools import cache
from core.filemanager import FileManager
from core.cache import HashCache, TupleCache
from core.util import get_obj
import json
import re


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


def str_line(s: str | None):
    if s is None:
        return ''
    return re_sp.sub(" ", s).strip()


def str_tuple(s: str | None, spl: str) -> tuple[str, ...]:
    if s in (None, ""):
        return tuple()
    arr: set[str] = set()
    for x in re.split(spl, s):
        x = x.strip()
        if len(x) > 0:
            arr.add(x)
    return tuple(sorted(arr))


def date_to_str(d: date | datetime | None):
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.strftime("%Y-%d-%m %H:%M")
    if isinstance(d, date):
        return d.strftime("%Y-%d-%m")
    raise ValueError(d)


def find_euros(prc: str | None):
    if prc is None:
        return None
    if re.search(r"\b(gratuit[ao] (para|con)|(entrada|acceso) (gratuit[oa]|libre)|actividad(es)? gratuitas?)\b", prc, flags=re.I):
        return 0
    eur: set[float] = set()
    for s in re.findall(r"(\d[\d\.,]*)\s*(?:€|euros?)", prc, flags=re.I):
        p = float(s.replace(",", "."))
        if p == int(p):
            p = int(p)
        eur.add(p)
    if len(eur):
        return max(eur)


class MadridEsEvent(NamedTuple):
    id: int
    url: str
    title: str
    description: str
    dtstart: str
    dtend: str
    audience: tuple[str, ...]
    recurrence: bool
    latitude: float
    longitude: float
    location: str
    address: str
    price: Optional[float | int] = None

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
        return MadridEsEvent(**obj)


class MadridEsDictWraper(DictWraper):
    def get_price(self):
        if self.get_bool('free'):
            return 0
        for txt in (
            self.get_str_or_none('price'),
            self.get_str_or_none('description')
        ):
            prc = find_euros(txt)
            if prc is not None:
                return prc


class MadridEs:
    def __init__(self):
        self.__s = Session()

    def __get_json(self, url: str):
        r = self.__s.get(url)
        try:
            return r.json()
        except JSONDecodeError:
            text = re_sp.sub(r" ", r.text).strip()
            if len(text) == 0:
                logger.critical(f"{url} is empty : {text}")
            try:
                return json.loads(text)
            except Exception:
                pass
            logger.critical(f"{url} is not a JSON: {text}")
            raise

    def __get_dict(self, url: str):
        obj = self.__get_json(url)
        if not isinstance(obj, dict):
            raise ValueError(f"No dict: {url}")
        return obj

    @HashCache("rec/api_madrid_es/{}.json")
    def get_graph_list(self, url: str) -> tuple[dict, ...]:
        obj = self.__get_dict(url)
        lst = obj.get("@graph")
        if not isinstance(lst, list):
            raise ValueError(f"@graph is not list {url}")
        if len(lst) == 0:
            logger.critical(f"@graph is empty list {url}")
            return tuple()
        if not all(isinstance(x, dict) for x in lst):
            raise ValueError(f"@graph is not list[dict] {url}")
        return list(FileManager.parse_obj(i, compact=True) for i in lst)

    def get_graph_list_len_1(self, url: str):
        graph = self.get_graph_list(url)
        if len(graph) != 1:
            raise ValueError(f"@graph is not len(list[dict]) == 1 {url}")
        obj = FileManager.parse_obj(graph[0], compact=True) or {}
        return obj

    def __obj_to_event(self, obj: dict):
        i = MadridEsDictWraper(obj)
        loc = i.get_dict_or_none('location')
        if loc is None:
            logger.debug(f"Ignorado por location=None {obj}")
            return None
        area = self.__find_area(i)
        address = ", ".join([
            area.get_str('street-address'),
            area.get_str('postal-code'),
            area.get_str('locality')
        ])
        e = MadridEsEvent(
            id=i.get_int('id'),
            url=i.get_str('link'),
            title=str_line(i.get_str('title')),
            price=i.get_price(),
            description=str_line(i.get_str_or_none('description')),
            dtstart=date_to_str(i.get_datetime("dtstart", "%Y-%m-%d %H:%M:%S.0")),
            dtend=date_to_str(i.get_datetime("dtend", "%Y-%m-%d %H:%M:%S.0")),
            audience=str_tuple(i.get_str_or_none("audience"), r"\s*,\s*"),
            recurrence=i.get("recurrence") is not None,
            latitude=loc.get_float('latitude'),
            longitude=loc.get_float('longitude'),
            location=i.get_str('event-location'),
            address=address
        )
        return e

    def __find_area(self, e: MadridEsDictWraper):
        addr = e.get_dict('address')
        area = e.get_dict_or_none('area')
        if area:
            return area
        url = e.get_dict_or_empty('relation').get_str_or_none('@id') or ''
        if re.match(r"^https://datos\.madrid\.es/egob/catalogo/tipo/entidadesyorganismos/\S+\.json$", url):
            obj = DictWraper(self.get_graph_list_len_1(url))
            addr = obj.get_dict_or_none("address")
            if addr:
                return addr
        raise ValueError(f"Área no encontrada en {e}")

    @HashCache("rec/api_madrid_es/dataset.json")
    def __get_events(self):
        events_dict: dict[int, dict] = {}
        sources = {
            url: self.get_graph_list(url) for url in (
                "https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.json",
                "https://datos.madrid.es/egob/catalogo/206717-0-agenda-eventos-bibliotecas.json",
                "https://datos.madrid.es/egob/catalogo/206974-0-agenda-eventos-culturales-100.json"
            )
        }
        sources = dict(sorted(sources.items(), key=lambda kv:(-len(kv[1]), kv[0])))
        for i, (url, evs) in enumerate(sources.items()):
            logger.info(f"[{len(evs):5d}] {url}")
            new_ids: set[int] = set()
            for e in evs:
                _id_ = int(e['id'])
                if _id_ not in events_dict:
                    new_ids.add(_id_)
                obj = events_dict.get(_id_, {
                    "__source__": url
                })
                for k, v in e.items():
                    if obj.get(k) is None:
                        obj[k] = v
                events_dict[_id_] = obj
            if i > 0:
                count = len(new_ids)
                logger.info(f"[+{count:4d}] {url} {', '.join(map(str, sorted(new_ids)))}")
        return list(events_dict.values())

    @TupleCache("rec/api_madrid_es.json", builder=MadridEsEvent.build)
    def get_events(self):
        events_dict = self.__get_events()
        size = len(events_dict)
        events: set[MadridEsEvent] = set()
        for e in events_dict:
            x = self.__obj_to_event(e)
            if x is not None:
                events.add(x)
        logger.info(f"[{size:5d}] Total")
        logger.info(f"[-{size-len(events):4d}] Descartados")
        return tuple(sorted(events))

if __name__ == "__main__":
    from core.log import config_log
    config_log("log/apimadrides.log", log_level=(logging.INFO))
    m = MadridEs()
    print(len(m.get_events()))
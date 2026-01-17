from requests.sessions import Session
import logging
from typing import NamedTuple, Optional
from core.dictwraper import DictWrapper
from datetime import datetime, date
from requests.exceptions import JSONDecodeError
from core.filemanager import FileManager
from core.cache import HashCache, TupleCache
from core.util import get_obj, find_euros
import json
import re
from aiohttp import ClientResponse
from core.fetcher import Getter


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


async def rq_to_json(r: ClientResponse):
    try:
        return await r.json()
    except JSONDecodeError:
        text = await r.text()
        text = re_sp.sub(r" ", text).strip()
        if len(text) == 0:
            logger.critical(f"{r.url} is empty: {text}")
        try:
            return json.loads(text)
        except Exception:
            pass
        logger.critical(f"{r.url} is not a JSON: {text}")
        raise


async def rq_to_graph_list(r: ClientResponse) -> list[dict]:
    obj = await rq_to_json(r)
    if not isinstance(obj, dict):
        raise ValueError(f"data is not a dict {r.url}")
    lst = obj.get("@graph")
    if not isinstance(lst, list):
        raise ValueError(f"@graph is not list {r.url}")
    if len(lst) == 0:
        logger.critical(f"@graph is empty list {r.url}")
        return []
    if not all(isinstance(x, dict) for x in lst):
        raise ValueError(f"@graph is not list[dict] {r.url}")
    return lst


async def rq_to_graph_list_len_1(r: ClientResponse):
    graph = await rq_to_graph_list(r)
    if len(graph) != 1:
        raise ValueError(f"@graph is not len(list[dict]) == 1 {r.url}")
    return graph[0]


class ApiSession:
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

    @HashCache("rec/api_madrid_es/hash/{}.json")
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


SESSION = ApiSession()


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
        return d.strftime("%Y-%m-%d %H:%M")
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    raise ValueError(d)


class MadridEsPlace(NamedTuple):
    latitude: float
    longitude: float
    location: str
    address: str


class MadridEsEvent(NamedTuple):
    id: int
    url: str
    title: str
    description: str
    dtstart: str
    dtend: str
    audience: tuple[str, ...]
    recurrence: bool
    typ: Optional[str] = None
    place: Optional[MadridEsPlace] = None
    price: Optional[float | int] = None

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
            if k == "place" and isinstance(v, dict):
                obj[k] = MadridEsPlace(**v)
        return MadridEsEvent(**obj)


class MadridEsDictWrapper(DictWrapper):

    def get_api_org(self):
        url = self.get_dict_or_empty('relation').get_str_or_none('@id') or ''
        if re.match(r"^https://datos\.madrid\.es/egob/catalogo/tipo/entidadesyorganismos/\S+\.json$", url):
            return url

    @property
    def __organismo(self):
        url = self.get_api_org()
        if url:
            return DictWrapper(SESSION.get_graph_list_len_1(url))
        return DictWrapper({})

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
        free = self.get_bool_or_none('__free__')
        if free is True:
            return 0
        prc = self.get_str_or_none('price')
        if prc not in (
            None,
            "Entradas disponibles próximamente en entradas.com y en la taquilla del recinto",
            "Consultar descuentos especiales",
        ):
            logger.critical(f"Campo price inexperado: {prc}")

    def get_location(self):
        loc = self.get_dict_or_none('location')
        if loc:
            return loc
        loc = self.__organismo.get_dict_or_none('location')
        if loc:
            return loc

    def event_location(self):
        el = self.get_str_or_none('event-location')
        if el is not None:
            return el
        tt = self.__organismo.get_str_or_none('title')
        if tt is not None:
            return tt
        org = self.__organismo.get_dict_or_empty('organization')
        nam = org.get_str_or_none('organization-name')
        if nam is not None:
            return nam
        raise ValueError(f"[{self.get('id')}] event-location no encontrado")

    def get_address(self):
        area = self.__get_address()
        if area:
            return ", ".join([
                area.get_str('street-address'),
                area.get_str('postal-code'),
                area.get_str('locality')
            ])
        raise ValueError(f"[{self.get('id')}] address no encontrado")

    def __get_address(self):
        addr = self.get_dict_or_none('address')
        area = self.get_dict_or_none('area')
        if area:
            return area
        addr = self.__organismo.get_dict_or_none("address")
        if addr:
            return addr


class ApiMadridEs:

    def __obj_to_event(self, obj: dict):
        i = MadridEsDictWrapper(obj)
        place = None
        dtend = i.get_datetime("dtend", "%Y-%m-%d %H:%M:%S.0")
        loc = i.get_location()
        if loc is not None:
            place = MadridEsPlace(
                latitude=loc.get_float('latitude'),
                longitude=loc.get_float('longitude'),
                location=i.event_location(),
                address=i.get_address()
            )
        dtstart = i.get_datetime("dtstart", "%Y-%m-%d %H:%M:%S.0")
        hm_tm = i.get_str_or_none('time')
        if hm_tm not in (None, dtstart.strftime("%H:%M")):
            logger.critical(f"time={hm_tm} dtstart={dtstart} in {obj}")
        e = MadridEsEvent(
            id=i.get_int('id'),
            typ=i.get_str_or_none("@type"),
            url=i.get_str('link'),
            title=str_line(i.get_str('title')),
            price=i.get_price(),
            description=str_line(i.get_str_or_none('description')),
            dtstart=date_to_str(dtstart),
            dtend=date_to_str(dtend),
            audience=str_tuple(i.get_str_or_none("audience"), r"\s*,\s*"),
            recurrence=i.get("recurrence") is not None,
            place=place
        )
        return e

    @HashCache("rec/api_madrid_es/dataset.json")
    def __get_events(self):
        FREE_IF_IN = (
            'https://datos.madrid.es/egob/catalogo/206717-0-agenda-eventos-bibliotecas.json',
        )
        events_dict: dict[int, dict] = {}
        sources: dict[str, list[dict]] = Getter(
            onread=rq_to_graph_list
        ).get(
            "https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.json",
            "https://datos.madrid.es/egob/catalogo/206717-0-agenda-eventos-bibliotecas.json",
            "https://datos.madrid.es/egob/catalogo/206974-0-agenda-eventos-culturales-100.json",
            'https://datos.madrid.es/egob/catalogo/212504-0-agenda-actividades-deportes.json'
        )
        sources = dict(sorted(sources.items(), key=lambda kv: (-len(kv[1]), kv[0])))
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
                if url in FREE_IF_IN:
                    obj['__free__'] = True
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
        discard = size-len(events)
        if discard:
            logger.info(f"[-{discard:4d}] Descartados")
        return tuple(sorted(events))


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/apimadrides.log", log_level=(logging.INFO))
    m = ApiMadridEs()
    print(len(m.get_events()))

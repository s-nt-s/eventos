import logging
from typing import NamedTuple, Optional
from core.dictwraper import DictWrapper
from datetime import datetime, date
from json.decoder import JSONDecodeError
from core.filemanager import FileManager
from core.cache import TupleCache
from core.util import get_obj, un_camel
import json
import re
from aiohttp import ClientResponse
from core.fetcher import Getter
from bs4 import BeautifulSoup
from core.madrid_es.tp import Place


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")
re_org_api = re.compile(r"^https://datos\.madrid\.es/egob/catalogo/tipo/entidadesyorganismos/\S+\.json$")


async def rq_to_json(r: ClientResponse):
    try:
        return await r.json()
    except JSONDecodeError:
        text = await r.text()
        text = re_sp.sub(r" ", text).strip()
        text = re.sub('"longitude": --+', '"longitude": -', text)
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
    return list(FileManager.parse_obj(i, compact=True) for i in lst)


async def rq_to_graph_list_len_1(r: ClientResponse):
    graph = await rq_to_graph_list(r)
    if len(graph) != 1:
        raise ValueError(f"@graph is not len(list[dict]) == 1 {r.url}")
    obj = FileManager.parse_obj(graph[0], compact=True) or {}
    return obj


async def rq_to_label(r: ClientResponse):
    soup = BeautifulSoup(await r.read(), "xml")
    url = r.history[0].url if r.history else r.url
    node = soup.find("skos:prefLabel", attrs={"xml:lang": "es"})
    if node is None:
        node = soup.find("skos:prefLabel")
    if node is not None:
        txt = re_sp.sub(" ", node.get_text()).strip()
        return txt
    node = soup.find("rdf:Description")
    about = node.attrs.get('rdf:about') if node else None
    if about is None:
        val = {
            "https://datos.madrid.es/egob/kos/Provincia/Madrid/Municipio/Madrid%2028042/Distrito/Barajas": "Barajas",
            "https://datos.madrid.es/egob/kos/Provincia/Madrid/Municipio/Madrid/Distrito/Fuencarral-ElPardo": "Fuencarral El Pardo"
        }.get(str(url))
        if val is not None:
            return val
        logger.critical(f"skos:prefLabel not found in {url}")
        return None
    about = about.rsplit("/", 1)[-1]
    about = un_camel(about)
    return about


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


class Item(NamedTuple):
    id: int
    url: str
    title: str
    description: str
    dtstart: str
    dtend: str
    recurrence: bool
    audience: tuple[str, ...] = tuple()
    category: Optional[str] = None
    place: Optional[Place] = None
    price: Optional[str] = None
    free: Optional[bool] = None

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
            if k == "place" and isinstance(v, dict):
                obj[k] = Place(**v)
        return Item(**obj)


class MadridEsDictWrapper(DictWrapper):

    def get_api_org(self):
        url = self.get_dict_or_empty('relation').get_str_or_none('@id') or ''
        if re.match(r"^https://datos\.madrid\.es/egob/catalogo/tipo/entidadesyorganismos/\S+\.json$", url):
            return url

    @property
    def __organismo(self):
        org = self.get('__organization__')
        if isinstance(org, dict):
            return DictWrapper(org)
        return DictWrapper({})

    def is_free(self):
        if self.get_bool('free'):
            return True
        if self.get_bool_or_none('__free__') is True:
            return True
        return False

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
            return ", ".join(x for x in [
                area.get_str('street-address'),
                area.get_str_or_none('postal-code'),
                area.get_str('locality')
            ] if x is not None)
        raise ValueError(f"[{self.get('id')}] address no encontrado")

    def __get_address(self):
        addr = self.get_dict_or_empty('address')
        area = addr.get_dict_or_none('area')
        if area:
            return area
        addr = self.__organismo.get_dict_or_none("address")
        if addr:
            return addr

    def get_district(self):
        ko_district = (
            "https://datos.madrid.es/egob/kos/Provincia/Madrid/Municipio/Madrid/Distrito/Distrito",
        )
        is_ko = False
        for obj in (
            self.get_dict_or_empty('address'),
            self.__organismo.get_dict_or_empty("address"),
        ):
            aux = obj.get_dict_or_empty('district')
            district = aux.get_str_or_none('@id')
            if district is None:
                continue
            if district not in ko_district:
                return district
            is_ko = True
        msg = f"[{self.get('id')}] district no encontrado"
        if is_ko:
            logger.critical(msg)
        else:
            raise ValueError(msg)


class Dataset(NamedTuple):
    events: list[dict]
    organizations: dict[str, dict]

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        return Dataset(**obj)


class DatosMadridEs:

    def __obj_to_event(self, obj: dict):
        i = MadridEsDictWrapper(obj)
        place = None
        dtend = i.get_datetime("dtend", "%Y-%m-%d %H:%M:%S.0")
        loc = i.get_location()
        place = Place(
            latitude=loc.get_float('latitude'),
            longitude=loc.get_float('longitude'),
            location=i.event_location(),
            address=i.get_address(),
            district=i.get_district()
        ) if loc else None
        dtstart = i.get_datetime("dtstart", "%Y-%m-%d %H:%M:%S.0")
        hm_tm = i.get_str_or_none('time')
        if hm_tm not in (None, dtstart.strftime("%H:%M")):
            if dtstart.strftime("%H:%M") == "00:00":
                h, m = map(int, hm_tm.split(":"))
                dtstart = dtstart.replace(hour=h, minute=m)
            else:
                logger.critical(f"time={hm_tm} dtstart={dtstart} in {obj}")
        e = Item(
            id=i.get_int('id'),
            category=i.get_str_or_none("@type"),
            url=i.get_str('link'),
            title=str_line(i.get_str('title')),
            price=i.get_str_or_none('price'),
            description=i.get_str_or_none('description'),
            dtstart=date_to_str(dtstart),
            dtend=date_to_str(dtend),
            audience=tuple(map(un_camel, str_tuple(i.get_str_or_none("audience"), r"\s*,\s*"))),
            recurrence=i.get("recurrence") is not None,
            place=place,
            free=i.is_free()
        )
        return e

    @TupleCache("rec/apimadrides/dataset.json", builder=Dataset.build)
    def __get_events(self):
        ORGS = (
            'https://datos.madrid.es/dataset/300331-0-equipamientos-municipales/resource/300331-2-equipamientos-municipales-json/download/300331-2-equipamientos-municipales-json.json',
        )
        FREE_IF_IN = (
            'https://datos.madrid.es/dataset/206717-0-agenda-eventos-bibliotecas/resource/206717-1-agenda-eventos-bibliotecas-json/download/206717-1-agenda-eventos-bibliotecas-json.json',
            #'https://datos.madrid.es/egob/catalogo/206717-0-agenda-eventos-bibliotecas.json',
        )
        EVTS = (
            'https://datos.madrid.es/dataset/300107-0-agenda-actividades-eventos/resource/300107-5-agenda-actividades-eventos-json/download/300107-5-agenda-actividades-eventos-json.json',
            #"https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.json",
            'https://datos.madrid.es/dataset/206717-0-agenda-eventos-bibliotecas/resource/206717-1-agenda-eventos-bibliotecas-json/download/206717-1-agenda-eventos-bibliotecas-json.json',
            #"https://datos.madrid.es/egob/catalogo/206717-0-agenda-eventos-bibliotecas.json",
            'https://datos.madrid.es/dataset/206974-0-agenda-eventos-culturales-100/resource/206974-0-agenda-eventos-culturales-100-json/download/206974-0-agenda-eventos-culturales-100-json.json',
            #"https://datos.madrid.es/egob/catalogo/206974-0-agenda-eventos-culturales-100.json",
            'https://datos.madrid.es/dataset/212504-0-agenda-actividades-deportes/resource/212504-2-agenda-actividades-deportes-json/download/212504-2-agenda-actividades-deportes-json.json',
            #'https://datos.madrid.es/egob/catalogo/212504-0-agenda-actividades-deportes.json',
        )
        events_dict: dict[int, dict] = {}
        sources: dict[str, list[dict]] = Getter(
            onread=rq_to_graph_list
        ).get(
            *EVTS,
            *ORGS
        )

        org_list: dict[str, list[dict]] = {}
        for o in ORGS:
            org_list[o] = sources.pop(o, [])

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

        events = list(events_dict.values())
        orgs: set[str] = set()
        for e in events:
            relation = e.get("relation")
            if isinstance(relation, dict):
                url = relation.get('@id')
                if isinstance(url, str):
                    url = url.strip()
                    if re_org_api.match(url):
                        orgs.add(url)
                        e['__organization__'] = url

        org_data: dict[str, dict] = {}
        for o_list in org_list.values():
            for o in o_list:
                org_data[o['@id']] = o
                aux = {
                    'https://datos.madrid.es/egob/catalogo/tipo/entidadesyorganismos/1916-centro-cultura-contemporanea-condeduque.json': 'https://datos.madrid.es/egob/catalogo/tipo/entidadesyorganismos/1916-centro-cultura-contemporanea-conde-duque.json',
                    'https://datos.madrid.es/egob/catalogo/tipo/entidadesyorganismos/5463464-centro-educacion-ambiental-retiro.json':'https://datos.madrid.es/egob/catalogo/tipo/entidadesyorganismos/5463464-centro-informacion-educacion-ambiental-huerto-retiro.json',
                }.get(o['@id'])
                if aux is not None and aux not in org_data:
                    org_data[aux] = o

        for k, v in Getter(
            onread=rq_to_graph_list_len_1
        ).get(*orgs.difference(org_data.keys())).items():
            org_data[k] = v

        dts = Dataset(
            events=list(events_dict.values()),
            organizations=org_data
        )
        return dts

    @TupleCache("rec/apimadrides/items.json", builder=Item.build)
    def get_events(self):
        dataset = self.__get_events()
        size = len(dataset.events)
        events: set[Item] = set()
        for e in dataset.events:
            org = dataset.organizations.get(e.get('__organization__'))
            if isinstance(org, dict):
                e['__organization__'] = org
            x = self.__obj_to_event(e)
            if x is not None:
                events.add(x)
        logger.info(f"[{size:5d}] Total")
        discard = size-len(events)
        if discard:
            logger.info(f"[-{discard:4d}] Descartados")

        url_label: set[str] = set()
        for e in events:
            if e.category:
                url_label.add(e.category)
            if e.place and e.place.district:
                url_label.add(e.place.district)

        data_label: dict[str, str] = Getter(
            onread=rq_to_label,
            raise_for_status=False
        ).get(*url_label)

        evs: set[Item] = set()
        for e in events:
            e = e._replace(
                category=data_label.get(e.category)
            )
            if e.place and e.place.district:
                e = e._replace(place=e.place._replace(
                    district=data_label.get(e.place.district)
                ))
            evs.add(e)

        return tuple(sorted(evs))


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/apimadrides.log", log_level=(logging.INFO))
    m = DatosMadridEs()
    print(len(m.get_events()))

from requests.sessions import Session
import logging
from typing import NamedTuple, Optional
from collections import defaultdict
from core.dictwraper import DictWraper
from datetime import datetime
from requests.exceptions import JSONDecodeError
from functools import cache
from core.filemanager import FileManager
from core.cache import HashCache
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
    dtstart: datetime
    dtend: datetime
    audience: tuple[str, ...]
    recurrence: bool
    organization: str
    price: Optional[float | int] = None


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

    @HashCache("rec/api_madrid_es/{}.json", compact=True)
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
        return list(lst)

    @cache
    def get_graph_list_len_1(self, url: str):
        graph = self.get_graph_list(url)
        if len(graph) != 1:
            raise ValueError(f"@graph is not len(list[dict]) == 1 {url}")
        return graph[0]

    def __get_event_list(self, url: str):
        events: set[MadridEsEvent] = set()
        for i in map(DictWraper, self.get_graph_list(url)):
            e = MadridEsEvent(
                id=i.get_int('id'),
                url=i.get_str('link'),
                title=str_line(i.get_str('title')),
                price=self.__find_price(i),
                description=str_line(i.get_str_or_none('description')),
                dtstart=i.get_datetime("dtstart", "%Y-%m-%d %H:%M:%S.0"),
                dtend=i.get_datetime("dtend", "%Y-%m-%d %H:%M:%S.0"),
                audience=str_tuple(i.get_str_or_none("audience"), r"\s*,\s*"),
                recurrence=i.get("recurrence") is not None,
                organization=self.__find_organization(i)
            )
            events.add(e)
        return events

    def __find_organization(self, e: DictWraper):
        for k in (
            "organization-name",
            "event-location"
        ):
            org = e.get_str_or_none(k)
            if org:
                return org
        rel = e.get_dict_or_empty("relation")
        url = rel.get_str_or_none('@id') or ''
        if re.match(r"^https://datos\.madrid\.es/egob/catalogo/tipo/entidadesyorganismos/\S+\.json$", url):
            obj = DictWraper(self.get_graph_list_len_1(url))
            title = obj.get_str_or_none("title")
            if title:
                return title
            obj = obj.get_dict_or_empty('organization')
            name = obj.get_str_or_none('organization-name')
            if name:
                return name
        raise ValueError(f"Organización no encontrada en {e}")

    def get_events(self):
        events_dict: dict[int, list[MadridEsEvent]] = defaultdict(list)
        for url in (
            "https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.json",
            "https://datos.madrid.es/egob/catalogo/206717-0-agenda-eventos-bibliotecas.json",
            "https://datos.madrid.es/egob/catalogo/206974-0-agenda-eventos-culturales-100.json"
        ):
            for e in self.get_graph_list(url):
                events_dict[int(e['id'])].append(e)

        events: set[MadridEsEvent] = set()
        for evs in events_dict.values():
            events.add(self.__merge(*evs))
        return tuple(sorted(events))

    def __merge(self, *events_obj):
        obj: dict[str, set] = defaultdict(set)
        for e in map(MadridEsDictWraper, events_obj):
            obj['id'].add(e.get_int('id'))
            obj['url'].add(e.get_str('link'))
            obj['title'].add(str_line(e.get_str('title')))
            obj['price'].add(e.get_price())
            obj['description'].add(str_line(e.get_str_or_none('description')))
            obj['dtstart'].add(e.get_datetime("dtstart", "%Y-%m-%d %H:%M:%S.0"))
            obj['dtend'].add(e.get_datetime("dtend", "%Y-%m-%d %H:%M:%S.0"))
            obj['audience'].add(str_tuple(e.get_str_or_none("audience"), r"\s*,\s*"))
            obj['recurrence'].add(e.get("recurrence") is not None)
            obj['organization-name'].add(e.get_str_or_none("organization-name"))
            obj['event-location'].add(e.get_str_or_none('event-location'))


if __name__ == "__main__":
    m = MadridEs()
    list(m.get_events())
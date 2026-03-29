from requests import Session as ReqSession
from core.cache import TupleCache
from urllib.parse import urljoin
from core.util import parse_obj, get_obj, re_or, find_euros
from typing import NamedTuple, Optional
from types import MappingProxyType
from core.event import Event, Category, CategoryUnknown, Session
from core.place import Places
from functools import cache, cached_property
from core.web import buildSoup, get_text
import re
import logging
from core.fetcher import Getter
from aiohttp import ClientResponse
from bs4 import Tag
from datetime import datetime

logger = logging.getLogger(__name__)


@cache
def html_to_text(soup: str | Tag):
    if soup is None:
        return None
    if isinstance(soup, str):
        soup = buildSoup(None, soup)
    for x in soup.select("br, p"):
        x.append("\n")
    return get_text(soup)


class InfoSoup(NamedTuple):
    capacity: Optional[str] = None
    audience: Optional[str] = None
    shop: Optional[str] = None
    txt: Optional[str] = None
    disabled: Optional[bool] = False


class Index(NamedTuple):
    categories: dict[int, str]
    info: dict[int, InfoSoup]
    items: tuple[dict, ...]

    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        obj['categories'] = MappingProxyType({int(k): v for k, v in obj.get("categories", {}).items()})
        obj['info'] = MappingProxyType({int(k): InfoSoup(**v) for k, v in obj.get("info", {}).items()})
        obj['items'] = tuple(obj.get("items", []))
        return Index(**obj)


async def rq_to_info(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    info = {}
    div = soup.select_one("#stickyAside")
    if div is None:
        return None
    b = div.find("button", string="Ver condiciones")
    if b:
        b.extract()
    for dt, dd in zip(div.select("dt"), div.select("dd")):
        k = get_text(dt)
        v = get_text(dd)
        if None in (k, v):
            continue
        k = {
            "aforo": "capacity",
            "audiencia": "audience"
        }.get(k.lower())
        if k:
            info[k] = v
    a = div.select_one('a[href^="https://entradas.museoreinasofia.es/es/"]')
    if a:
        info['shop'] = a.attrs["href"]
    f = div.select_one("a[class*='LinkButton-module--disabled']")
    if f:
        info['disabled'] = True
    txt = []
    for n in div.select("div[class*='ButtonWithHelpText']"):
        s = html_to_text(n)
        if s:
            txt.append(s)
    if txt:
        info['txt'] = "\n\n".join(txt)
    return InfoSoup(**info)


def _has_to_be(i: dict, k: str, val):
    v = i.get(k)
    if v is None and val is not None:
        raise ValueError(f"¿{k}={v}?")
    if val is None and v is not None:
        raise ValueError(f"¿{k}={v}?")
    if type(v) is not type(val):
        raise ValueError(f"¿{k}={v}?")
    if v != val:
        raise ValueError(f"¿{k}={v}?")
    del i[k]


def _unwrap_dict(i: dict, k: str, attr: str):
    v = i.get(k)
    if not isinstance(v, dict):
        return
    if not isinstance(v, dict) or tuple(v.keys()) != (attr, ):
        raise ValueError(f"¿{k}={v}?")
    val = v[attr]
    i[k] = val


def _has_to_be_list_value_dict(i: dict, k: str, attr: str):
    v = i.get(k)
    if v is None:
        return
    if not isinstance(v, list):
        raise ValueError(f"¿{k}={v}?")
    arr = []
    for x in v:
        if not isinstance(x, dict) or tuple(x.keys()) != (attr, ):
            raise ValueError(f"¿{k}={v}?")
        arr.append(x[attr])
    i[k] = arr


def re_parse(obj):
    if not isinstance(obj, dict):
        return obj
    for v, ks in {
        'path': ('url',),
        'id': ('language',),
        'data': ('activities', ),
        'entity': ('mainMedia', 'parent', 'location'),
        'url': ('searchUrl', ),
        'value': (
            'title',
            'subtitle',
            'captionTitle',
            'description',
            'captionSubtitle',
            'languageText'
        ),
    }.items():
        for k in ks:
            _unwrap_dict(obj, k, v)
    for v, ks in {
        'entity': ('categories', 'events', 'dates',),
        'value': ('processedDates',),
    }.items():
        for k in ks:
            _has_to_be_list_value_dict(obj, k, v)
    url = obj.get("url")
    if url:
        obj['url'] = urljoin(ReinaSofia.ROOT, url)
    src = obj.get("originalSrc")
    if src:
        obj['originalSrc'] = urljoin(ReinaSofia.IMG, src)
    s_id = obj.get('id')
    if isinstance(s_id, str) and s_id.isdecimal():
        obj['id'] = int(s_id)
    lang = obj.get("language")
    if lang not in ("es", None):
        return None
    if lang == "es":
        del obj['language']
    return obj


class ReinaSofia:
    ROOT = "https://www.museoreinasofia.es"
    IMG = "https://recursos.museoreinasofia.es/styles/large_landscape/public/"
    SEARCH = "https://buscador.museoreinasofia.es/api/search?langcode=es&exactMatch=false"
    ENTRADA_GENERAL = 12

    def __init__(self):
        self.__s = ReqSession()
        self.__size = 100

    @cached_property
    @TupleCache("rec/reinasofia/index.json", builder=Index.build)
    def _index(self):
        r = self.__s.get(f"{ReinaSofia.SEARCH}&pageSize={self.__size}")
        js = r.json()
        arr: list[dict] = []
        categories: dict[int, str] = {}
        urls: dict[str, int] = {}
        now = datetime.now().isoformat(timespec="seconds")
        for i in js['results']:
            if not isinstance(i, dict):
                raise ValueError(i)
            i = parse_obj(
                i,
                compact=True,
                re_parse=re_parse,
                rm_key=('uncropable', 'score', 'translations'),
                keep_re_parse_none=True,
            )
            parent: dict = i.get('parent', {})
            for c in parent.get('categories', []):
                categories[c['id']] = c.get("name") or c.get("title")
            activities: list[dict] = parent.get('activities', [])
            for a in activities:
                for c in a.get("categories", []):
                    categories[c['id']] = c.get("name") or c.get("title")
            t = i.get('template')
            if t == "past":
                continue
            processedDates = []
            for d in i.get('processedDates', []):
                if d >= now:
                    processedDates.append(d)
            if len(processedDates) == 0:
                continue
            i['processedDates'] = processedDates
            _has_to_be(i, "template", "future")
            _has_to_be(i, "hidden", False)
            _has_to_be(i, "isPublished", True)
            _has_to_be(i, "bundle", "activity")
            if 'parent' not in i:
                i['parent'] = None
            urls[i['url']] = i['id']
            arr.append(i)

        info: dict[int, InfoSoup] = {}
        for u, i in Getter(
            onread=rq_to_info
        ).get(*urls.keys()).items():
            if i is not None:
                info[urls[u]] = i
        return Index(
            categories=MappingProxyType(categories),
            info=info,
            items=tuple(arr),
        )

    @property
    @TupleCache("rec/reinasofia.json", builder=Event.build)
    def events(self):
        logger.info("Buscando eventos en Reina Sofia")
        evs: set[Event] = set()
        for i in self._index.items:
            _id_ = i['id']
            info = self._index.info.get(_id_)
            if info and info.disabled is True:
                continue
            url = i['url']
            e = Event(
                id=f"rs{_id_}",
                url=url,
                name=html_to_text(i['title']),
                img=i['mainMedia']['image']['originalSrc'],
                price=self.__find_price(i),
                category=self.__find_category(i),
                place=Places.MUSEO_REINA_SOFIA.value,
                duration=self.__find_duration(i),
                sessions=tuple(Session(
                    date=s.replace("T", " ")[:-3],
                    url=info.shop if info else None,
                ) for s in i['processedDates']),
                cycle=None
            )
            if e.category != Category.CINEMA:
                e = e.merge(
                    cycle=self.__find_cycle(e, i)
                )
            evs.add(e)
        logger.info(f"Buscando eventos en Reina Sofia = {len(evs)}")
        return tuple(sorted(evs))

    def __find_cycle(self, ev: Event, i: dict):
        if ev.category == Category.CONFERENCE and re_or(
            ev.name,
            "Guernica africano",
            "African Guernica",
            flags=re.I
        ):
            return "Guernica africano"
        return html_to_text(i.get('subtitle'))

    def __find_price(self, i: dict):
        _id_ = i['id']
        info = self._index.info.get(_id_)
        if info:
            price = find_euros(info.txt)
            if price is not None:
                return price
            if re_or(
                info.txt,
                "sometido a la entrada general al museo",
                flags=re.I
            ):
                return ReinaSofia.ENTRADA_GENERAL
        activities: list[dict] = (i.get('parent') or {}).get('activities', [])
        if not activities:
            return 0
        for a in activities:
            if a['id'] == _id_:
                for e in a.get("events", []):
                    capacity = e.get("capacity", "").lower()
                    price = {
                        "sometido a la entrada general al museo": ReinaSofia.ENTRADA_GENERAL
                    }.get(capacity)
                    if price is not None:
                        return price
        return 0

    def __find_category(self, i: dict):
        _id_ = i['id']
        info = self._index.info.get(_id_)
        if info:
            if re_or(
                info.audience,
                r"Educaci[oó]n Infantil",
                r"p[uú]blico infantil",
                r"familias",
                flags=re.I
            ):
                return Category.CHILDISH
            if re_or(
                info.audience,
                r"personas mayores",
                flags=re.I
            ):
                return Category.SENIORS
            if re_or(
                info.audience,
                r"adolescentes",
                r"j[oó]venes",
                flags=re.I
            ):
                return Category.YOUTH
            if re_or(
                info.audience,
                r"investigador[ao]s",
                r"equipo de voluntariado",
                r"docentes",
                flags=re.I
            ):
                return Category.NON_GENERAL_PUBLIC
            if re_or(
                info.txt,
                (r"Una comisi[óo]n conjunta", "seleccionar[áa] a las personas participantes"),
                flags=re.I
            ):
                return Category.NON_GENERAL_PUBLIC

        title = html_to_text(i['title'])
        subtitle = html_to_text(i.get('subtitle'))
        if re_or(
            subtitle,
            "para grupos de Educaci[oó]n Infantil",
            r"p[uú]blico infantil",
            flags=re.I
        ):
            return Category.CHILDISH
        if re_or(
            subtitle,
            "Visitas para asociaciones",
            flags=re.I
        ):
            return Category.ORGANIZATIONS
        if re_or(
            subtitle,
            "para docentes",
            flags=re.I
        ):
            return Category.NON_GENERAL_PUBLIC
        arr: list[str] = []
        for c in i.get('processedCategories', []):
            if c in self._index.categories:
                arr.append(self._index.categories[c])
        txt = None
        if len(arr):
            txt = "\n".join(arr)
            if re_or(
                txt,
                "Cine",
                flags=re.I
            ):
                return Category.CINEMA
            if re_or(
                txt,
                "Taller(es)?",
                flags=re.I
            ):
                return Category.WORKSHOP
            if re_or(
                txt,
                "conferencias?",
                "Encuentros?",
                flags=re.I
            ):
                return Category.CONFERENCE
            if re_or(
                txt,
                "visita",
                flags=re.I
            ):
                return Category.VISIT
            if re_or(
                txt,
                "Artes en vivo",
                flags=re.I
            ):
                return Category.THEATER
            if re_or(
                txt,
                "exposici[oó]n(es)?",
                flags=re.I
            ):
                return Category.EXPO
        if re_or(
            subtitle,
            "grupo de estudio",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            title,
            ("comunidad", "investigaci[oó]n"),
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            subtitle,
            "Mediaci[oó]n en sala para p[uú]blico",
            flags=re.I
        ):
            return Category.VISIT
        logger.critical(str(CategoryUnknown(i['url'], f"{subtitle} {txt}")))
        return Category.UNKNOWN

    def __find_duration(self, i: dict):
        _id_ = i['id']
        activities: list[dict] = (i.get('parent') or {}).get('categories', [])
        if not activities:
            return 60
        durations: set[int] = set()
        for a in activities:
            d = a.get('duration')
            if not isinstance(d, int):
                continue
            if a['id'] == _id_:
                return d
            durations.add(d)
        if durations:
            return max(durations)
        return 60


if __name__ == "__main__":
    r = ReinaSofia()
    r.events

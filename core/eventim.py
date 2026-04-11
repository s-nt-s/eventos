from core.cache import HashCache
from core.util import parse_obj
import logging
from typing import NamedTuple
import re
from datetime import datetime
from core.md import MD
import pytz
from requests import Session
from core.fetcher import Getter
from aiohttp import ClientResponse


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")
DT_NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))
NOW = DT_NOW.strftime("%Y-%m-%d %H:%M")


async def rq_to_events(r: ClientResponse):
    js = await r.json()
    ev = js['data']['events']
    if not isinstance(ev, list):
        raise ValueError(str(r.url))
    if not all(isinstance(i, dict) for i in ev):
        raise ValueError(str(r.url))
    return ev


def trim(s: str):
    if s is None:
        return s
    s = re_sp.sub(" ", s).strip()
    if len(s):
        return s


def _iter(obj: dict, *args):
    for k in args:
        v = obj.get(k)
        if v is not None:
            yield k, v


def _iso_to_str(s: str):
    return datetime.fromisoformat(s).strftime("%Y-%m-%d %H:%M")


def _date(s: str):
    if s is None:
        return None
    if isinstance(s, list):
        return list(map(_iso_to_str, s))
    return _iso_to_str(s)


def _re_parse(obj):
    if not isinstance(obj, dict):
        return obj
    for k, v in _iter(
        obj,
        "categoryId",
        'zipCode',
    ):
        obj[k] = int(v)
    for k, v in _iter(
        obj,
        "start",
        "end",
        "salesStart",
        "salesEnd",
        "eventDates"
    ):
        obj[k] = _date(v)
    im = obj.get("image")
    if im is not None:
        if not isinstance(im, dict) and tuple(im.keys()) != ('id',):
            raise ValueError(obj)
        obj["image"] = f"https://www.eventim-light.com/es/api/image/{im['id']}/shop_cover_v3/webp"
    et1 = obj.get("eventType")
    et2 = obj.get("eventTypeCount")
    if et1 and et2:
        raise ValueError(obj)
    if et2 is not None:
        if not isinstance(et2, dict) and tuple(sorted(et2.keys())) != ('NORMAL', 'ONLINE'):
            raise ValueError(obj)
        tp = tuple(
            kv[0] for kv in
            sorted(
                (kv for kv in et2.items() if kv[1] > 0),
                key=lambda kv: (-kv[1], kv[0])
            )
        )
        if len(tp) == 0:
            raise ValueError(obj)
        if len(tp) == 1:
            del obj['eventTypeCount']
        obj['eventType'] = " ".join(tp)

    return obj


def get_data(url: str, js: dict):
    dt = js.get('data')
    if not isinstance(dt, list):
        raise ValueError(f"{url} no es un {{'data': list[dict]}}")
    if not all(isinstance(d, dict) for d in dt):
        raise ValueError(f"{url} no es un {{'data': list[dict]}}")
    return dt


class Eventim:
    def __init__(self, id: str):
        self.__id = id
        self.__s = Session()
        self.__getter_events = Getter(
            onread=rq_to_events
        )

    @property
    def id(self):
        return self.__id

    @HashCache("rec/eventim/{}.json")
    def __get_data(self, url: str) -> list[dict]:
        series: set[str] = set()
        data: list[dict] = []
        r = self.__s.get(url)
        js = r.json()
        for e in get_data(url, js):
            _id_ = e['id']
            tp = e['type']
            if tp not in ("event", "series"):
                raise ValueError(e)
            if tp == "event":
                data.append(e)
                continue
            a_id = e['affiliate']['id']
            url = f"https://www.eventim-light.com/es/a/{a_id}/s/{_id_}/index.pageContext.json"
            series.add(url)
        for evs in self.__getter_events.get(*series).values():
            for e in evs:
                tp = e.get("type")
                if tp not in ("event", None):
                    raise ValueError(tp)
                data.append(e)
        data = parse_obj(
            data,
            compact=True,
            re_parse=_re_parse
        )
        return data

    def __get_info(self):
        url = f"https://www.eventim-light.com/es/a/{self.__id}/index.pageContext.json"
        return self.__get_data(url)

    def get_info(self):
        items: dict[str, Item] = {}
        for e in self.__get_info():
            st_dates = set(e.get("eventDates", []))
            if len(st_dates) == 0:
                st_dates.add(e['start'])
            dates = tuple(d for d in st_dates if d >= NOW)
            if len(dates) == 0:
                continue

            price = 0
            if 'minPrice' in e:
                price = e['minPrice']['value']

            description = MD.convert(e.get("description")) or MD.convert(e.get("teaser"))
            _id_ = e['id']
            eventType = e['eventType']
            i = Item(
                url=f"https://www.eventim-light.com/es/a/{self.__id}/{eventType[0]}/{_id_}/",
                id=_id_,
                title=trim(e['title']),
                category=trim(e['category']),
                header=trim(e.get('header')),
                image=e.get('image'),
                status=e['status'],
                soldout=e['soldout'],
                eventType=eventType,
                description=description,
                dates=dates,
                price=price,
                place=Place(
                    name=trim(e['venue']['name']),
                    adders=trim(e['venue']['street'])+f" {e['venue']['zipCode']}"
                ),
                seriesId=e.get('seriesId'),
                affiliateId=e['affiliate']['id'],

            )
            items[i.id] = i
        return tuple(sorted(items.values()))


class Place(NamedTuple):
    name: str
    adders: str


class Item(NamedTuple):
    url: str
    id: str
    seriesId: str | None
    affiliateId: str
    title: str
    category: str
    header: str
    image: str
    status: str
    soldout: bool
    eventType: str
    description: str
    price: int
    place: Place
    dates: tuple[str, ...]


if __name__ == "__main__":
    e = Eventim(
        "67349f8ab667c57a7581e251",
    )
    e.get_info()

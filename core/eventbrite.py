from core.fetcher import Getter
from aiohttp import ClientResponse
from core.web import buildSoup, get_domain, get_text
from core.util import clean_url, parse_obj, get_main_value
from typing import NamedTuple
import json
from core.cache import HashCache
from core.event import Event
import logging

logger = logging.getLogger(__name__)


class Info(NamedTuple):
    id: int
    url: str
    img: str
    name: str
    description: str
    price: float | None
    full: bool


def _re_parse(obj):
    if not isinstance(obj, dict):
        return obj
    for k in ("lowPrice", "highPrice"):
        v = obj.get(k)
        if isinstance(v, str):
            f = float(v)
            i = int(f)
            obj[k] = i if f == i else f
    return obj


async def rq_to_dict(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    for txt in map(
        get_text,
        soup.select("script[type='application/ld+json']")
    ):
        if txt is None:
            continue
        js = json.loads(txt)
        if not isinstance(js, dict):
            continue
        tp = js.get("@type")
        if not isinstance(tp, str):
            continue
        if "event" not in tp.lower():
            continue
        js = parse_obj(
            js,
            compact=True,
            re_parse=_re_parse
        )
        return js


class Api:
    def __init__(self):
        self.__get_info = Getter(
            onread=rq_to_dict,
            raise_for_status=False,
        )
        self.__cache: dict[int, Info] = dict()

    @HashCache(r"rec/eventbrite/{}.json")
    def __get(self, *ids: int):
        urls = tuple(f"https://eventbrite.es/e/{id_}" for id_ in ids)
        return self.__get_info.get(*urls)

    def get(self, *ids: int):
        info: set[Info] = set()
        ok_ids: set[int] = set(ids)
        for i in tuple(ok_ids):
            nf = self.__cache.get(i)
            if nf:
                info.add(nf)
                ok_ids.remove(i)
        for url, o in self.__get(*ok_ids).items():
            if o is None:
                continue
            offers = self.__find_offers(o)
            i = Info(
                id=int(url.rsplit("/")[-1]),
                url=o['url'],
                name=o["name"],
                description=o["description"],
                img=o.get('image'),
                full=(len(offers) == 0),
                price=self.__find_price(offers),
            )
            self.__cache[i.id] = i
            info.add(i)
        return tuple(sorted(info))

    def __find_offers(self, obj: dict):
        offers: list[dict] = []
        for o in obj['offers']:
            if o['availability'] != "SoldOut":
                offers.append(o)
        return tuple(offers)

    def __find_price(self, offers: tuple[dict, ...]):
        price = None
        for o in offers:
            p = o.get("highPrice")
            if isinstance(p, (int, float)):
                price = max(p, price or 0)
        return price

    @staticmethod
    def get_id(url: str):
        url = clean_url(url)
        if get_domain(url) == "eventbrite.es":
            return int(url.rsplit("/")[-1])

    def fix_events(self, events: tuple[Event]):
        ids: set[int] = set()

        for e in events:
            for _id_ in map(Api.get_id, e.iter_urls()):
                if _id_ is not None:
                    ids.add(_id_)

        info = {i.id: i for i in self.get(*ids)}
        if len(info) == 0:
            return events

        evs: set[Event] = set()
        for e in events:
            price = 0
            imgs: list[str] = []
            full: set[int] = set()
            for _id_ in map(Api.get_id, e.iter_urls()):
                i = info.get(_id_)
                if i is None:
                    continue
                if i.price:
                    price = max(price, i.price)
                if i.img:
                    imgs.append(i.img)
                if i.full:
                    full.add(i.id)

            tp_ss = tuple(
                s for s in e.sessions if Api.get_id(s.url) not in full
            )
            e = e.merge(
                price=max(price, e.price),
                img=e.img or get_main_value(imgs),
                sessions=tp_ss
            )
            evs.add(e)

        return tuple(sorted(evs))


if __name__ == "__main__":
    import sys
    api = Api()
    print(api.get(*map(int, sys.argv[1:])))
from core.fetcher import Getter
from aiohttp import ClientResponse
from core.web import buildSoup, get_domain, get_text
from core.util import clean_url
from typing import NamedTuple
import json
from core.cache import HashCache
from core.event import Event, Session
import logging

logger = logging.getLogger(__name__)


class Info(NamedTuple):
    id: int
    url: str
    image: str
    name: str
    description: str
    price: float | None
    full: bool


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
        if "event" in tp.lower():
            return js


class Api:
    def __init__(self):
        self.__get_info = Getter(
            onread=rq_to_dict,
        )

    def _get(self, *urls: str):
        _urls: set[str] = set()
        for url in urls:
            url = clean_url(url)
            if get_domain(url) == "eventbrite.es":
                _urls.add(url)
        return self.__get_info.get(*_urls)

    @HashCache(r"rec/eventbrite/{}.json")
    def __get(self, *ids: str):
        urls = tuple(f"https://eventbrite.es/e/{id_}" for id_ in ids)
        return self._get(*urls)

    def get(self, *ids: str):
        info: set[Info] = set()
        for url, o in self.__get(*ids).items():
            if o is None:
                continue
            offers = self.__find_offers(o)

            info.add(Info(
                id=int(url.rsplit("/")[-1]),
                url=o['url'],
                name=o["name"],
                description=o["description"],
                img=o['image'],
                full=(len(offers) == 0),
                price=self.__find_price(offers),
            ))
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
            p = float(o.get("highPrice") or "0")
            price = max(p, price or 0)
        return price

    def fix_sessions(self, events: tuple[Event]):

        def _iter_session(evs: tuple[Event]):
            for e in evs:
                for s in e.sessions:
                    url = clean_url(s.url)
                    if get_domain(url) == "eventbrite.es":
                        _id_ = int(url.rsplit("/")[-1])
                        yield _id_, s

        ids = set(x[0] for x in _iter_session(events))
        full = set(i.id for i in self.get(*ids) if i.full)
        if len(full) == 0:
            return events

        ban_session: set[str] = set()
        for _id_, s in _iter_session(events):
            if _id_ in full:
                logger.debug(f"FULL session sold out {s.url}")
            ban_session.add(s.url)

        evs: set[Event] = set()
        for e in events:
            ss: list[Session] = []
            for s in e.sessions:
                if s.url not in ban_session:
                    ss.append(s)
            if tuple(ss) != e.sessions:
                e = e.merge(sessions=tuple(ss))
            evs.add(e)

        return tuple(sorted(evs))


if __name__ == "__main__":
    import sys
    api = Api()
    print(api.get(*sys.argv[1:]))
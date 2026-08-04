from portal.base import Base
from core.util import to_uuid, re_or
from core.event import Event, Category, Session, Cinema
from core.place import Places
from core.ics import IcsEventWrapper, IcsReader
from core.web import buildSoup
import re
import logging
from core.giglon import GIGLON, Item
from core.fetcher import Getter
from aiohttp import ClientResponse
from collections import defaultdict


logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")
re_tail = r"\s+\((\d{4})\)\s*(?:\s*(?:Doblada al español|[vose\.]+|venta de entradas))*\s*$"
re_name = re.compile(r"^Sala \d+[\.:\s]*|"+re_tail, flags=re.I)
re_year = re.compile(re_tail, flags=re.I)


def _find_year(s: str):
    m = re_year.search(s)
    if m:
        return int(m.group(1))


async def rq_shop(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    urls: set[str] = set()
    for a in soup.select('div.tribe_events a[href^="https://www.giglon"]'):
        urls.add(a.attrs["href"])
    return tuple(sorted(urls))


class ArtisticMetropol(Base):
    def __init__(
        self,
        cache: str | bool = True
    ):
        super().__init__(cache=cache)
        self.__ics = IcsReader(
            "https://artisticmetropol.es/calendario-de-sesiones/lista/?ical=1",
            "https://artisticmetropol.es/calendario-de-sesiones/lista/p%c3%a1gina/2/?ical=1",
            "https://artisticmetropol.es/calendario-de-sesiones/lista/p%c3%a1gina/3/?ical=1",
            "https://artisticmetropol.es/calendario-de-sesiones/lista/p%c3%a1gina/4/?ical=1",
            "https://artisticmetropol.es/calendario-de-sesiones/lista/p%c3%a1gina/5/?ical=1",
        )
        self.__get_shop = Getter(
            onread=rq_shop,
            max_concurrency=10,
            timeout=60,
            raise_for_status=False,
        )

    def _get_events(self):
        ics = self._get_ics_events()
        urls_event: dict[str, set[Cinema]] = defaultdict(set)
        shop_event: dict[str, set[Cinema]] = defaultdict(set)
        for i in ics:
            if i.url:
                urls_event[i.url].add(i)
        for u, shops in self.__get_shop.get(*urls_event.keys()).items():
            for i in urls_event[u]:
                for s in shops:
                    shop_event[s].add(i)
        gg: set[Cinema] = set()
        for e in self._get_gg_events():
            ok_ics: set[str] = set()
            for i in shop_event.get(e.url, set()):
                if i.sessions == e.sessions:
                    ics.discard(i)
                    ok_ics.add(i.url)
            ok_ics.discard(None)

            if len(ok_ics) == 1 and (e.url is None or (len(e.sessions) == 1 and e.sessions[0].url is None)):
                ics_url = ok_ics.pop()
                if e.url is None:
                    e = e.merge(url=ics_url)
                else:
                    e = e.merge(
                        url=ics_url,
                        sessions=(e.sessions[0]._replace(url=e.url),)
                    )
            else:
                e = e.merge(also_in=tuple(sorted(ok_ics)))
            gg.add(e)
        ok_events = gg.union(ics)
        ok_events = set(Event.fusionIfSimilar(
            ok_events,
            ('name', 'price')
        ))

        rt = tuple(sorted(e.merge(
            id=f"armo{e.id}",
            price=e.price+0.33
        ) for e in ok_events))
        return rt

    def _get_ics_events(self):
        ok_events: set[Cinema] = set()
        done: set[str] = set()
        for e in self.__ics.events:
            if e.UID in done:
                continue
            done.add(e.UID)
            event = self.__to_ics_event(e)
            if event:
                ok_events.add(event)
        return ok_events

    def __to_ics_event(self, e: IcsEventWrapper):
        if e.DTSTART is None or re_or(
            e.SUMMARY,
            "Pase PRIVADO",
            flags=re.I
        ):
            return None
        name = re_name.sub("", e.SUMMARY)
        event = Cinema(
            id=e.UID,
            url=e.URL,
            name=name,
            duration=e.duration or 60,
            img=e.ATTACH,
            price=self.__find_price(e),
            category=Category.CINEMA,
            place=Places.ARTISTIC_METROPOL.value,
            year=_find_year(e.SUMMARY),
            sessions=(
                Session(
                    date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                ),
            ),
        )
        return event

    def __find_price(self, e: IcsEventWrapper):
        if e.DTSTART.weekday() == 2:
            return 4.50
        return 7.50

    def _get_gg_events(self):
        ok_events: set[Cinema] = set()
        for e in GIGLON.get_items("artistic-metropol"):
            for x in self.__to_gg_event(e):
                ok_events.add(x)
        return ok_events

    def __to_gg_event(self, e: Item):
        name = re_name.sub("", e.name)
        event = Cinema(
            id=to_uuid(e.url),
            url=e.url,
            name=name,
            price=e.price,
            duration=60,
            img=e.img,
            category=Category.CINEMA,
            place=Places.ARTISTIC_METROPOL.value,
            year=_find_year(e.name),
            sessions=(),
        )
        for dt in e.dates:
            prc = _min(*(s.price for s in dt.seats)) or e.price
            if prc is None:
                logger.warning(f"NO PRICE {e.url}")
                continue
            ev = event.merge(
                price=prc,
                sessions=(Session(
                    date=dt.dt.strftime("%Y-%m-%d %H:%M")
                ),)
            )
            yield ev


def _min(*args):
    x = None
    for a in args:
        if a is None:
            continue
        if x is None or x > a:
            x = a
    return x


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/artisticmetropol.log", log_level=logging.INFO)
    m = ArtisticMetropol()
    e = m.get_events()
    print(*e, sep="\n")

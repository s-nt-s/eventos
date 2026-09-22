from core.web import Web, get_text, get_attr, buildSoup
from core.util import find_euros
import re
from core.fetcher import Getter
from aiohttp import ClientResponse
from datetime import datetime
from collections import defaultdict
from types import MappingProxyType
from bs4 import Tag

from typing import NamedTuple, Optional
import logging

logger = logging.getLogger(__name__)
re_event = re.compile(r"^https://www\.giglon\.com/evento/([^/]+)/?$")
re_date = re.compile(r"^\s*(\d{2})/(\d{2})/(\d{4}) (\d{2}):(\d{2}).*$")


def _clean_dict(x: dict):
    for k, v in list(x.items()):
        if v is None:
            del x[k]
    return x



class Seat(NamedTuple):
    name: str
    available: int
    price: Optional[float | int] = None


class Dt(NamedTuple):
    dt: datetime
    url: str
    seats: tuple[Seat, ...] = tuple()


class Item(NamedTuple):
    name: str
    url: str
    img: Optional[str] = None
    price: Optional[float | int] = None
    dates: tuple[Dt, ...] = tuple()


async def rq_to_seats(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    seats: dict[Seat, int] = {}

    value_inputs = soup.find_all('input', id=re.compile(r'^values\d+$'))

    for inp in value_inputs:
        seat_id = inp.get('data-seat')
        if not seat_id:
            continue

        # Buscar el tooltip asociado
        msg = get_text(soup.find('div', id=f'hoverSeat{seat_id}'))
        if msg is None:
            continue
        # Extraer precio si existe
        price = find_euros(msg)
        if price is None:
            logger.warning(f"PRICE NOT FOUND in {r.url}")

        # Determinar el nombre del asiento (según el tipo)
        if msg.startswith('PMR'):
            name = "PMR"
        else:
            name = "NORMAL"

        s = Seat(name=name, available=1, price=price)
        seats[s] = seats.get(s, 0) + 1

    aux: set[Seat] = set()
    for s, a in seats.items():
        aux.add(s._replace(available=a))
    return tuple(sorted(aux))


async def rq_to_dates(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    option_url = _find_url_ajax(
        soup,
        r"https?://www\.giglon\.com/todos[^\"']+?zoneSale\.jsp",
        str(r.url)
    )
    slc = soup.select_one("#dateEvent")
    for k in ("zoneId", ):
        v = get_attr(soup.select_one("#"+k), "value")
        if v:
            option_url = f"{option_url}&{k}={v}"

    dts: set[Dt] = set()
    for o in slc.select("option[value]"):
        txt = get_text(o)
        v = o.attrs["value"]
        mt = re_date.match(txt)
        if mt is None:
            logger.critical(f"BAD DATE ({txt}) in {r.url}")
            continue
        d, m, y, h, mm = map(int, mt.groups())
        dt = datetime(y, m, d, h, mm)
        d = Dt(
            dt=dt,
            url=f"{option_url}&eventDateId={v}",
        )
        dts.add(d)
    if len(dts) == 0:
        logger.warning(f"0 dates in {r.url}")
    return tuple(sorted(dts))


def _find_url_ajax(soup: Tag, re_url: str, source_url: str):
    urls: set[str] = set()
    for txt in map(get_text, soup.select("script")):
        if txt is None:
            continue
        for url in re.findall(r'"('+re_url+r')"', txt):
            urls.add(url)
    if len(urls) == 1:
        return urls.pop()
    if len(urls) == 0:
        logger.critical(f"{re_url} NOT FOUND in {source_url}")
    else:
        logger.critical(f"{re_url} AMBIGUOUS in {source_url} : " + " ".join(sorted(urls)))


def _parse_url(url: str):
    m = re_event.search(url)
    if m:
        id_e = m.group(1)
        return f"https://www.giglon.com/todos?idEvent={id_e}"
    return url


class Giglon:
    def __init__(self):
        self.__w = Web()
        self.__get_dates = Getter(
            onread=rq_to_dates,
            max_concurrency=10,
            timeout=60,
            raise_for_status=False,
        )
        self.__get_dates_seats = Getter(
            onread=rq_to_seats,
            max_concurrency=10,
            timeout=60,
            raise_for_status=False,
        )

    def __iter_items(self, id_sala: str):
        home = f"https://www.giglon.com/salas?idSala={id_sala}"
        url = _find_url_ajax(
            self.__w.get(home),
            r"https?://www\.giglon\.com/salas[^\"']+?mosaicAux\.jsp",
            home
        )
        if url is None:
            return
        page = 0
        while True:
            page = page + 1
            page_url = url+f"&page={page}"
            soup = self.__w.get(page_url)
            divs = tuple(soup.select("div.main_box_container > div.box_container2"))
            if len(divs) == 0:
                break
            for d in divs:
                yield d
            if not soup.select_one(f"#nextList{page+1}"):
                break

    def get_dates(self, *urls: str):
        dt_urls: dict[str, set[str]] = defaultdict(set)
        tail = "directPurchase=true"
        for u in urls:
            new_url = _parse_url(u)
            if tail not in new_url:
                new_url = f"{new_url}&{tail}" if "?" in new_url else f"{new_url}?{tail}"
            dt_urls[new_url].add(u)
        url_dates: dict[str, tuple[Dt, ...]] = _clean_dict(
            self.__get_dates.get(*set(dt_urls.keys()))
        )
        url_seats: set[str] = set()
        for k, v in list(url_dates.items()):
            for x in v:
                url_seats.add(x.url)
        data_seats: dict[str, tuple[Seat, ...]] = _clean_dict(
            self.__get_dates_seats.get(*url_seats)
        )

        data: dict[str, tuple[Dt, ...]] = {}
        for k, v in url_dates.items():
            for u in dt_urls[k]:
                data[u] = tuple((x._replace(
                    seats=data_seats.get(x.url) or tuple()
                ) for x in v))

        return MappingProxyType(data)

    def get_items(self, id_sala: str):
        aux: set[Item] = set()
        for d in self.__iter_items(id_sala):
            a = d.select_one("a")
            img = get_attr(d.select_one("img"), "src")
            if img:
                img = re.sub(r"(\.[a-z]+)/[a-z0-9\-]+\?imageThumbnail=\d+$", r"\1", img)
            i = Item(
                img=img,
                name=a.attrs["title"],
                url=_parse_url(get_attr(a, "href")),
                price=find_euros(get_text(a.select_one("span.price-ev")))
            )
            aux.add(i)
        items: set[Item] = set()
        dates = self.get_dates(*(i.url for i in aux))
        for i in aux:
            dts = dates.get(i.url) or tuple()
            i = i._replace(
                dates=dts
            )
            items.add(i)
        return tuple(sorted(items, key=lambda x: (x.dates, x)))


GIGLON = Giglon()


if __name__ == "__main__":
    for d in GIGLON.get_items("artistic-metropol"):
        continue
        print(d)

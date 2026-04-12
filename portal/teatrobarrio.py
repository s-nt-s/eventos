from core.web import Web, get_text, buildSoup, Tag
from core.cache import TupleCache
from typing import NamedTuple, Optional
from core.util import get_obj, find_euros, to_uuid, re_or, get_query
import logging
from core.fetcher import Getter
from aiohttp import ClientResponse
import re
from datetime import datetime
from core.event import Event, Session, Category, CategoryUnknown
from core.place import Places
from urllib.parse import urlencode
from core.md import MD
import pytz


logger = logging.getLogger(__name__)

MONTHS = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")
NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))
RE_WAIT = re.compile(r"Está en la cola virtual.*eres es el (\d+)º en la cola.*llegará el turno en ([\d:]+ (?:minutos?|horas?))")


async def rq_to_shop_sessions(r: ClientResponse):
    sessions: set[ItemSession] = set()
    root = str(r.url)
    soup = buildSoup(root, await r.text())
    divs = soup.select("#performances div.performance")
    if len(divs) == 0:
        return tuple()
    frm = divs[0].find_parent("form")
    if frm is None:
        raise ValueError(f"frm={frm} {root}")
    prd = frm.select_one("#prod_id")
    if prd is None:
        raise ValueError(f"prd={prd} {root}")
    for div in soup.select("#performances div.performance"):
        npt = div.select_one("input[name*='perf_id']")
        txt = get_text(div)
        if None in (txt, npt):
            raise ValueError(f"txt={txt} npt={npt} {root}")
        if re.search(r"Localidades agotadas", txt):
            continue
        m = re.search(r"(\d{1,2}):(\d{1,2})\D+(\d+)\D+?\b(" + "|".join(MONTHS) + r")\D", txt)
        if m is None:
            raise ValueError(f"{txt} {root}")
        dt = datetime(NOW.year, MONTHS.index(m.group(4))+1, int(m.group(3)), int(m.group(1)), int(m.group(2)))
        if dt.month < NOW.month:
            dt = dt.replace(year=NOW.year+1)

        sessions.add(ItemSession(
            date=dt.strftime("%Y-%m-%d %H:%M"),
            shop=frm.attrs["action"] + "?" + prd.attrs["name"] + "=" + prd.attrs["value"] + "&" + npt.attrs["name"] + "=" + npt.attrs["value"]
        ))
    return tuple(sorted(sessions))


async def rq_to_seats(r: ClientResponse):
    seats: set[str] = set()
    root = str(r.url)
    if root.startswith("https://es.patronbase.com/_TeatroDelBarrio/Seats/NumSeats?"):
        return (root, )
    soup = buildSoup(root, await r.text())
    inpt = soup.select("input[name='seat_type_id']")
    if len(inpt) == 0:
        return None
    frm = inpt[0].find_parent("form")
    if frm is None:
        raise ValueError(f"NOT FOUND FORM {root}")
    params = get_query(root, strip=True)
    target = "https://es.patronbase.com/_TeatroDelBarrio/Seats/ChooseMyOwn?"
    for i in inpt:
        name = get_text(i.find_parent("label"))
        if "disabled" in i.attrs:
            continue
        val = i.attrs['value']
        if val == "SR" or re.search(r"silla de ruedas", name or '', flags=re.I):
            continue
        prms = dict(params)
        prms[i.attrs['name']] = i.attrs['value']
        section_id = i.find_parent("div", class_="pb-section")
        if section_id:
            prms['section_id'] = section_id.attrs["section_id"]
        query = {
            k: prms[k]
            for k in
            ("prod_id", "perf_id", "section_id", "seat_type_id",)
        }
        seat_url = target + urlencode(query)
        seats.add(seat_url)
    return tuple(sorted(seats))


async def rq_to_page(r: ClientResponse):
    root = str(r.url)
    soup = buildSoup(root, await r.text())
    duration = None
    for txt in map(get_text, soup.select("div.meta-container div.meta")):
        m = re.match(r"^(\d+)\s+minutos?$", txt or "")
        if m:
            duration = max(duration or 0, int(m.group(1)))
    img = soup.select_one("meta[property='og:image'][content]")
    return Page(
        duration=duration,
        url=root,
        img=img.attrs['content'] if img else None
    )


class Page(NamedTuple):
    url: str
    duration: int
    img: str


class ItemSession(NamedTuple):
    date: str
    shop: str


class Item(NamedTuple):
    name: str
    img: str
    shop: str
    url: str
    category: str
    summary: str
    price: float
    sessions: tuple[ItemSession, ...]
    duration: Optional[int] = None

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
        ss = obj.get("sessions")
        if ss:
            obj["sessions"] = tuple(ItemSession(**s) for s in ss)
        return Item(**obj)


class TeatroBarrio:
    AGENDA = "https://teatrodelbarrio.com/programacion/"
    SHOP = "https://es.patronbase.com/_TeatroDelBarrio/Productions"

    def __init__(
        self,
        max_price: float = None
    ):
        self.__w = Web()
        self.__max_price = max_price
        self.__get_page = Getter(
            onread=rq_to_page
        )
        self.__get_shop_sessions = Getter(
            onread=rq_to_shop_sessions
        )
        self.__get_shop_seats = Getter(
            onread=rq_to_seats
        )

    @TupleCache("rec/teatrobarrio/items.json", builder=Item.build)
    def get_items(self):
        items = self.__get_items_from_shop()
        if items is None:
            return None
        return tuple(sorted(items))

    def __get_items_from_shop(self):
        items: set[Item] = set()
        search_sesions: set[str] = set()
        soup = self.__w.get(TeatroBarrio.SHOP)
        divs = soup.select("#pb_productions div.pb_production")
        if len(divs) == 0:
            txt = tuple(map(get_text, soup.select("#pb_page_title, #pb_content div.pb_queue_container")))
            if any(txt):
                msg = ", ".join(t for t in txt if t is not None)
                m = RE_WAIT.search(msg)
                if m:
                    msg = f"Cola de espera: eres el {m.group(1)}º, quedan {m.group(2)}"
                logger.warning(msg)
            return None
        for div in divs:
            a = div.select_one("div.pb_event_title a")
            shop = a.attrs["href"]
            txt_price = get_text(div.select_one("span.pb_pricing"))
            price = find_euros(txt_price)
            if price is None:
                logger.warning(f"NOT FOUND price txt_price={txt_price} {shop}")
            elif self.__max_price is not None and price > self.__max_price:
                logger.debug(f"Descartado por price={price} {shop}")
                continue
            d = self.__get_date_from_shop(div)
            ss = None
            if d:
                ss = (
                    ItemSession(date=d, shop=shop),
                )
            else:
                search_sesions.add(shop)
            img = div.select_one("span.pb_event_icon img")
            i = Item(
                name=get_text(a),
                img=img.attrs["src"] if img else None,
                shop=shop,
                url=div.select_one("a.pb_more_info").attrs["href"],
                category=get_text(div.select_one("span.pb_category_name")).lower(),
                price=price,
                summary=MD.convert(div.select_one("p.pb_event_summary")),
                sessions=ss
            )
            items.add(i)
        url_sessions: dict[str, tuple[ItemSession, ...]] = self.__get_shop_sessions.get(*search_sesions)
        for i in list(items):
            if i.sessions is not None:
                continue
            items.remove(i)
            ss = url_sessions.get(i.shop)
            if ss is None:
                logger.warning(f"NOT FOUND sessions=None {i.shop}")
                continue
            items.add(i._replace(sessions=ss))
        url_shops: set[str] = set()
        for i in items:
            for s in i.sessions:
                url_shops.add(s.shop)
        url_seats: dict[str, tuple[ItemSession, ...]] = self.__get_shop_seats.get(*url_shops)
        url_page: set[str] = set()
        for i in list(items):
            ss = set(i.sessions)
            for s in i.sessions:
                seats = url_seats.get(s.shop)
                if seats is None:
                    continue
                size = len(seats)
                if size < 2:
                    ss.remove(s)
                    if size == 1:
                        s = s._replace(shop=seats[0])
                        ss.add(s)
            items.remove(i)
            if len(ss):
                i = i._replace(sessions=tuple(sorted(ss)))
                items.add(i)
                url_page.add(i.url)
        url_page: dict[str, Page] = self.__get_page.get(*url_page)
        for i in list(items):
            page = url_page.get(i.url)
            if page is None:
                continue
            items.remove(i)
            items.add(i._replace(
                url=page.url or i.url,
                duration=page.duration,
                img=page.img or i.img,
            ))
        return items

    def __get_date_from_shop(self, div: Tag):
        txt = get_text(div.select_one("div.pb_event_date"))
        if txt is None:
            return None
        m = re.search(r"(\d+)\D+?\b(" + "|".join(MONTHS) + r")\b.*?(\d{4})\D+(\d{1,2}):(\d{1,2})", txt)
        if m is None:
            return None
        dt = datetime(int(m.group(3)), MONTHS.index(m.group(2))+1, int(m.group(1)), int(m.group(4)), int(m.group(5)))
        return dt.strftime("%Y-%m-%d %H:%M")

    def __get_items_from_agenda(self):
        items: set[Item] = set()
        soup = self.__w.get(TeatroBarrio.AGENDA)
        for div in soup.select("div.asy-grid > div[id]"):
            h2 = div.select_one("h2")
            shop = div.select_one("a:has(.btn-buytickets)")
            i = Item(
                id=int(div.attrs["id"].split("-")[-1]),
                name=get_text(h2),
                url=h2.find_parent("a").attrs["href"],
                shop=shop.attrs["href"]
            )
            items.add(i)
        return items

    @property
    @TupleCache("rec/teatrobarrio.json", builder=Event.build)
    def events(self):
        logger.info("Teatro del barrio: Buscando eventos")
        events: set[Event] = set()
        for i in self.get_items():
            e = self.__item_to_event(i)
            if e:
                events.add(e)
        logger.info(f"Teatro del barrio: Buscando eventos = {len(events)}")
        return tuple(sorted(events))

    def __item_to_event(self, i: Item):
        return Event(
            id="tb"+to_uuid(i.url),
            url=i.url,
            name=i.name,
            price=i.price,
            category=self.__find_category(i),
            place=Places.TEATRO_BARRIO.value,
            duration=i.duration or 60,
            img=i.img,
            sessions=tuple(Session(date=s.date, url=s.shop) for s in i.sessions),
        )

    def __find_category(self, i: Item):
        if re_or(
            i.category,
            "infantil"
        ):
            return Category.CHILDISH
        if re_or(
            i.category,
            "baile"
        ):
            return Category.DANCE
        if re_or(
            i.name,
            "AMEIS"
        ):
            return Category.NO_EVENT
        if re_or(
            i.summary,
            ("acto", "para (cuestionar|pensar)"),
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.summary,
            "club de lectura",
            flags=re.I
        ):
            return Category.READING_CLUB

        logger.critical(str(CategoryUnknown(i.url, f"{i.name} {i.category} {i.summary}")))
        return Category.UNKNOWN
        


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/teatrobarrio.log", log_level=logging.INFO)
    T = TeatroBarrio(max_price=10)
    T.events
from requests import Session as ReqSession
from core.cache import Cache, TupleCache
from urllib.parse import urlencode
from core.util import parse_obj, find_euros, re_or
import re
from core.event import Event, Category, CategoryUnknown, Session, Place, find_book_category
from core.place import Places
from functools import cached_property
import logging
from datetime import datetime
from core.fetcher import Getter
from aiohttp import ClientResponse
from core.web import buildSoup, get_text, Tag
from typing import NamedTuple, Optional
from unidecode import unidecode as ori_unidecode
from core.md import MD

logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")
re_min = re.compile(r"(\d+)\s*(?:min|minutos?)\b")


def unidecode(s: str):
    if s is None:
        return None
    fake_n = "%%---###"
    s = re.sub(r"ñ", fake_n, s.lower())
    s = ori_unidecode(s)
    s = re.sub(fake_n, "ñ", s)
    s = re_sp.sub(" ", s).strip()
    if len(s) == 0:
        return None
    return s


class InfoSoup(NamedTuple):
    img: Optional[str] = None
    duration: Optional[int] = None
    description: Optional[str] = None
    status_code: Optional[int] = None


async def rq_to_info(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    img = soup.select_one("div.container picture img.img-fluid")
    if img:
        img = img.attrs['src']
    duration = _find_duration(soup)
    desc = MD.convert(soup.select_one("div.event-calendar-infotext-container"))
    return InfoSoup(
        status_code=r.status,
        img=img,
        duration=duration,
        description=desc
    )


def _find_duration(soup: Tag):
    for p in map(get_text, soup.select("div.event-calendar-date p")):
        m = re.search(r"(\d+):(\d+)\s*[-\+]\s*(\d+):(\d+)", p or "")
        if m:
            h1, m1, h2, m2 = map(int, m.groups())
            return ((h2*60)+m2)-((h1*60)+m1)

    duration = None
    for li in map(get_text, soup.select("ul.event-calendar-fact-list li")):
        for d in map(int, re_min.findall(li or "")):
            duration = (duration or 0) + d
    if duration is not None:
        return duration


def _clean_name(name: str):
    name = re.sub(r"^CINE CLUB GOETHE[\s\|]*", "", name, flags=re.I)
    return name


def _re_parse(obj):
    if not isinstance(obj, dict):
        return obj
    for k in (
        "date_start_Date",
        "timezone_gmt",
        "category_id"
    ):
        v = obj.get(k)
        if isinstance(v, str):
            obj[k] = int(v)
    for k in (
        "time_start_txt",
        "time_end_txt",

    ):
        v = obj.get(k)
        if isinstance(v, str):
            obj[k] = re.sub(r"\s+(h|&#104;)$", "", v)
    for k, v in {
        "country_IDtxt": "España",
        "event_city": "Madrid",
        "is_online": 0
    }.items():
        val = obj.get(k)
        if val not in (None, v):
            raise ValueError(f"¿{k}={v}?")
        if val == v:
            del obj[k]
    return obj


def _to_date(f: str, h: str):
    if f is None:
        return None
    if h is None:
        h = "00:00"
    return datetime(*map(int, re.findall(r"\d+", f"{f} {h}")))


class Goethe:
    SEARCH = "https://www.goethe.de/rest/objeventcalendarRedesign/events/fetchEvents"

    def __init__(self, max_price: int = None):
        self.__s = ReqSession()
        self.__max_price = max_price

    def __search(self, params: dict, filterData: dict):
        params = urlencode({
            "langId": 4,
            "viewMode": -1,
            "configData": params,
            "filterData": filterData
        })
        r = self.__s.get(Goethe.SEARCH+"?"+params)
        return r.json()

    @Cache("rec/goethe/items.json")
    def get_items(self):
        obj = self.__search(
            {
                "category_ID": "", #, "178926_178927_178937_178936_178935_178934_178933_178932_178931_178930_178929_178928_178938",
                "elementsperpage": 100,
                "frontendfilter": "adress_IDtxt,category_IDtxt,date_range",
                "headline": "Calendario",
                "outputtype": "standardkalender",
                "institute_ID": 375,
                "week_day_start": 1,
                "timezone": 29
            },
            {
                "start": 0,
                "excluded_objectIds": [], #27189962, 27195189, 27229838],
                "count_records_per_filter": True,
                "mapped_data": True,
                "adress_IDtxt": ["Madrid"]
            }
        )
        obj = parse_obj(
            obj['eventItems'],
            compact=True,
            re_parse=_re_parse
        )
        return obj

    @cached_property
    @TupleCache("rec/goethe.json", builder=Event.build)
    def events(self):
        logger.info("Goethe: Buscando eventos")
        evs: set[Event] = set()
        for i in self.get_items():
            _id_ = i['object_id']
            url = f"https://www.goethe.de/ins/es/es/sta/mad/ver.cfm?event_id={_id_}"
            name = i['headline']
            if re_or(
                name,
                "entradas agotadas",
                flags=re.I
            ):
                continue
            lang = unidecode(i.get('language'))
            if lang and not re.search(r"\bespañol\b", lang):
                logger.warning(f"Descartado por lang={lang} {url}")
                continue
            place = self.__find_place(url, i)
            if place is None:
                logger.critical(f"Descartado por place=None {url}")
                continue
            price = self.__find_price(url, i)
            if self.__max_price is not None and self.__max_price < price:
                logger.debug(f"Descartado por price={price} {url}")
                continue
            sessions, duration = self.__find_session_duration(url, i)
            e = Event(
                id=f"gt{_id_}",
                url=url,
                name=_clean_name(name),
                img=None,
                price=self.__find_price(url, i),
                category=self.__find_category(url, i),
                place=place,
                duration=duration,
                sessions=sessions,
                cycle=None
            )
            evs.add(e)
        url_info: dict[str, InfoSoup] = Getter(
            onread=rq_to_info,
            raise_for_status=False
        ).get(*(e.url for e in evs))
        for e in list(evs):
            evs.remove(e)
            i = url_info.get(e.url)
            if i is None or i.status_code == 404:
                logger.critical(f"KO url {e.url}")
                continue
            e = e.merge(
                img=i.img,
                duration=max(i.duration or 0, e.duration or 0),
                category=self.__improve_category(i, e)
            )
            if (i.duration, e.duration) == (None, None):
                logger.warning(f"NOT FOUND duration {e.url}")
            evs.add(e)
        logger.info(f"Goethe: Buscando eventos = {len(evs)}")
        return tuple(sorted(evs))

    def __improve_category(self, i: InfoSoup, e: Event):
        if e.category in (
            Category.LITERATURE,
            Category.READING_CLUB
        ):
            return find_book_category(e.name, i.description, e.category)
        #if e.category == Category.UNKNOWN:
        #    logger.critical(str(CategoryUnknown(e.url, None)))
        return e.category

    def __find_session_duration(self, url: str, i: dict):
        duration = None
        a = _to_date(i['date_start_ical'], i.get('time_start_txt'))
        z = _to_date(i.get('date_end_ical'), i.get('time_end_txt'))
        sl = i['subheadline']
        for d in map(int, re_min.findall(sl)):
            duration = (duration or 0) + d
        if duration is None and z:
            duration = (z-a).total_seconds() // 60
        rl = i.get("registration_link_url")
        if rl and not re.match(r"^https?://.*", rl, flags=re.I):
            rl = None
        s = Session(
            date=a.strftime("%Y-%m-%d %H:%M"),
            url=rl
        )
        return (s, ), duration

    def __find_place(self, url: str, i: dict):
        lc = i['location_IDtxt']
        if lc is None:
            return None
        if re_or(
            lc,
            "Goethe-Institut",
            flags=re.I
        ):
            return Places.GOETHE.value
        if re_or(
            lc,
            "Teatro Cuarta Pared",
            flags=re.I
        ):
            return Places.CUARTA_PARED.value
        if re_or(
            lc,
            "Valle-Incl[aá]n",
            flags=re.I
        ):
            return Places.VALLE_INCLAN.value
        if re_or(
            lc,
            "mariqueen",
            flags=re.I
        ):
            return Places.MARIQUEEN.value
        if re_or(
            lc,
            "r[eé]plika",
            flags=re.I
        ):
            return Places.REPLIKA.value
        logger.warning(f"NOT FOUND place {lc} in {url}")
        return Place(
            address=lc,
            name=lc
        )

    def __find_price(self, url: str, i: dict):
        price = i.get("price")
        prc = find_euros(price)
        if prc is not None:
            return prc
        logger.critical(f"NOT FOUND price {price} {url}")
        return 0

    def __find_category(self, url: str, i: dict):
        et = i['event_type']
        if re_or(
            et,
            "Encuentro literario",
            flags=re.I
        ):
            return Category.LITERATURE
        if re_or(
            et,
            "conferencia",
            "presentaci[oó]n",
            "Seminario",
            "Mesa redonda",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            et,
            "club de lectura",
            flags=re.I
        ):
            return Category.READING_CLUB
        if re_or(
            et,
            "formación",
            "Ponencia y taller",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            et,
            "proyecci[oó]n",
            "Pel[ií]cula",
            flags=re.I
        ):
            return Category.CINEMA
        if re_or(
            et,
            "concierto",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            et,
            "teatro",
            "esc[eé]nicas?",
            flags=re.I
        ):
            return Category.THEATER
        logger.warning(str(CategoryUnknown(url, et)))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/goethe.log", log_level=logging.INFO)
    g = Goethe(max_price=10)
    print(len(g.events))

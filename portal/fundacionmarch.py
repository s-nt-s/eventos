from core.web import Web, get_text, buildSoup
from core.event import Event, Place, Category, Session, Cinema, CategoryUnknown
from bs4 import Tag
import logging
from urllib.parse import urlparse, parse_qs, unquote_plus
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import NamedTuple, Optional
from core.util import to_uuid, re_and
from core.fetcher import Getter
from aiohttp import ClientResponse
from core.cache import TupleCache
from functools import cached_property
import re


MADRID_TZ = ZoneInfo("Europe/Madrid")
UTC_TZ = ZoneInfo("UTC")


logger = logging.getLogger(__name__)


async def rq_to_cal(r: ClientResponse):
    cals: set[AnchorCal] = set()
    soup = buildSoup(str(r.url), await r.text())
    div = soup.select_one("div.c-hero-bg-image__fechas-container")
    for a in div.select("a[href^='https://calendar.google.com/']"):
        cal = parse_google_calendar_template(a.attrs.get("href"))
        if cal is not None:
            cals.add(cal)
    cycle: set[str] = set()
    for txt in map(get_text, div.select("a.c-enlace")):
        if txt and txt.startswith("Ciclo "):
            cycle.add(txt[6:].strip())

    return Info(
        cals=tuple(sorted(cals)),
        cycle=cycle.pop() if len(cycle) == 1 else None
    )


class AnchorCal(NamedTuple):
    title: str
    location: str
    start: datetime
    end: datetime


class Info(NamedTuple):
    cals: tuple[AnchorCal, ...] = tuple()
    cycle: Optional[str] = None


def parse_google_calendar_template(url: str):
    if url is None:
        return None

    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    def _get_param(name, default=None):
        if name not in params:
            return default
        return unquote_plus(params[name][0])

    def _to_date(d: str):
        dt = datetime.strptime(d, "%Y%m%dT%H%M%SZ")
        utc = dt.replace(tzinfo=UTC_TZ)
        return utc.astimezone(MADRID_TZ)

    text = _get_param("text")
    location = _get_param("location")
    dates_raw = _get_param("dates")

    start_dt, end_dt = map(_to_date, dates_raw.split("/"))

    return AnchorCal(
        title=text,
        location=location,
        start=start_dt,
        end=end_dt,
    )


def _clean_name(name: str):
    name = re.sub(r"^(Aula de \(Re\)estrenos) \d+.\s+", "", name)
    return name


class FundacionMarch:
    URL = "https://www.march.es/es/madrid"
    CASTELLO = Place(
        name="Fundación Juan March",
        address="Calle de Castelló, 77, Salamanca, 28006 Madrid",
        latlon="40.43129877084539,-3.6812832326177465"
    ).normalize()

    @cached_property
    def __web(self):
        w = Web()
        #w.s = Driver.to_session(
        #    "firefox",
        #    FundacionMarch.URL,
        #    session=w.s,
        #)
        w.s.headers.update({
            'Accept-Encoding': 'gzip, deflate'
        })
        return w

    @TupleCache("rec/fundacionmarch.json", builder=Event.build)
    def get_events(self):
        logger.info("Fundación March: Buscando eventos")
        all_events: set[Event] = set()
        self.__web.get(FundacionMarch.URL)
        for div in self.__web.soup.select("div.snippet"):
            ev = self.__div_to_event(div)
            if ev is not None:
                all_events.add(ev)

        urls_cals: dict[str, Info] = Getter(
            onread=rq_to_cal,
            headers=self.__web.s.headers,
            cookie_jar=self.__web.s.cookies,
        ).get(*(e.url for e in all_events))

        for e in list(all_events):
            s = set(e.sessions)
            info = urls_cals.get(e.url)
            if info is None:
                continue
            all_events.remove(e)
            for c in info.cals:
                s.add(Session(date=c.start.strftime("%Y-%m-%d %H:%M")))
            s = tuple(sorted(s))
            name = str(e.name)
            if info.cycle:
                name = re.sub(
                    r"^"+re.escape(info.cycle)+r".*:\s*",
                    "",
                    name
                )
            e = e.merge(
                name=name,
                sessions=s,
                cycle=info.cycle if e.category != Category.CINEMA else None
            ).fix_type()
            if isinstance(e, Cinema):
                m = re.match(r"^(.+?) \((\d{4})\) de (.+)$", name)
                if m:
                    e = e.merge(
                        name=m.group(1),
                        year=int(m.group(2)),
                        director=(m.group(3),)
                    )
            all_events.add(e)

        evs = Event.fusionIfSimilar(
            all_events,
            ('name', 'place'),
            firstEventUrl=True
        )
        size = len(evs)
        logger.info(f"Fundación March: Buscando eventos = {size}")
        if size == 0:
            logger.warning(str(self.__web.soup))
        return tuple(evs)

    def __div_to_event(self, div: Tag):
        a_cal = div.select_one("a[href^='https://calendar.google.com/']")
        if a_cal is None:
            return None
        cal = parse_google_calendar_template(a_cal.attrs["href"])
        a = div.select_one("a.c-snippet__titular")
        img = div.select_one("img")

        place = self.__get_place(cal).normalize()
        url = a.attrs["href"]
        ev = Event(
            id="fm"+to_uuid(url),
            url=url,
            name=_clean_name(cal.title),
            price=0,
            category=self.__find_category(url, div),
            img=img.attrs["src"],
            place=place,
            duration=(cal.end-cal.start).seconds//60,
            sessions=(
                Session(date=cal.start.strftime("%Y-%m-%d %H:%M")),
            )
        )
        return ev

    def __find_category(self, url: str, div: Tag):
        cat = get_text(div.select_one("div.c-titular"))
        if cat is not None:
            cat = cat.lower()
        val = {
            "concierto": Category.MUSIC,
            "cine": Category.CINEMA,
            "proyección": Category.CINEMA,
            "magia": Category.MAGIC,
            "conferencia": Category.CONFERENCE,
            "entrevista": Category.CONFERENCE,
            "debate": Category.CONFERENCE,
        }.get(cat)
        if val:
            return val
        logger.critical(str(CategoryUnknown(url, cat)))
        return Category.UNKNOWN

    def __get_place(self, cal: AnchorCal):
        if re_and(
            cal.location,
            "March",
            "Castelló 77"
        ):
            return FundacionMarch.CASTELLO
        return Place(
            name=cal.location,
            address=cal.location,
        )


if __name__ == "__main__":
    F = FundacionMarch()
    print(len(F.get_events()))

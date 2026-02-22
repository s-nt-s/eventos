from core.eventon import EventOn, Event as EventOnEvent
from core.event import Event, Place, Session, Category, CategoryUnknown
from functools import cached_property
from core.util import find_euros, re_or, re_and, capitalize
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Callable
from core.web import buildSoup, get_text
import re
from collections import defaultdict
from core.fetcher import Getter
from aiohttp import ClientResponse
from core.web import WEB


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


def to_datetime(i: int):
    dt = datetime.fromtimestamp(i, tz=ZoneInfo("UTC"))
    return dt.astimezone(ZoneInfo("Europe/Madrid"))


def _clean_name_place(name: str):
    name = re_sp.sub(" ", name)
    if re_or(name, "GILITOS"):
        return "Centro Cultural Gilitos"
    if re_and(name, "parador", "Alcalá", "Henares", flags=re.I):
        return "Parador Alcalá de Henares"
    if re_and(name, "HOSPITAL", "SANTA", r"MAR[ÍI]A", "RICA", flags=re.I):
        return "Hospital Santa María la Rica"
    if re_and(name, "C[ií]rculo", "Contribuyentes", flags=re.I):
        return "Círculo de contribuyentes"
    return capitalize(name)


def _clean_name(name: str):
    name = re_sp.sub(" ", name)
    name = re.sub(r"[ /]+Alcal[aá] a escena$", "", name, flags=re.I)
    return capitalize(name)


async def rq_to_dates(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    span = get_text(soup.select_one("span.event-time-sala"))
    if len(span) == 0:
        return tuple()
    dts: set[datetime] = set()
    for x in re.findall(r"(\d{2})/(\d{2})/(\d{4}) (\d{2}):(\d{2})", span):
        d, m, y, h, mm = map(int, x)
        dts.add(datetime(y, m, d, h, mm))
    return tuple(sorted(dts))


class Alcala:
    def __init__(
            self,
            isOkDate: Callable[[datetime], bool] = None,
    ):
        self.__eventon = EventOn("https://culturalcala.es/wp-json")
        self.__isOkDate = isOkDate or (lambda x: True)
        self.__get_store = Getter(
            onread=rq_to_dates,
            max_concurrency=10,
            timeout=60,
            raise_for_status=False,
        )

    @cached_property
    def events(self):
        id_store: dict[str, list[str]] = defaultdict(list)
        logger.info("Alcala: Buscando eventos")
        events: dict[str, Event] = {}
        for x in self.__eventon.get_eventon():
            e = self.__eventon_to_event(x)
            if e is not None:
                for c in x.customfields:
                    if c.startswith("https://www.giglon.com/evento/"):
                        id_store[e.id].append(c)
                events[e.id] = e
        sessions = self.__find_session_in_store(id_store)
        for id, ses in sessions.items():
            if len(ses):
                events[id] = events[id].merge(sessions=tuple(sorted(ses)))
        for id, ev in list(events.items()):
            if len(ev.sessions) == 0:
                logger.critical(f"sessions=None {ev.url}")
                del events[id]
        evs = tuple(events.values())
        logger.info(f"Alcala: Buscando eventos = {len(evs)}")
        return evs

    def __find_session_in_store(self, id_store: dict[str, list[str]]):
        store_url: set[str] = set()
        sessions: dict[str, set[str]] = defaultdict(set)
        for id, urls in id_store.items():
            store_url.update(urls)
        store_dates = self.__get_store.get(*store_url)
        for id, urls in id_store.items():
            for u in urls:
                for d in store_dates.get(u, []):
                    if self.__isOkDate(d):
                        sessions[id].add(Session(
                            date=d.strftime("%Y-%m-%d %H:%M")
                        ))
        return sessions

    def __eventon_to_event(self, x: EventOnEvent):
        place = self.__get_place(x)
        if place is None:
            logger.critical(f"place=None {x.permalink}")
            return None
        price = self.__find_price(x)
        if price is None:
            logger.critical(f"price=None {x.permalink}")
            return None
        duration, sessions = self.__find_session(x)
        e = Event(
            id=f"al{x.id}",
            url=x.permalink,
            name=_clean_name(x.name),
            price=self.__find_price(x),
            category=self.__find_category(x),
            place=place,
            duration=duration,
            sessions=sessions,
            img=x.image_url
        )
        return e

    def __get_place(self, x: EventOnEvent):
        if x.location_name is None:
            return None
        latlon = None if None in (x.location_lat, x.location_lon) else f"{x.location_lat},{x.location_lon}"
        if x.location_address is None and latlon is None:
            return None
        return Place(
            name=_clean_name_place(x.location_name),
            address=x.location_address,
            latlon=latlon
        ).normalize()

    def __find_price(self, x: EventOnEvent):
        return find_euros(
            *x.customfields
        )

    def __find_category(self, x: EventOnEvent):
        if not x.event_types:
            logger.critical(f"event_types=None {x.permalink}")
            return Category.UNKNOWN
        content = buildSoup(x.permalink, x.content or x.details or '')
        for br in content.select("br, p"):
            br.append("\n")
        txt_content = get_text(content)
        for k, cat in {
            r"T[IÍ]TERES.*P[UÚ]BLICO FAMILIAR": Category.CHILDISH,
            r"TEATRO INFANTIL": Category.CHILDISH,
            r"EXPERIENCIA GASTRON[OÓ]MICA": Category.PARTY,
            r"VISITA TEATRALIZADA": Category.THEATER,
            r"[dD]eclamaci[oó]n de poemas": Category.POETRY,
        }.items():
            if re_or(txt_content, k):
                return cat
        tp = x.event_types[0].lower()
        if tp == "música y danza":
            if re_or(txt_content, "danza", flags=re.I):
                return Category.DANCE
            return Category.MUSIC
        cat = {
            "cine": Category.CINEMA,
            "exposiciones": Category.EXPO,
            "teatro": Category.THEATER,
            "talleres": Category.WORKSHOP,
            "literatura y conferencias": Category.CONFERENCE,
            "programación familiar": Category.CHILDISH,
        }.get(tp)
        if cat == Category.THEATER:
            if re_and(txt_content, r"CLOW", r"P[UÚ]BLICO FAMILIAR"):
                return Category.CHILDISH
            if re_or(txt_content, r"T[ÍI]TERES"):
                return Category.PUPPETRY
        if cat is not None:
            return cat
        logger.critical(str(CategoryUnknown(x.permalink, ', '.join(x.event_types))))
        return Category.UNKNOWN

    def __find_session(self, x: EventOnEvent):
        durations: set[int] = set()
        sessions: set[Session] = set()
        dts = list(x.repeats)
        dts.insert(0, (x.start, x.end))
        for s, e in dts:
            if None in (s, e) or s > e:
                continue
            st = to_datetime(s)
            en = to_datetime(e)
            durations.add((en-st).seconds // 60)
            if not self.__isOkDate(st):
                continue
            sessions.add(Session(
                date=st.strftime("%Y-%m-%d %H:%M")
            ))
        minutes = max(durations) if durations else 0
        return minutes, tuple(sorted(sessions))


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/alcala.log", log_level=(logging.DEBUG))
    a = Alcala()
    print(len(a.events))

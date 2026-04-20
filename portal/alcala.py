from core.eventon import EventOn, Event as EventOnEvent
from core.event import Event, Place, Session, Category, CategoryUnknown, Cinema
from functools import cached_property
from core.util import find_euros, re_or, re_and
from core.util.strng import capitalize
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Callable
from core.web import buildSoup, get_text
import re
from collections import defaultdict
from core.fetcher import Getter
from aiohttp import ClientResponse
from core.cache import TupleCache
from functools import cache
from core.md import MD


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")
MAX_YEAR = datetime.now().year + 1


@cache
def get_content(x: EventOnEvent):
    html = x.content or x.details
    if html is None:
        return None
    content = buildSoup(x.permalink, html)
    return MD.convert(content)


def to_datetime(i: int | str):
    if i is None:
        return None
    if isinstance(i, str):
        dt = datetime.strptime(i, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=ZoneInfo("Europe/Madrid"))
    if isinstance(i, int):
        dt = datetime.fromtimestamp(i, tz=ZoneInfo("UTC"))
        return dt.astimezone(ZoneInfo("Europe/Madrid"))
    raise ValueError(i)


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
    if span is None or len(span) == 0:
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
    @TupleCache("rec/alcala.json", builder=Event.build)
    def events(self):
        id_store: dict[str, set[str]] = defaultdict(set)
        logger.info("Alcala: Buscando eventos")
        events: dict[str, Event] = {}
        for x in self.__eventon.get_eventon():
            e = self.__eventon_to_event(x)
            if e is not None:
                for c in x.customfields:
                    if c.startswith("https://www.giglon.com/evento/"):
                        id_store[e.id].add(c)
                events[e.id] = e
        sessions = self.__find_session_in_store(id_store)
        for id, ev in list(events.items()):
            ss: set[Session] = set()
            for s in sessions.get(ev.id, ev.sessions):
                if self.__isOkDate(to_datetime(s.date)):
                    ss.add(s)
            if len(ss) == 0:
                logger.critical(f"sessions=None {ev.url}")
                del events[id]
                continue
            events[id] = ev.merge(
                sessions=tuple(sorted(ss))
            )
        evs = tuple(events.values())
        logger.info(f"Alcala: Buscando eventos = {len(evs)}")
        return evs

    def __find_session_in_store(self, id_store: dict[str, set[str]]):
        store_url: set[str] = set()
        sessions: dict[str, set[str]] = defaultdict(set)
        for id, urls in id_store.items():
            store_url.update(urls)
        store_dates: dict[str, tuple[datetime, ...]] = self.__get_store.get(*store_url)
        for id, urls in id_store.items():
            for u in urls:
                for d in (store_dates.get(u) or tuple()):
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
            img=x.image_url,
            cycle=self.__find_cycle(x)
        )
        e = self.__complete(e, x) or e
        return e

    def __complete(self, e: Event | Cinema, x: EventOnEvent):
        e = e.fix_type()
        if not isinstance(e, Cinema):
            return e
        txt_content = get_content(x)
        if txt_content is None:
            return e
        m = set(y for y in map(int, re.findall(r"Año: (\d+)", txt_content)) if y > 1900 and y <= MAX_YEAR)
        if len(m) == 1:
            e = e.merge(year=m.pop())
        m = set(s for s in map(str.strip, re.findall(r"Direcci[óo]n: ([^\n]+)", txt_content)) if s)
        if len(m) == 1:
            e = e.merge(director=(m.pop(), ))
        return e

    def __find_cycle(self, x: EventOnEvent):
        txt_content = get_content(x)
        if re_or(
            txt_content,
            r"Tour del talento \d+",
            flags=re.I
        ):
            return "Tour del talento"

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
        if re_or(
            x.name,
            r"Jornada de puertas abiertas",
            flags=re.I
        ):
            return Category.NO_EVENT
        if re_or(
            x.name,
            "MARCHAS PROCESIONALES",
            "m[uú]sica procesional",
            flags=re.I
        ):
            return Category.RELIGION
        if re_or(
            x.name,
            ("taller", "para j[oó]venes"),
            flags=re.I
        ):
            return Category.YOUTH
        if not x.event_types:
            logger.critical(f"event_types=None {x.permalink}")
            return Category.UNKNOWN
        txt_content = get_content(x)
        for cat, _or_ in {
            Category.CHILDISH: (
                r"T[IÍ]TERES.*P[UÚ]BLICO FAMILIAR",
                r"TEATRO (INFANTIL|FAMILIAR)",
                r"[Ee]spect[aá]culo infantil",
                r"ESPECIALMENTE RECOMENDADA PARA LA INFANCIA",
                r"[Ss]umergirse en familia"
                r"Teatro familiar",
                r"sumergirse en familia",
                r"a partir de \d años",
            ),
            Category.PARTY: (
                r"EXPERIENCIA GASTRON[OÓ]MICA",
            ),
            Category.THEATER: (
                r"VISITA TEATRALIZADA",
                "TEATRO",
                r"ESPECT[AÁ]CULO DE CALLE.*[pP]asacalles?.*[Aa]ctor[aex@]s?",
            ),
            Category.POETRY: (
                r"[dD]eclamaci[oó]n de poemas",
                r"[iI]nstalaci[oó]n po[eé]tica",
                r"[Gg][eé]nero: [pP]oes[ií]a",
            ),
            Category.PUPPETRY: (
                r"T[IÍ]TERES",
            ),
            Category.MUSIC: (
                r"ESPECT[AÁ]CULO DE CALLE.*Batucada itinerante",
            ),
            Category.WORKSHOP: (
                r"G[eé]nero: Taller",
            )
        }.items():
            if re_or(txt_content, *_or_, flags=re.DOTALL):
                return cat
        for tp in map(str.lower, x.event_types):
            if tp == "música y danza":
                if re_or(
                    txt_content,
                    r"Tour del talento \d+",
                    flags=re.I
                ):
                    return Category.MUSIC
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
        txt_content = get_content(x)
        times = set(re.findall(r"(\d{2}:\d{2})", txt_content or ''))
        dates: set[datetime] = set()
        max_dur = 2*60
        for s, e in dts:
            if None in (s, e) or s > e:
                continue
            st = to_datetime(s)
            en = to_datetime(e)
            dr = (en-st).seconds // 60
            if en.strftime("%H:%M") == "23:59" and dr > max_dur:
                dr = max_dur
            durations.add(dr)
            dates.add(st)
        if len(dates) == 0:
            return 0, tuple()
        if len(times) == 1 and len(dates) == 1:
            h, m = map(int, times.pop().split(":"))
            st = dates.pop().replace(hour=h, minute=m)
            dates = {st, }
        for st in sorted(dates):
            sessions.add(Session(
                date=st.strftime("%Y-%m-%d %H:%M")
            ))
        return max(durations), tuple(sorted(sessions))


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/alcala.log", log_level=(logging.DEBUG))
    a = Alcala()
    print(len(a.events))

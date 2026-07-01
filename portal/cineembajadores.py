from core.web import Web, Tag, get_text
from core.place import Places, Place
from core.event import Event, CategoryUnknown, Category, Session
from core.cache import TupleCache
from core.util import to_uuid, re_or
from collections import defaultdict
import re
import logging
from datetime import datetime
import pytz
from core.md import MD
from portal.base import Base


logger = logging.getLogger(__name__)

RE_SUFIX = re.compile(r"\s*\(\s*(VOSE|DOBLADA AL ESPAÑOL)\s*\)$", re.IGNORECASE)
NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))


def _clean_name(name: str):
    name = RE_SUFIX.sub("", name)
    name = re.sub(r"\s+\(En diferido desde[^\(\)]+\)\s*$", "", name, flags=re.I)
    return name


def _get_date_time(p: Tag):
    d, m, hh, mm = map(int, re.findall(r"\d+", p.attrs["data-dia"] + " " + p.attrs["data-hora"]))
    dt = datetime(
        NOW.year,
        m,
        d,
        hh,
        mm,
        tzinfo=pytz.timezone('Europe/Madrid')
    )
    if dt.date() < NOW.date() and NOW.month == 12 and dt.month == 1:
        dt = dt.replace(year=NOW.year + 1)
    return dt


def _find_place(p: Tag):
    rec = p.attrs["data-recinto"]
    if rec == "Cine Embajadores":
        return Places.CINE_EMBAJADORES.value
    if rec == "Cine Embajadores Río":
        return Places.CINE_EMBAJADORES_RIO.value
    logger.warning(f"Recinto desconocido: {rec}")
    return None


class CineEmbajadores(Base):
    def __init__(self, cache: str | bool = True):
        super().__init__(cache=cache)
        self.__web = Web(verify=False)

    def _get_events(self):
        events: set[Event] = set()
        soup = self.__web.get_soup("https://cinesembajadores.es/madrid/")
        for div in soup.select("li.movie"):
            evs = self.__div_to_event(div)
            if evs is not None and len(evs):
                events.update(evs)
        return tuple(sorted(events))

    def __find_duration(self, div: Tag):
        txt = get_text(div.select_one("li.minutos"))
        if txt is None:
            return None
        num = map(int, re.findall(r"\d+", txt))
        return next(num, None)

    def __div_to_event(self, div: Tag):
        a = div.select_one("div.info h2 a")
        img = div.select_one("div.poster img")
        url = a["href"]
        template = Event(
            id=f"cemb{to_uuid(url)}",
            name=_clean_name(get_text(a)),
            url=url,
            category=Category.UNKNOWN,
            duration=self.__find_duration(div),
            img=img.attrs["src"] if img else None,
            sessions=tuple(),
            price=self.__find_price(div),
            place=None,
        )
        place_session = self.__find_place_session(div)
        if len(place_session) == 0:
            return None
        director = self.__find_director(div)
        if director and len(director):
            template = template.merge(category=Category.CINEMA).fix_type().merge(
                director=director
            )
        if template.category == Category.UNKNOWN:
            template = template.merge(category=self.__find_category(div))
        events: set[Event] = set()
        for place, sessions in place_session.items():
            if len(sessions):
                events.add(template.merge(
                    place=place,
                    sessions=tuple(sorted(sessions))
                ))
        return events

    def __find_category(self, div: Tag):
        a = div.select_one("div.info h2 a")
        url = a["href"]
        name = get_text(a)
        if re_or(
            name,
            r"Sesi[oó]n TETA",
            flags=re.I
        ):
            return Category.NON_GENERAL_PUBLIC
        if re_or(
            name,
            r"OPERA FESTIVAL",
            r"Teatro alla Scala de Mil[aá]n",
            flags=re.I
        ):
            return Category.THEATER
        if re_or(
            name,
            r"Cl[aá]sicos al detalle",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            name,
            r"German Film Fest",
            r"Cortometrajes?",
            r"Pel[ií]cula SORPRESA",
            r"Cine a ciegas",
            r"VOSE",
            r"DOBLADA AL ESPAÑOL",
            r"Cine con piano en directo",
            r"Domingo de cl[aá]sicos",
            r"Cuba vibra",
            r"Espacio Queer",
            r"festival de cine",
            flags=re.I
        ):
            return Category.CINEMA
        if re_or(
            name,
            r"M[uú]sica en la oscuridad",
            flags=re.I
        ):
            return Category.MUSIC
        if next(self.__yield_field(
            div,
            re.compile(r"^\s*Reparto\s*:\s*.*?(Documental).*?$", re.I)
        ), None):
            return Category.CINEMA
        sinopsis = MD.convert(div.select_one("div.sinopsis"))
        if re_or(
            sinopsis,
            r"es un podcast",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            sinopsis,
            r"documental",
            r"proyecci[óo]n",
            r"cortometrajes",
            flags=re.I
        ):
            return Category.CINEMA
        if len(tuple(self.__yield_field(
            div,
            re.compile(r"^\s*(Reparto|Direcci[oó]n)\s*:\s*.+\s*$", re.I)
        ))) > 1:
            return Category.CINEMA
        if get_text(div.select_one("div.info li.doblaje")):
            return Category.CINEMA
        logger.critical(str(CategoryUnknown(url, name)))
        return Category.UNKNOWN

    def __find_price(self, div: Tag):
        name = get_text(div.select_one("div.info h2 a"))
        if re_or(
            name,
            r"German Film Fest.*NEXT GENERATION SHORT TIGER",
            flags=re.I
        ):
            return 3.50
        if re_or(
            name,
            r"German Film Fest",
            flags=re.I
        ):
            return 5
        if re_or(
            name,
            r"M[uú]sica en la oscuridad",
            flags=re.I
        ):
            return 6
        if re_or(
            name,
            r"CINE CON PIANO EN DIRECTO",
            flags=re.I
        ):
            return 12
        if re_or(
            name,
            r"Cl[aá]sicos al detalle",
            flags=re.I
        ):
            return 10
        if re_or(
            name,
            r"OPERA FESTIVAL",
            r"Teatro alla Scala de Mil[aá]n",
            flags=re.I
        ):
            return 9
        if re_or(
            name,
            r"Running Film Festival",
            flags=re.I
        ):
            return 25

        return 7.5

    def __yield_field(self, div: Tag, rgx: re.Pattern):
        for h in map(get_text, div.select("div.more h5")):
            m = rgx.match(h)
            if m:
                yield m

    def __find_director(self, div: Tag):
        for m in self.__yield_field(
            div,
            re.compile(r"^\s*Director\s*:\s*(.+)$", re.I)
        ):
            directors: list[str] = []
            for d in tuple(map(str.strip, re.split(r",\s+", m.group(1)))):
                if d and d not in directors:
                    directors.append(d)
            if len(directors):
                return tuple(directors)

    def __find_place_session(self, div: Tag):
        place_session: dict[Place, set[Session]] = defaultdict(set)
        for p in div.select("p[data-direccion][data-dia][data-hora]"):
            mdt = _get_date_time(p)
            place = _find_place(p)
            url = p.select_one("a").attrs["href"]
            if place is None:
                continue
            if mdt < NOW:
                logger.warning(f"Se ha encontrado una sesión pasada: {mdt}")
                continue
            place_session[place].add(Session(
                date=mdt.strftime("%Y-%m-%d %H:%M"),
                url=url
            ))
        return place_session


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/cineembajadores.log", log_level=logging.INFO)
    CineEmbajadores().get_events()

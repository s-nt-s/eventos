from dataclasses import dataclass, asdict
from typing import NamedTuple, Tuple, Dict, List, Union, Any
from .util import get_obj
from urllib.parse import quote
from enum import IntEnum
from functools import cached_property
from urllib.parse import quote_plus
import re
from datetime import date, datetime
from core.web import Web, get_text
from core.filemanager import FM

FIX_EVENT: Dict[str, Dict[str, Any]] = FM.load("fix/event.json")

MONTHS = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")

re_filmaffinity = re.compile(r"https://www.filmaffinity.com/es/film\d+.html")


class FieldNotFound(Exception):
    def __init__(self, field: str, scope=None):
        msg = "NOT FOUND "+field
        if scope is not None:
            msg = msg + f" in {scope}"
        super().__init__(msg)


class FieldUnknown(Exception):
    def __init__(self, field: str, value: str):
        super().__init__(f"UNKNOWN {field}: {value}")


class Category(IntEnum):
    CINEMA = 1
    MUSIC = 2
    CIRCUS = 3
    WORKSHOP = 4
    DANCE = 5
    PUPPETRY = 6  # Títeres
    THEATER = 7
    EXPO = 8
    CONFERENCE = 9
    VISIT = 10
    CHILDISH = 11 # infantil
    OTHERS = 12
    RECITAL = 13
    YOUTH = 14

    def __str__(self):
        if self == Category.CINEMA:
            return "cine"
        if self == Category.MUSIC:
            return "música"
        if self == Category.CIRCUS:
            return "circo"
        if self == Category.WORKSHOP:
            return "taller"
        if self == Category.DANCE:
            return "danza"
        if self == Category.PUPPETRY:
            return "títeres"
        if self == Category.THEATER:
            return "teatro"
        if self == Category.EXPO:
            return "exposición"
        if self == Category.CONFERENCE:
            return "conferencia"
        if self == Category.VISIT:
            return "visita"
        if self == Category.CHILDISH:
            return "infantil"
        if self == Category.OTHERS:
            return "otros"
        if self == Category.RECITAL:
            return "recital"
        raise ValueError()


class Session(NamedTuple):
    url: str = None
    date: str = None

    def merge(self, **kwargs):
        return Session(**{**self._asdict(), **kwargs})

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        return Session(**obj)

    @property
    def hour(self):
        if self.date is not None:
            fch = self.date.split(" ")
            if len(fch[-1]) == 5:
                return fch[-1]
        return None

    @property
    def id(self):
        return re.sub(r"\D+", "", self.date)


class Place(NamedTuple):
    name: str
    address: str
    latlon: str = None

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        return Place(**obj)

    @property
    def url(self):
        if self.latlon is not None:
            return "https://www.google.com/maps?q=" + self.latlon
        if self.address is None:
            return "#"
        if re.match(r"^[\d\.,]+$", self.address):
            return "https://www.google.com/maps?q=" + self.address
        return "https://www.google.com/maps/place/" + quote(self.address)


def _clean_name(name: str):
    if name is None:
        return None
    name = re.sub(r"\s*\(Ídem\)\s*$", "", name, flags=re.IGNORECASE)
    name = name.strip(". ")
    if re.search(r"^Cinefórum[^':]*:[^':]*'.*'", name):
        return name.split("'", 2)[1].strip()
    if re.search(r"^Cinefórum en la Biblioteca Mario Vargas Llosa:", name):
        return name.split(":", 1)[1].strip()
    if re.search(r"^Madrid, plató de cine: '.*'", name):
        return name.split("'", 2)[1].strip()
    return name


@dataclass(frozen=True, order=True)
class Event:
    id: str
    url: str
    name: str
    img: str
    price: float
    category: Category
    place: Place
    duration: int
    sessions: Tuple[Session] = tuple()

    def __post_init__(self):
        object.__setattr__(self, 'name', _clean_name(self.name))
        if self.img in (
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/CineForum_260x260.jpg',
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/MadridPlat%C3%B3Cine_260.png',
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Infantiles_Juveniles/Cine/ficheros/2504_CineForumPerezGaldos_260x260.jpg'
        ):
            object.__setattr__(self, 'img', None)
        if self.img is None and re_filmaffinity.match(self.more or ''):
            soup = Web().get(self.more)
            img = soup.select_one("#right-column a.lightbox img")
            if img:
                object.__setattr__(self, 'img', img.attrs.get('src'))

    def merge(self, **kwargs):
        return Event(**{**asdict(self), **kwargs})

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        obj['category'] = Category(obj['category'])
        obj['place'] = Place.build(obj['place'])
        obj['sessions'] = tuple(map(Session.build, obj['sessions']))
        return Event(**obj)

    @cached_property
    def title(self):
        txt = str(self.name)
        if txt == txt.upper():
            txt = txt.title()
        if txt[0]+txt[-1] == "«»":
            _txt = txt[1:-1]
            if "«" not in _txt and "»" not in _txt:
                txt = _txt
        return txt

    @cached_property
    def more(self):
        fix_more = FIX_EVENT.get(self.id, {}).get("more")
        if fix_more:
            return fix_more
        title = re.sub(r"\s*\+\s*Coloquio\s*$", "", self.title, flags=re.IGNORECASE)
        txt = quote_plus(title)
        if self.category == Category.CINEMA:
            w = Web()
            w.get("https://www.filmaffinity.com/es/search.php?stext="+txt)
            if re_filmaffinity.match(w.url):
                return w.url
            lwtitle = title.lower()
            for a in w.soup.select("div.mc-title a"):
                if get_text(a).lower() == lwtitle:
                    return a.attrs["href"]
            return "https://www.google.es/search?&complete=0&gbv=1&q="+txt

    @property
    def dates(self):
        days: Dict[str, List[Session]] = {}
        for e in self.sessions:
            dh = e.date.split(" ")
            dt = date(*map(int, dh[0].split("-")))
            day = "LMXJVSD"[dt.weekday()] + \
                f' {dt.day:>2}-'+MONTHS[dt.month-1]
            if day not in days:
                days[day] = []
            days[day].append(e)
        return tuple(days.items())

    @property
    def end(self):
        if len(self.sessions) == 0:
            return None
        endings = set()
        for s in self.sessions:
            endings.add(s.date)
        return max(endings)

    def remove_old_sessions(self, now: Union[str, datetime]):
        if isinstance(now, datetime):
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
        sessions = tuple(filter(lambda s:s.date>=now, self.sessions))
        object.__setattr__(self, 'sessions', sessions)
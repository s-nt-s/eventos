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
import logging
from functools import cache

logger = logging.getLogger(__name__)

FIX_EVENT: Dict[str, Dict[str, Any]] = FM.load("fix/event.json")

MONTHS = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")

re_filmaffinity = re.compile(r"https://www.filmaffinity.com/es/film\d+.html")


@cache
def get_festivos(year: int):
    dates: set[str] = set()
    soup = Web().get(f"https://www.calendarioslaborales.com/calendario-laboral-madrid-{year}.htm")
    for month, div in enumerate(soup.select("#wrapIntoMeses div.mes")):
        for day in map(get_text, div.select("td[class^='cajaFestivo']")):
            dt = date(year, month+1, int(day))
            dates.add(dt)
    return tuple(sorted(dates))


class FieldNotFound(Exception):
    def __init__(self, field: str, scope=None):
        msg = "NOT FOUND "+field
        if scope is not None:
            msg = msg + f" in {scope}"
        super().__init__(msg)


class FieldUnknown(Exception):
    def __init__(self, url: str, field: str, value: str):
        super().__init__(f"UNKNOWN {field}: {value} <-- {url}")


class CategoryUnknown(FieldUnknown):
    def __init__(self, url: str, value: str):
        super().__init__(url, "category", value)


class Category(IntEnum):
    SPAM = -1
    UNKNOWN = 9999999
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
    #OTHERS = 12
    YOUTH = 14
    READING_CLUB = 15
    CONTEST = 16
    SPORT = 17
    POETRY = 18
    ACTIVISM = 19
    SENIORS = 20
    ORGANIZATIONS = 21
    MARGINNALIZED = 22
    NON_GENERAL_PUBLIC = 23
    ONLINE = 24
    HIKING = 35 # senderismo
    MAGIC = 36

    def __str__(self):
        #if self == Category.OTHERS:
        #    return "otros"
        if self == Category.UNKNOWN:
            return "otros"
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
        if self == Category.YOUTH:
            return "juventud"
        if self == Category.READING_CLUB:
            return "club lectura"
        if self == Category.CONTEST:
            return "concurso"
        if self == Category.SPORT:
            return "deporte"
        if self == Category.POETRY:
            return "poesía"
        if self == Category.ACTIVISM:
            return "activismo"
        if self == Category.SENIORS:
            return "mayores"
        if self == Category.ORGANIZATIONS:
            return "organizaciones"
        if self == Category.MARGINNALIZED:
            return "marginados"
        if self == Category.NON_GENERAL_PUBLIC:
            return "público no general"
        if self == Category.ONLINE:
            return "online"
        if self == Category.HIKING:
            return "senderismo"
        if self == Category.SPAM:
            return "spam"
        if self == Category.MAGIC:
            return "magia"
        raise ValueError(self.value)

    def __lt__(self, other):
        if self == Category.UNKNOWN:
            return False
        if other == Category.UNKNOWN:
            return True
        return str(self).__lt__(str(other))


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

    def isWorkingHours(self):
        if self.date is None:
            return False
        dt = self.get_date()
        if dt.weekday() in (5, 6):
            return False
        if dt.date() in get_festivos(dt.year):
            return False
        if dt.hour > 15:
            return False
        return True

    def get_date(self):
        dt_int = tuple(map(int, re.split(r"\D+", self.date)))
        return datetime(*dt_int)


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


def unquote(s: str):
    quotes = ("'", '"')
    bak = ''
    while bak != s:
        bak = str(s)
        if len(s) > 2 and s[0] == s[-1] and s[0] in quotes:
            s = s[1:-1]
        if (s.count('"'), s.count("'")) == (1, 1):
            s = s.replace('"', "'")
        if len(s) > 2 and s[0] in quotes and s[0] not in s[1:]:
            s = s[1:]
        if len(s) > 2 and s[-1] in quotes and s[-1] not in s[:-1]:
            s = s[:-1]
        s = s.strip()
    return s


def _clean_name(name: str):
    if name is None:
        return None
    bak = ''
    while bak != name:
        bak = str(name)
        name = re.sub(r"\s*\(Ídem\)\s*$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"\.\s*(conferencia)\s*$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"Visita a la exposición '([^']+)'\. .*", r"\1", name, flags=re.IGNORECASE)
        name = re.sub(r"^(lectura dramatizada|presentación del libro|Cinefórum[^:]*|^Madrid, plató de cine)\s*[\.:]\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^(conferencia|visita[^'\"]*)[\s:]+(['\"])", r"\2", name, flags=re.IGNORECASE)
        name = re.sub(r"^(conferencia)\s*-\s*", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^(conferencia)\s*", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^visita (comentada|guiada)(:| -)\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^Proyección del documental:\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^(Cine .*)?Proyección de (['\"])", r"\2", name, flags=re.IGNORECASE)
        name = re.sub(r"^Cineclub con .* '([^']+)'.*", r"\1", name, flags=re.IGNORECASE)
        name = unquote(name.strip(". "))
        if len(name) < 2:
            name = str(bak)
    name = unquote(name)
    w1 = name[0]
    if w1.isalpha():
        name = w1.upper()+name[1:]
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
        new_name = _clean_name(self.name)
        if new_name != self.name:
            logger.debug(f"FIX: {new_name} <- {self.name}")
            object.__setattr__(self, 'name', new_name)
        if self.img in (
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/CineForum_260x260.jpg',
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/MadridPlat%C3%B3Cine_260.png',
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Infantiles_Juveniles/Cine/ficheros/2504_CineForumPerezGaldos_260x260.jpg',
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Teatro_Performance/ficheros/250429_BuscandoHogar_260x260.jpg',
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/Cineclub_javierdelatorre_260.jpg',
            'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Conferencias/ficheros/Ajam_260x260.jpg',
            'https://www.casamerica.es/themes/casamerica/images/cabecera_generica.jpg',
        ):
            object.__setattr__(self, 'img', None)

    def fix(self):
        if self.img is None and re_filmaffinity.match(self.more or ''):
            soup = Web().get(self.more)
            img = soup.select_one("#right-column a.lightbox img")
            if img:
                object.__setattr__(self, 'img', img.attrs.get('src'))
        return self

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
            if self.url and self.url.startswith("https://tienda.madrid-destino.com/es/"):
                w.get(self.url)
                a = w.soup.select_one("a.c-mod-file-event__content-link")
                if a and a.attrs.get("href"):
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
        sessions = tuple(filter(lambda s: s.date >= now, self.sessions))
        object.__setattr__(self, 'sessions', sessions)

    def remove_working_sessions(self):
        sessions = []
        w = 'LMXJVSD'
        for s in self.sessions:
            if s.isWorkingHours():
                d = s.get_date()
                logger.debug(f"Sesion {s.date} {w[d.weekday()]} eliminada por estar en horario de trabajo")
                continue
            sessions.append(s)
        object.__setattr__(self, 'sessions', tuple(sessions))

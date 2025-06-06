from dataclasses import dataclass, asdict, fields
from typing import NamedTuple, Tuple, Dict, List, Union, Any, Optional, Set
from core.util import get_obj, plain_text, getKm, get_domain, get_img_src, get_a_href, re_or, get_main_value
from core.util.madrides import find_more_url as find_more_url_madrides
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
from .util import to_uuid

logger = logging.getLogger(__name__)

NOW = date.today().strftime("%Y-%m-%d")
FIX_EVENT: Dict[str, Dict[str, Any]] = FM.load("fix/event.json")

MONTHS = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")

re_filmaffinity = re.compile(r"https://www.filmaffinity.com/es/film\d+.html")

WEB = Web()

@cache
def get_festivos(year: int):
    dates: set[str] = set()
    soup = WEB.get_cached_soup(f"https://www.calendarioslaborales.com/calendario-laboral-madrid-{year}.htm")
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
        return to_uuid(re.sub(r"\D+", "", self.date) + (self.url or ""))

    def isWorkingHours(self):
        if self.date is None:
            return False
        dt = self.get_date()
        hm = dt.hour + (dt.minute/100)
        if hm == 0 or hm > 15:
            return False
        if dt.weekday() in (5, 6):
            return False
        if dt.date() in get_festivos(dt.year):
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

    def getKmFrom(self, lat: float, lon: float):
        if self.latlon is None:
            return None
        lt, ln = map(float, self.latlon.split(","))
        return getKm(lt, ln, lat, lon)


def unquote(s: str):
    quotes = ("'", '"')
    bak = ''
    while bak != s:
        bak = str(s)
        for q in quotes:
            s = re.sub(rf'^{q}([^{q}]+: {q}[^{q}]+{q})$', r"\1", s)
        if len(s) > 2 and s[0] == s[-1] and s[0] in quotes:
            s = s[1:-1]
        if len(s) > 2 and s[0] in quotes and s[0] not in s[1:]:
            s = s[1:]
        if len(s) > 2 and s[-1] in quotes and s[-1] not in s[:-1]:
            s = s[:-1]
        s = s.strip()
    return s


def _clean_name(name: str, place: str):
    if name is None:
        return None
    place = plain_text((place or "").lower())
    bak = ['']
    while bak[-1] != name:
        bak.append(str(name))
        if "'" not in name:
            name = re.sub(r'["`´”“]', "'", name)
        for k, v in {
            "A.I At War": "A.I. At War",
            "AI At War": "A.I. At War",
            "El sorprendente Dr.Clitterhouse": "El sorprendente Dr. Clitterhouse",
            "El sorprendente Dr.Clitterhousem": "El sorprendente Dr. Clitterhouse"
        }.items():
            name = re.sub(r"^\s*"+(r"\s+".join(map(re.escape, re.split("\s+", k))))+r"\s*$", v, name, flags=re.IGNORECASE)
        name = re.sub(r"Matadero (Madrid )?Centro de Creación Contemporánea", "Matadero", name, flags=re.IGNORECASE)
        name = re.sub(r"\s*\(Ídem\)\s*$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"\.\s*(conferencia)\s*$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"Visita a la exposición '([^']+)'\. .*", r"\1", name, flags=re.IGNORECASE)
        name = re.sub(r"^(lectura dramatizada|presentación del libro|Cinefórum[^:]*|^Madrid, plató de cine)\s*[\.:]\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^(conferencia|visita[^'\"]*)[\s:]+(['\"])", r"\2", name, flags=re.IGNORECASE)
        name = re.sub(r"^(conferencia|concierto|espect[aá]culo|proyección( película)?)\s*[\-:\.]\s*", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^(conferencia)\s*", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^visita (comentada|guiada)(:| -)\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^Proyección del documental:\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^(Cine .*)?Proyección de (['\"])", r"\2", name, flags=re.IGNORECASE)
        name = re.sub(r"^Cineclub con .* '([^']+)'.*", r"\1", name, flags=re.IGNORECASE)
        name = re.sub(r"\s*-\s*(moncloa|arganzuela|retiro|chamberi)\s*$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"^(Exposición|Danza|Música):? ([\"'`])(.+)\2$", r"\3", name, flags=re.IGNORECASE)
        name = re.sub(r"Red de Escuelas Municipales del Ayuntamiento de Madrid", "red de Escuelas", name, flags=re.IGNORECASE)
        name = re.sub(r".*Ciclo de conferencias de la Sociedad Española de Retórica': (['\"])", r"\1", name, flags=re.IGNORECASE)
        name = re.sub(r"\s*-\s*$", "", name)
        name = re.sub(r"Asociación (de )?Jubilados( (del )?Ayuntamiento( de Madrid)?)?", "asociación de jubilados", name, flags=re.I)
        name = re.sub(r"^Proyección de la película '([^']+)'", r"\1", name, flags=re.I)
        name = re.sub(r"^(Obra de teatro|Noches? de Clásicos?|21 Distritos)\s*[:\-]\s*", r"", name, flags=re.I)
        name = re.sub(r"Piano City (Madrid \d+|Madrid|\d+)", r"Piano City", name, flags=re.I)
        name = re.sub(r"CinePlaza:.*?> (Proyección|Cine)[^:]*:\s+", "", name, flags=re.I)
        name = unquote(name.strip(". "))
        if len(name) < 2:
            name = bak[-1]
    name = unquote(name)
    w1 = name[0]
    if w1.isalpha():
        name = w1.upper()+name[1:]
    return name


KO_IMG = (
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/CineForum_260x260.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/MadridPlat%C3%B3Cine_260.png',
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Infantiles_Juveniles/Cine/ficheros/2504_CineForumPerezGaldos_260x260.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Teatro_Performance/ficheros/250429_BuscandoHogar_260x260.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/Cineclub_javierdelatorre_260.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Conferencias/ficheros/Ajam_260x260.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/DistritoVillaverde/Actividades/ficheros/Bohemios.jpg',
    'https://www.madrid.es/UnidadWeb/Contenidos/Ficheros/TemaCulturaYOcio/Bohemios.jpg',
    'https://www.madrid.es/UnidadWeb/Contenidos/Ficheros/canalcasareloj.png',
    'https://www.casamerica.es/themes/casamerica/images/cabecera_generica.jpg',
    'https://cdn.lacasaencendida.es/storage/39522/conversions/stivijoes-6-adricuerdo-adria?n-cuerdojpg-detail.jpg',
    'https://www.madrid.es/UnidadWeb/UGBBDD/EntidadesYOrganismos/CulturaYOcio/InstalacionesCulturales/CentrosCulturalesMunicipales/CCArganzuela/centrodotacionalArganzuela.png',
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/Cine_260x260.jpg'
)


@dataclass(frozen=True, order=True)
class Event:
    id: str
    url: str
    name: str
    price: float
    category: Category
    place: Place
    duration: int
    publish: str = NOW
    img: Optional[str] = None
    also_in: Tuple[str] = tuple()
    sessions: Tuple[Session] = tuple()

    def __post_init__(self):
        new_name = _clean_name(self.name, self.place.name)
        if new_name != self.name:
            logger.debug(f"FIX: {new_name} <- {self.name}")
            object.__setattr__(self, 'name', new_name)
        for f in fields(self):
            v = getattr(self, f.name, None)
            if isinstance(v, list):
                object.__setattr__(self, f.name, tuple(v))
            elif isinstance(v, str) and len(v) == 0:
                object.__setattr__(self, f.name, None)

    def fix(self, **kwargs):
        for k, v in kwargs.items():
            if v is not None:
                object.__setattr__(self, k, v)
        for f in fields(self):
            self.__fix_field(f.name)
        return self

    def __fix_field(self, name: str):
        fix_event = FIX_EVENT.get(self.id, {})
        old_val = getattr(self, name, None)
        if name in fix_event:
            fix_val = fix_event[name]
        else:
            fnc = getattr(self, f'_fix_{name}', None)
            if fnc is None or not callable(fnc):
                return
            fix_val = fnc()
        if fix_val == old_val:
            return
        logger.debug(f"FIX: {name} {fix_val} <- {old_val}")
        object.__setattr__(self, name, fix_val)

    def __get_urls(self):
        arr: List[str] = [None, ]
        if self.url not in arr:
            arr.append(self.url)
        for url in self.also_in:
            if url not in arr:
                arr.append(url)
        for s in self.sessions:
            if s.url not in arr:
                arr.append(s.url)
        return tuple(arr[1:])

    def iter_urls(self):
        urls = self.__get_urls()
        for url in urls:
            yield url
        if self.more and self.more not in urls:
            yield self.more

    def _fix_url(self):
        if self.url:
            return self.url
        if get_domain(self.more) == "madrid.es":
            return self.__dict__.pop("more")

    def _fix_name(self):
        if self.name is not None:
            return self.name
        if get_domain(self.url) == "madrid.es":
            title = get_text(WEB.get_cached_soup(self.url).select_one("title"))
            if title and " - " in title:
                return _clean_name(title.split(" - ")[0].strip(), self.place.name)

    def _fix_img(self):
        ko = (None, '') + KO_IMG
        if self.img not in ko:
            return self.img
        for url in self.iter_urls():
            src = self.__get_img_from_url(url)
            if src not in ko:
                return src

    def _fix_category(self):
        dom = get_domain(self.url)
        if self.category == Category.CHILDISH or dom != "madrid.es":
            return self.category
        soup = WEB.get_cached_soup(self.url)
        for txt in map(plain_text, soup.select("div.tramites-content div.tiny-text")):
            if txt is None:
                continue
            if re_or(
                txt,
                "actividad dirigida a familias",
                "para que menores y mayores aprendan",
                "musical? infantil",
                "teatro infantil",
                "concierto familiar",
                "relatos en familia"
            ):
                return Category.CHILDISH
        return self.category

    def __get_img_from_url(self, url: str):
        if url is None:
            return None
        if get_domain(url) == "madrid.es":
            soup = WEB.get_cached_soup(url)
            for src in map(get_img_src, soup.select("div.image-content img, div.tramites-content div.tiny-text img")):
                if src:
                    return src
        if re_filmaffinity.match(url):
            soup = WEB.get_cached_soup(self.more)
            img = get_img_src(soup.select_one("#right-column a.lightbox img"))
            if img:
                return img

    def merge(self, **kwargs):
        return Event(**{**asdict(self), **kwargs})

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        if isinstance(obj['category'], int):
            obj['category'] = Category(obj['category'])
        if isinstance(obj['place'], dict):
            obj['place'] = Place.build(obj['place'])
        if isinstance(obj['sessions'], (list, tuple)) and len(obj['sessions']) > 0 and isinstance(obj['sessions'][0], dict):
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
        if self.category == Category.CINEMA:
            title = re.sub(r"\s*\+\s*Coloquio\s*$", "", self.title, flags=re.IGNORECASE)
            title = re.sub(r"\s*,\s+de\s+[A-ZÁÉÍÓÚÑÜ]+.*$", "", title)
            title = unquote(title)
            WEB.get("https://www.filmaffinity.com/es/search.php?stext="+quote_plus(title))
            if re_filmaffinity.match(WEB.url):
                return WEB.url
            lwtitle = title.lower()
            for a in WEB.soup.select("div.mc-title a"):
                if get_text(a).lower() == lwtitle:
                    return a.attrs["href"]
        urls = self.__get_urls()
        for url in urls:
            dom = get_domain(url)
            if dom == "tienda.madrid-destino.com":
                soup = WEB.get_cached_soup(url)
                href = get_a_href(soup.select_one("a.c-mod-file-event__content-link"))
                if href and href not in urls:
                    return href
            if dom == "madrid.es":
                href = find_more_url_madrides(url)
                if href and href not in urls:
                    return href

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
    def days(self):
        days: Set[date] = set()
        for e in self.sessions:
            dh = e.date.split(" ")
            dt = date(*map(int, dh[0].split("-")))
            days.add(dt)
        return tuple(sorted(days))

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

    def isSimilar(self, e: "Event"):
        ob1 = asdict(e)
        ob2 = asdict(self)
        for k, v1 in ob1.items():
            if v1 is None:
                continue
            v2 = ob2[k]
            if k == "name":
                if plain_text(v1) != plain_text(v2):
                    return False
                continue
            if v1 != v2:
                return False
        return True

    def _asdict(self):
        return asdict(self)

    @staticmethod
    def fusion(*events: "Event"):
        if len(events) == 0:
            raise ValueError("len(events)==0")
        if len(events) == 1:
            return events[0]
        logger.debug("Fusión: " + " + ".join(map(lambda e: f"{e.id} {e.duration}", events)))
        sessions: Set[Session] = set()
        sessions_with_url: Set[Session] = set()
        categories: List[Category] = []
        durations: List[float] = []
        imgs: List[str] = []
        seen_in: Set[str] = set()
        for e in events:
            if e.category not in (None, Category.UNKNOWN):
                categories.append(e.category)
            if e.duration is not None:
                durations.append(e.duration)
            if e.img is not None:
                imgs.append(e.img)
            for s in e.sessions:
                sessions.add(s)
                sessions_with_url.add(s._replace(url=e.url))
            seen_in.add(e.url)
            for u in e.also_in:
                seen_in.add(u)
        seen_in = tuple(sorted((u for u in seen_in if u is not None)))
        url = seen_in[0]
        also_in = seen_in[1:]
        if len(sessions) > 1:
            sessions = sessions_with_url
            url = None
            also_in = tuple()
        return events[0].merge(
            url=url,
            also_in=also_in,
            duration=get_main_value(durations),
            img=get_main_value(imgs),
            category=get_main_value(categories, default=Category.UNKNOWN),
            sessions=tuple(sorted(sessions, key=lambda s: (s.date, s.url))),
        )

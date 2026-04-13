from dataclasses import dataclass, asdict, fields, replace, is_dataclass
from typing import NamedTuple, Tuple, Dict, List, Union, Any, Optional, Set, Callable
from core.util import get_obj, plain_text, get_domain, get_img_src, re_or, re_and, get_main_value
from enum import IntEnum
from functools import cached_property
import re
from datetime import date, datetime
from core.web import WEB
from core.filemanager import FM
import logging
from functools import cache
from core.util import to_uuid, isWorkingHours
from core.dblite import DB
from typing import TypeVar, Type
from core.goodreads import GR
from core.util import my_filter
from core.util.strng import clean_name
from collections import defaultdict
from core.place import Place
import pytz

T = TypeVar("T")

logger = logging.getLogger(__name__)

DT_NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))
TODAY = DT_NOW.today()
NOW = DT_NOW.strftime("%Y-%m-%d")


def _get_fix_event():
    fix_event: Dict[str, Dict[str, Any]] = FM.load("fix/event.json")
    for k, v in list(fix_event.items()):
        if not isinstance(v, dict):
            continue
        for kk, vv in list(v.items()):
            if isinstance(vv, list):
                v[kk] = tuple(vv)
        if set(v.keys()).intersection({"filmaffinity", "imdb"}):
            if "category" not in v:
                v["category"] = "CINEMA"
        fix_event[k] = v
    return fix_event


FIX_EVENT: Dict[str, Dict[str, Any]] = _get_fix_event()

MONTHS = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")

re_filmaffinity = re.compile(r"https://www.filmaffinity.com/es/film\d+.html")


@cache
def safe_expand_url(url: str):
    if not isinstance(url, str):
        return url
    if re_or(
        url,
        r"^https?://\S+/node/\d+$",
        r"^https?://21distritos\.es/.*\bp=\d+.*$",
        r"^https://forms.gle/\w+$",
    ):
        dom = get_domain(url)
        new_dom = {
            "forms.gle": "docs.google.com",
        }.get(dom, dom)
        WEB.get(url)
        if isinstance(WEB.url, str) and get_domain(WEB.url) == new_dom:
            return WEB.url
    return url


def new_dataclass(cls: Type[T], obj: dict) -> T:
    if not is_dataclass(cls):
        raise TypeError(f"{cls} no es un dataclass")
    ks = tuple(f.name for f in fields(cls))
    obj = {k: v for k, v in obj.items() if k in ks}
    return cls(**obj)


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
    OTHERS = 12
    YOUTH = 14
    READING_CLUB = 15
    CONTEST = 16
    SPORT = 17
    POETRY = 18
    ACTIVISM = 19
    SENIORS = 20
    ORGANIZATIONS = 21
    MARGINALIZED = 22
    NON_GENERAL_PUBLIC = 23
    ONLINE = 24
    HIKING = 35 # senderismo
    MAGIC = 36
    VIEW_POINT = 37
    NO_EVENT = 38
    PARTY = 39
    LITERATURE = 40
    MATERNITY = 41
    INSTITUTIONAL_POLICY = 42
    DUPE = 43
    NARRATIVE = 44
    ENTERPRISE = 45
    RELIGION = 46
    PHOTO = 47
    PICTURE = 48
    FULL = 49

    def __str__(self):
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
            return "club de lectura"
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
        if self == Category.MARGINALIZED:
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
        if self == Category.LITERATURE:
            return "literatura"
        if self == Category.NO_EVENT:
            return "no-evento"
        if self == Category.VIEW_POINT:
            return "punto de interés"
        if self == Category.INSTITUTIONAL_POLICY:
            return "política instucional"
        if self == Category.PARTY:
            return "fiesta"
        if self == Category.DUPE:
            return "duplicada"
        if self == Category.MATERNITY:
            return "maternidad"
        return self.name

    def __lt__(self, other):
        if self == Category.UNKNOWN:
            return False
        if other == Category.UNKNOWN:
            return True
        return str(self).__lt__(str(other))


class Session(NamedTuple):
    date: str
    url: Optional[str] = None
    title: Optional[str] = None
    full: Optional[bool] = None
    duration: Optional[int] = None

    def merge(self, **kwargs):
        return self._replace(**kwargs)

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        obj['url'] = safe_expand_url(obj.get('url'))
        return Session(**obj)

    @staticmethod
    def parse_list(obj) -> Optional[Tuple['Session', ...]]:
        if obj is None:
            return None
        if not isinstance(obj, (list, tuple)):
            raise ValueError(obj)
        if len(obj) == 0:
            return tuple()
        if isinstance(obj[0], Session):
            return tuple(obj)
        if isinstance(obj[0], dict):
            return tuple(map(Session.build, obj))
        raise ValueError(obj)

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
        return isWorkingHours(dt)

    def get_date(self):
        dt_int = tuple(map(int, re.split(r"\D+", self.date)))
        return datetime(*dt_int)


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
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/Cine_260x260.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/DistritoRetiro/FICHEROS/FICHEROS%20ACTIVIDADES%20JUNIO/CineVeranoRetiro25-001.jpg',
    'https://entradasfilmoteca.gob.es//Contenido/ImagenesEspectaculos/00_5077/Jazz%20On%20A%20Summer',
    'https://www.madrid.es/UnidadesDescentralizadas/MuseosMunicipales/DepartamentoExposiciones/Actividades/Ciclo%20Cine%20Una%20tarde%20con%20%20Marilyn/Cartel%20Marilyn%20jpg.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/DistritoRetiro/FICHEROS/FICHEROS%20ACTIVIDADES%20ENERO/18%20enero%20%20CONCIERTO%20Ra%C3%ADzes-001.jpg',
    'https://www.madrid.es/UnidadWeb/UGBBDD/EntidadesYOrganismos/CulturaYOcio/InstalacionesCulturales/CentrosCulturalesMunicipales/CCVillaverde/Ficheros/CentroSocioCult.jpg',
    'https://cdn.tenemosplan.com/tenemosplan/default_image.jpg',
    'https://www.goethe.de/resources/files/jpg1436/clad-event-02-1000x1000-formatkey-jpg-w320r.jpg',
)


@dataclass(frozen=True)
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
    also_in: Tuple[str, ...] = tuple()
    sessions: Tuple[Session, ...] = tuple()
    cycle: Optional[str] = None
    more: Optional[str] = None

    def __lt__(self, other):
        if not isinstance(other, Event):
            return NotImplemented
        flds = fields(Event)
        a = asdict(self)
        b = asdict(other)
        tp_a = tuple(a[f.name] for f in flds)
        tp_b = tuple(b[f.name] for f in flds)
        return tp_a < tp_b

    def fix_type(self):
        if self.category == Category.CINEMA:
            return new_dataclass(Cinema, self._asdict())
        return new_dataclass(Event, self._asdict())

    def __post_init__(self):
        plc = self.place
        if isinstance(plc, dict):
            plc = Place.build(plc)
        if isinstance(plc, Place):
            plc = plc.normalize()
        object.__setattr__(self, 'place', plc)
        new_name = clean_name(self.name)
        if new_name != self.name:
            logger.debug(f"[{self.id}].__post_init__ name={new_name} <- {self.name}")
            object.__setattr__(self, 'name', new_name)
        fix_event = FIX_EVENT.get(self.id, {})
        for f in fields(self):
            old_val = getattr(self, f.name, None)
            v = fix_event.get(f.name) or old_val
            if f.name == "sessions":
                v = Session.parse_list(v)
            if f.name == "place" and isinstance(v, dict):
                v = Place(**v).normalize()
            if isinstance(v, list):
                v = tuple(v)
            elif isinstance(v, str):
                v = v.strip()
                if len(v) == 0:
                    v = None
            if f.name == "more" and v == self.url:
                continue
            if f.name == "url" and v == self.more:
                continue
            if f.name == "category" and isinstance(v, str):
                v = Category[v]
            if f.name == "price" and isinstance(v, float) and int(v) == v:
                v = int(v)
            if v != old_val or (type(v) is not type(old_val)):
                if v != old_val:
                    logger.debug(f"[{self.id}].__post_init__ {f.name}={v} <- {old_val}")
                object.__setattr__(self, f.name, v)

    def fix(self, **kwargs):
        for k, v in kwargs.items():
            if v is not None:
                object.__setattr__(self, k, v)
        self.__fix()
        nil = []
        if self.name is None:
            nil.append("name")
        if nil:
            raise ValueError(f"[{self.id}] Missing required fields: {', '.join(nil)}")
        return self

    def __fix(self):
        MAIN_DOM = ("condeduquemadrid.es", "teatroespanol.es")
        doit = True
        while doit:
            doit = False
            for f in fields(self):
                if self._fix_field(f.name):
                    doit = True
            if self.url is not None and self.url == self.more:
                logger.debug(f"[{self.id}].__fix: more=None <- more=url={self.url}")
                object.__setattr__(self, "more", None)
                doit = True
            if self.url is None and get_domain(self.more) in (MAIN_DOM+("madrid.es", )):
                logger.debug(f"[{self.id}].__fix: more=None url={self.more}")
                object.__setattr__(self, "url", self.more)
                object.__setattr__(self, "more", None)
                doit = True
            if get_domain(self.url) in ("madrid.es", ) and get_domain(self.more) in MAIN_DOM:
                logger.debug(f"[{self.id}].__fix: more={self.url} url={self.more}")
                a, b = self.more, self.url
                object.__setattr__(self, "url", a)
                object.__setattr__(self, "more", b)
                doit = True
            also_in = tuple(u for u in self.also_in if u not in (None, self.url, self.more))
            if also_in != self.also_in:
                object.__setattr__(self, "also_in", also_in)
                doit = True
            s_changed = False
            sessions = list(self.sessions)
            for i, s in enumerate(sessions):
                s_id = f"{self.id}_{s.date}"
                url = FIX_EVENT.get(s_id)
                if url is not None and s.url is None:
                    logger.debug(f"[{self.id}].__fix: sessions {s_id} url = {url}")
                    sessions[i] = s.merge(url=url)
                    s_changed = True
            if s_changed:
                object.__setattr__(self, "sessions", tuple(sessions))
                doit = True

    def _fix_field(self, name: str, fnc=None):
        isUrl = name in ('more', 'url')
        fix_event = FIX_EVENT.get(self.id, {})
        old_val = getattr(self, name, None)
        fix_val = None
        if name in fix_event:
            fix_val = fix_event[name]
        else:
            if fnc is None:
                fnc = getattr(self, f'_fix_{name}', None)
            if fnc is not None and callable(fnc):
                fix_val = fnc()
            elif not isUrl:
                return False
        if isUrl:
            fix_val = safe_expand_url(fix_val or old_val)
        if name == "sessions":
            fix_val = Session.parse_list(fix_val)
        if name == "category" and isinstance(fix_val, str):
            fix_val = Category[fix_val]
        if name == "place" and isinstance(fix_val, dict):
            fix_val = Place(**fix_val).normalize()
        if fix_val == old_val:
            return False
        if name == "more" and fix_val == self.url:
            return False
        if name == "url" and fix_val == self.more:
            return False
        if fix_val == fix_event.get(name):
            logger.debug(f"FIX_EVENT: {name}={fix_val} <- {old_val}")
        else:
            logger.debug(f"FIX._fix_field: {name}={fix_val} <- {old_val}")
        object.__setattr__(self, name, fix_val)
        return True

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

    @cached_property
    def sites(self):
        dom: list[str] = [None]
        for d in map(get_domain, self.iter_urls()):
            if d not in dom:
                dom.append(d)
        return tuple(dom[1:])

    def _fix_img(self):
        ko = (None, '') + KO_IMG
        if self.img not in ko:
            return self.img
        for url in self.iter_urls():
            src = self._get_img_from_url(url)
            if src not in ko:
                return src

    def _fix_category(self):
        if self.category == Category.CHILDISH:
            return self.category
        if self.category == Category.CONFERENCE and get_domain(self.more) == "goodreads.com":
            return Category.LITERATURE
        return self.category

    def _get_img_from_url(self, url: str):
        if url is None:
            return None

    def merge(self, **kwargs):
        return replace(self, **kwargs)

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
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
        if obj["category"] == Category.CINEMA:
            return new_dataclass(Cinema, obj)
        return new_dataclass(Event, obj)

    def _fix_more(self):
        if self.more:
            return self.more
        if self.category in (
            Category.READING_CLUB,
            Category.CONFERENCE,
        ):
            more = {
                "el antiedipo": "https://gestiona.comunidad.madrid/biblio_publicas/cgi-bin/abnetopac?TITN=267016"
            }.get(plain_text(self.name).lower())
            if more:
                return more
        dom = get_domain(self.url)
        if self.category in {
            "madrid.es": (Category.CONFERENCE, Category.LITERATURE),
            "ateneodemadrid.com": (Category.LITERATURE, )
        }.get(dom, tuple()):
            books = GR.find(self.name)
            if books:
                return books[0].url

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

    def remove_working_sessions(self, to_log: bool = True):
        sessions = []
        w = 'LMXJVSD'
        for s in self.sessions:
            if s.isWorkingHours():
                d = s.get_date()
                if to_log:
                    logger.debug(f"[{self.id}] Sesión {s.date} {w[d.weekday()]} eliminada por estar en horario de trabajo. {s.url or self.url}")
                continue
            sessions.append(s)
        object.__setattr__(self, 'sessions', tuple(sessions))

    def remove_ko_sessions(
        self,
        isOkDate: Callable[[datetime], bool],
        to_log: bool = True
    ):
        sessions = []
        w = 'LMXJVSD'
        for s in self.sessions:
            d = s.get_date()
            if isOkDate(d):
                sessions.append(s)
                continue
            if to_log:
                logger.debug(f"[{self.id}] Sesión {s.date} {w[d.weekday()]} eliminada por estar fuera de horario. {s.url or self.url}")
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
    def fusionIfSimilar(
        all_events: tuple["Event", ...],
        keys: tuple[str, ...]
    ) -> tuple["Event", ...]:
        if len(all_events) == 0:
            return tuple()

        empty = {k: None for k in list(all_events)[0]._asdict().keys()}

        mrg_events: set[Event] = set()
        ko_events: list[Event] = sorted(all_events)

        while ko_events:
            e = ko_events[0]
            obj = {k: v for k, v in e._asdict().items() if k in keys}
            k: Event = Event.build({
                **empty,
                **obj
            })
            ok, ko_events = my_filter(ko_events, lambda x: x.isSimilar(k))
            if len(ok):
                mrg_events.add(Event.fusion(*ok))
            else:
                logger.warning(f"fusionIfSimilar: resutado inesperado {e} ~ {k}")
                ko_events = [x for x in ko_events if x != e]
                mrg_events.add(e)
        return tuple(sorted(mrg_events))

    @staticmethod
    def fusion(*evs: "Event", name: str = None, id: str = None, url: str = None, also_in: tuple[str, ...] = None):
        if len(evs) == 0:
            raise ValueError("len(events)==0")
        if len(evs) == 1:
            return evs[0]
        logger.debug("Fusión: " + " + ".join(map(lambda e: e.id, evs)))
        logger.debug("Fusión: " + " + ".join(map(str, evs)))
        f_info = _get_info_fusion(evs)
        if len(set(f_info.names)) == 1:
            name = f_info.names[0]
        elif name is None:
            name = get_main_value(f_info.names)
        sessions: list[Session] = []
        for d in f_info.dates:
            f_d = f_info.sessions[d]
            s_url = get_main_value(
                [u for u in f_d.url_session if u != url]
            ) or get_main_value(
                [u for u in f_d.url_event if u != url]
            )
            title = f_info.url_title.get(s_url)
            if title == name:
                title = None
            sessions.append(Session(
                date=d,
                url=s_url,
                title=title,
                full=f_d.full
            ))
        ss_url = set(s.url for s in sessions if s.url is not None)
        if len(sessions) > 1 and len(ss_url) == 1:
            s_url = ss_url.pop()
            if url is None:
                url = s_url
            if s_url == url:
                sessions = [s._replace(title=None, url=None) for s in sessions]
        if url is None:
            url = get_main_value(u for u in f_info.seen_in if u not in ss_url)

        category = get_main_value(f_info.categories, default=Category.UNKNOWN)
        no_more = category in (Category.CINEMA, )
        more = None
        st_more = set(f_info.mores)
        if len(st_more) == 1:
            more = st_more.pop()
            no_more = False
        elif not no_more:
            more = get_main_value(f_info.mores)
        if also_in is None:
            st_also_in = set(f_info.seen_in)
            st_also_in.discard(url)
            st_also_in.discard(more)
            for s in sessions:
                st_also_in.discard(s.url)
            if more is None and len(st_also_in) == 1 and not no_more:
                more = st_also_in.pop()
            also_in = tuple(sorted(st_also_in))
        if id is None:
            ids = set(e.id for e in evs)
            if len(ids) == 1:
                id = ids.pop()
            else:
                id = to_uuid("".join(sorted(ids)))
        e = evs[0].merge(
            id=id,
            url=url,
            more=more,
            name=name,
            also_in=also_in,
            duration=get_main_value(f_info.durations),
            img=get_main_value(f_info.imgs),
            category=category,
            sessions=tuple(sessions),
            price=max(f_info.prices),
        )
        e = e.fix()
        logger.debug(f"=== {e}")
        return e

    def _fix_cycle(self):
        if self.cycle:
            return self.cycle
        urls = set(self.iter_urls())
        name = self.name or ''
        if re.search(r"Festival Centro al comp[áa]s", name, flags=re.I):
            return "Festival Centro al compás"
        if re.search(r"Festival L[ií]rica al margen", name, flags=re.I):
            return "Festival Lírica al margen"
        if re.search(r"Charlas de astronomía para profanos", name):
            return "Charlas de astronomía para profanos"
        if re.search(r"^Derechos [dD]igitales: ", name):
            return "Derechos digitales"
        if re.search(r"^Nuevos [Ii]maginarios: ", name):
            return "Nuevos imaginarios"
        if re.search(r"^Los artesanos de la tumba", name):
            return "Los artesanos de la tumba"
        if re.search(r"^las mujeres escritoras de", name, flags=re.I):
            return "Las mujeres escritoras de…"
        if self.category == Category.THEATER and self.place.name == "Sala Berlanga":
            return "Teatro en la Berlanga"
        if self.category == Category.DANCE and self.place.name == "Sala Berlanga":
            return "Bailar en la Berlanga"
        #if re.search(r"\s*\-\s*Teatro en la [Bb]erlanga$", name):
        #    return "Teatro en la Berlanga"
        m = re.match(r"^(Interautor 20\d+)\b.*", name)
        if m:
            #if self.category == Category.THEATER and self.place.name == "Sala Berlanga":
            #    return "Teatro en la Berlanga"
            return m.group(1)
        if self.category == Category.CINEMA and self.place.name == "Cineteca":
            if re.search(r"^(Esc[áa]ner|Mrgente|Sesi[oó]n) \d+$", name, flags=re.I) or re.search("Stop Motion exquisito|Alzo mi voz.*realidades animadas", name, flags=re.I):
                return "Cortometrajes"
        if self.category == Category.CINEMA:
            if re.search(r"SGAE en corto", name, flags=re.I):
                return "Cortometrajes"
        if re.search(r"cat[áa]logo.*Madrid entre libros", self.name, flags=re.I):
            return "Madrid entre libros"
        if self.category == Category.VISIT and re_and(self.name, "ruta", "retiro", flags=re.I):
            return "Rutas por el Retiro"
        if self.category == Category.CONFERENCE and re_or(self.name, "Ciclo conferencias Maqueta León Gil de Palacio", flags=re.I):
            return "Maqueta León Gil de Palacio"
        if urls.intersection((
            "https://www.centrocentro.org/musica/limo-2026",
            "https://www.centrocentro.org/musica/kali-malone",
            "https://www.centrocentro.org/musica/arianna-casellas-y-kaue",
            "https://www.centrocentro.org/musica/ustad-noor-bakhsh",
            "https://www.centrocentro.org/musica/lucrecia-dalt",
            "https://www.centrocentro.org/musica/senyawa",
            "https://www.centrocentro.org/musica/lise-barkas"
        )):
            return "Musica corriente"
        if urls.intersection((
            "https://www.centrocentro.org/musica/vang-VIII-musicas-en-vanguardia",
            "https://www.centrocentro.org/musica/jurg-frey-y-phill-niblock-cuartetos-de-cuerda",
            "https://www.centrocentro.org/musica/vacio-musica-de-ustvolskaya-y-feldman-en-dialogo-con-musica-barroca",
            "https://www.centrocentro.org/musica/maryanne-amacher-plaything"
        )):
            return "Música de vanguardía"
        if urls.intersection((
            "https://www.centrocentro.org/musica/sinetiq-2026",
            "https://www.centrocentro.org/musica/raul-rodriguez-3f-power-trio",
            "https://www.centrocentro.org/musica/zaruk-iris-azquinecer-rainer-seiferth",
            "https://www.centrocentro.org/musica/antonio-serrano-kaele-jimenez",
            "https://www.centrocentro.org/musica/javier-ruibal",
            "https://www.centrocentro.org/musica/maria-toro-en-cuarteto",
            "https://www.centrocentro.org/musica/feten-feten"
        )):
            return "Música sin etiquetas"
        return None


@dataclass(frozen=True)
class Cinema(Event):
    year: int = None
    director: tuple[str, ...] = tuple()
    aka: tuple[str, ...] = tuple()
    imdb: Optional[str] = None
    filmaffinity: Optional[int] = None

    def fix(self, **kwargs):
        self._fix_field('cycle')
        self._fix_field('year')
        self._fix_name_director()
        self._fix_name_year()
        self._fix_field('imdb', self.__find_imdb)
        self._fix_field('filmaffinity')
        super().fix(**kwargs)
        return self

    def _fix_name_director(self):
        def _mk_re(dr: str):
            return re.compile(r"\s*,?\s*\bde\s+"+re.escape(dr)+"$", flags=re.I)

        if self.director:
            if len(self.director) == 1:
                new_name = _mk_re(self.director[0]).sub("", self.name).strip()
                if new_name and new_name != self.name:
                    object.__setattr__(self, "name", new_name)
            return
        for d in (
            'James Ward Byrkit',
            'Angela Schanelec',
            'Stephen Daldry',
            'Woody Allen',
            'Albert Serra'
        ):
            new_name = _mk_re(d).sub("", self.name).strip()
            if new_name and new_name != self.name:
                new_name = clean_name(new_name)
                logger.debug(f"[{self.id}].__fix_name_director: director={d} name={new_name} <- {self.name}")
                object.__setattr__(self, "director", (d, ))
                object.__setattr__(self, "name", new_name)
                return

    def _fix_name_year(self):
        m = re.match(r"^(.*?)\s*\(\s*(\d+)\s*\)\s*$", self.name)
        if m is None:
            return
        year = int(m.group(2))
        if year > 1900 and year <= (TODAY.year + 1) and (self.year is None or self.year == year):
            new_name = clean_name(m.group(1))
            logger.debug(f"[{self.id}]._fix_name_year: year={year} name={new_name} <- {self.name}")
            object.__setattr__(self, "year", year)
            object.__setattr__(self, "name", new_name)
        
    def _fix_year(self):
        if self.year is not None:
            return self.year
        if self.imdb:
            return DB.one("select year from MOVIE where id = ?", self.imdb)

    def get_full_aka(self):
        aka = [self.name]
        for t in (self.aka or []):
            if t not in aka:
                aka.append(t)
        m = re.match(r"^([^\(\)]+) \(([^\(\)\d]+)\)$", self.name)
        if m:
            for t in m.groups():
                if t not in aka:
                    aka.append(t)
        return tuple(aka)

    def __find_imdb(self):
        if isinstance(self.cycle, str):
            return None
        for t in self.get_full_aka():
            imdb = DB.search_imdb_id(
                t,
                year=self.year,
                director=self.director,
                duration=self.duration
            )
            if imdb:
                return imdb

    def _fix_filmaffinity(self) -> int:
        if self.filmaffinity is not None:
            return self.filmaffinity
        if self.imdb is not None:
            _id_ = DB.one("select filmaffinity from EXTRA where movie = ?", self.imdb)
            if _id_:
                return _id_

    def _fix_cycle(self):
        if isinstance(self.cycle, str):
            return self.cycle
        if re.search(r"\b(cortometrajes?)\b", self.name, flags=re.I):
            return "Cortometrajes"
        if re.search(r"\bCortos (nacionales|internacionales|disidentes)\b", self.name, flags=re.I):
            return "Cortometrajes"
        if re.search(r"Juventud líquida.*Sesión \d+", self.name, flags=re.I):
            return "Juventud líquida"
        if re.search(r"Futuros raros.*Sesión \d+", self.name, flags=re.I):
            return "Futuros raros"

        return super()._fix_cycle()

    def _fix_more(self):
        fix_more = FIX_EVENT.get(self.id, {}).get("more")
        if fix_more not in (None, self.url):
            return fix_more
        if self.filmaffinity:
            return f"https://www.filmaffinity.com/es/film{self.filmaffinity}.html"
        if self.imdb:
            return f"https://www.imdb.com/es-es/title/{self.imdb}"
        if self.more:
            return self.more
        return super()._fix_more()

    def _fix_duration(self):
        imdb_duration = None
        if self.imdb:
            imdb_duration = DB.one("select duration from MOVIE where id = ?", self.imdb)
        if imdb_duration is not None and (self.duration or 0) < imdb_duration:
            return imdb_duration
        return self.duration

    def _get_img_from_url(self, url: str):
        img = super()._get_img_from_url(url)
        if img is not None:
            return img
        if self.filmaffinity is not None:
            url = f"https://www.filmaffinity.com/es/film{self.filmaffinity}.html"
            soup = WEB.get_cached_soup(url)
            img = get_img_src(soup.select_one("#right-column a.lightbox img"))
            if img:
                return img
        if self.imdb is not None:
            url = f"https://www.imdb.com/title/{self.imdb}/"
            soup = WEB.get_cached_soup(url)
            img = get_img_src(soup.select_one("div.ipc-media img"))
            if img:
                return img


class FusionSession(NamedTuple):
    url_event: tuple[str]
    url_session: tuple[str]
    full: bool


class FusionInfo(NamedTuple):
    urls: list[str]
    names: list[str]
    url_title: dict[str, str]
    categories: list[Category]
    durations: list[float]
    imgs: list[str]
    mores: list[str]
    seen_in: list[str]
    sessions: dict[str, FusionSession]
    prices: list[float]
    dates: tuple[str]


def _get_info_fusion(evs: tuple[Event, ...]):
    def _add(arr: list, v, avoid=(None, )):
        if v not in avoid:
            arr.append(v)
    s_event_url: dict[str, list[str]] = defaultdict(list)
    s_sessi_url: dict[str, list[str]] = defaultdict(list)
    date_with_url: Set[str] = set()
    date_full: Set[str] = set()
    url_title: dict[str, str] = dict()
    names: list[str] = []
    categories: List[Category] = []
    durations: List[float] = []
    imgs: List[str] = []
    urls: List[str] = []
    mores: List[str] = []
    seen_in: list[str] = []
    s_dates: set[str] = set()
    prices: list[float] = []
    for e in evs:
        _add(urls, e.url)
        _add(names, e.name)
        _add(categories, e.category, avoid=(None, Category.UNKNOWN))
        _add(mores, e.more)
        _add(durations, e.duration)
        _add(imgs, e.img)
        _add(prices, e.price)
        _add(seen_in, e.url)
        for u in e.also_in:
            _add(seen_in, u)
        if e.name and e.url and e.url not in url_title:
            url_title[e.url] = e.name
        for s in e.sessions:
            if s.title and s.url and s.url not in url_title:
                url_title[s.url] = s.title
            if s.url is not None:
                date_with_url.add(s.date)
    for e in evs:
        for s in e.sessions:
            if s.url is None and s.date in date_with_url:
                continue
            s_dates.add(s.date)
            if s.full is True:
                date_full.add(s.date)
            if e.url:
                s_event_url[s.date].append(e.url)
            if s.url:
                s_sessi_url[s.date].append(s.url)

    for e in evs:
        if e.name:
            for s in e.sessions:
                if s.url and s.url not in url_title:
                    url_title[s.url] = e.name

    ts_dates = tuple(sorted(s_dates))
    sessions: dict[str, FusionSession] = {}
    for d in ts_dates:
        sessions[d] = FusionSession(
            url_event=tuple(s_event_url.get(d, [])),
            url_session=tuple(s_sessi_url.get(d, [])),
            full=d in date_full
        )
    return FusionInfo(
        urls=urls,
        names=names,
        url_title=url_title,
        categories=categories,
        durations=durations,
        imgs=imgs,
        mores=mores,
        seen_in=seen_in,
        sessions=sessions,
        prices=prices,
        dates=ts_dates
    )


def find_book_category(name: str, description: str, default: Category):
    txt = f"{name or ''}\n{description or ''}".strip()
    if re_or(
        txt,
        r"novela gr[aá]fica",
        r"comic",
        r"tebeo",
        flags=re.I
    ):
        return default
    if re_or(
        description,
        r"En estos versos el autor",
        r"Presentaci[oó]n del poemario",
        r"recital de poes[íi]a",
        r"presenta su poemario",
        r"presentan? este poemario de",
        r"poemas in[eé]ditos",
        r"libros? de poes[ií]a",
        flags=re.I
    ):
        return Category.POETRY
    if re_or(
        description,
        r"(La|Esta) novela (relata|retrata|presenta|publicada)",
        r"(La|Esta) nueva novela de",
        r"(La|Esta) novela es la cr[oó]nica",
        r"A partir de ese momento comienza una aventura",
        r"una novela (de aventuras|sobre|breve)",
        r"novela hist[oó]rica",
        r"presenta su primera novela",
        r"Presentaci[oó]n de la novela editada",
        r"El retrato de Dorian Gray",
        r"libro de cuentos",
        r"una de las novelas m[áa]s conocidas",
        ("Madrid junto al mar", "Mar Garc[íi]a Lozano"),
        ("a trav[eé]s de estas ficciones", "literatura"),
        flags=re.I
    ):
        return Category.NARRATIVE
    if re_or(
        name,
        "Presentaci[óo]n de la novela",
        flags=re.I
    ):
        return Category.NARRATIVE
    return default

from dataclasses import dataclass, asdict, fields, replace, is_dataclass
from typing import NamedTuple, Tuple, Dict, List, Union, Any, Optional, Set
from core.util import get_obj, plain_text, get_domain, get_img_src, re_or, re_and, get_main_value
from portal.util.madrides import find_more_url as find_more_url_madrides
from urllib.parse import quote
from enum import IntEnum
from functools import cached_property
import re
from datetime import date, datetime
from core.web import get_text, WEB
from core.filemanager import FM
import logging
from functools import cache
from core.util import to_uuid
from core.dblite import DB
from typing import TypeVar, Type
from core.goodreads import GR
from core.zone import Zones
from enum import Enum
from core.util import my_filter

T = TypeVar("T")

logger = logging.getLogger(__name__)

TODAY = date.today()
NOW = TODAY.strftime("%Y-%m-%d")
FIX_EVENT: Dict[str, Dict[str, Any]] = FM.load("fix/event.json")
for k, v in list(FIX_EVENT.items()):
    if isinstance(v, dict):
        for kk, vv in list(v.items()):
            if isinstance(vv, list):
                v[kk] = tuple(vv)
        FIX_EVENT[k] = v

MONTHS = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")

re_filmaffinity = re.compile(r"https://www.filmaffinity.com/es/film\d+.html")


@cache
def safe_expand_url(url: str):
    if not isinstance(url, str):
        return url
    if re.match(r"^https?://\S+/node/\d+$", url):
        dom = get_domain(url)
        WEB.get(url)
        if isinstance(WEB.url, str) and get_domain(WEB.url) == dom:
            return WEB.url
    return url


def new_dataclass(cls: Type[T], obj: dict) -> T:
    if not is_dataclass(cls):
        raise TypeError(f"{cls} no es un dataclass")
    ks = tuple(f.name for f in fields(cls))
    obj = {k: v for k, v in obj.items() if k in ks}
    return cls(**obj)


@cache
def get_festivos(year: int):
    dates: set[str] = set()
    soup = WEB.get_cached_soup(f"https://www.calendarioslaborales.com/calendario-laboral-madrid-{year}.htm")
    for month, div in enumerate(soup.select("#wrapIntoMeses div.mes")):
        for day in map(get_text, div.select("td[class^='cajaFestivo']")):
            dt = date(year, month+1, int(day))
            dates.add(dt)
    return tuple(sorted(dates))


def isWorkingHours(dt: datetime):
    if dt is None:
        return False
    hm = dt.hour + (dt.minute/100)
    if hm == 0 or hm > 15:
        return False
    if dt.weekday() in (5, 6):
        return False
    if dt.date() in get_festivos(dt.year):
        return False
    return True


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
        raise ValueError(self.value)

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


def safe_lt(a: str | None, b: str | None):
    if (a, b) == (None, None):
        return None
    if a is None and b is not None:
        return True
    if a is not None and b is None:
        return False
    if a.__eq__(b):
        return None
    return a.__lt__(b)


@dataclass(frozen=True)
class Place:
    name: str
    address: str
    latlon: str = None
    zone: str = None

    def _asdict(self):
        return asdict(self)

    def __lt__(self, o):
        if not isinstance(o, Place):
            return NotImplemented
        for lt in (
            safe_lt(self.zone, o.zone),
            safe_lt(self.name, o.name),
            safe_lt(self.address, o.address),
            safe_lt(self.latlon, o.latlon),
        ):
            if lt is not None:
                return lt
        return False

    @classmethod
    def build(cls, *args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        ks = set(f.name for f in fields(cls))
        obj = {k: v for k, v in obj.items() if k in ks}
        return Place(**obj)

    def __post_init__(self):
        for f in fields(self):
            v = getattr(self, f.name, None)
            if isinstance(v, list):
                v = tuple(v)
            elif isinstance(v, str):
                v = v.strip()
                if len(v) == 0 and f.name != 'zone':
                    v = None
            object.__setattr__(self, f.name, v)
        self.__fix()

    def __fix(self):
        doit = True
        while doit:
            doit = False
            for f in fields(self):
                if self._fix_field(f.name):
                    doit = True

    def _fix_field(self, name: str, fnc=None):
        old_val = getattr(self, name, None)
        if fnc is None:
            fnc = getattr(self, f'_fix_{name}', None)
        if fnc is None or not callable(fnc):
            return False
        fix_val = fnc()
        if fix_val == old_val:
            return False
        logger.debug(f"Place._fix_field: {name}={fix_val} <- {old_val}")
        object.__setattr__(self, name, fix_val)
        return True

    @property
    def url(self):
        if self.latlon is not None:
            return "https://www.google.com/maps?q=" + self.latlon
        if self.address is None:
            return "#"
        if re.match(r"^[\d\.,]+$", self.address):
            return "https://www.google.com/maps?q=" + self.address
        return "https://www.google.com/maps/place/" + quote(self.address)

    def _fix_zone(self):
        if self.zone is not None:
            return self.zone
        name = plain_text(self.name) or ''
        addr = plain_text(self.address) or ''
        if re_or(name, r"d?el retiro", ("biblioteca", "eugenio trias"), "casa de vacas"):
            return "El Retiro"
        if re_or(name, r"jardin(es)? del?\b.*\bretiro\b", flags=re.I):
            return "El Retiro"
        if re_or(name, r"Parque\b.*\bEnrique Tierno Galv[aá]n", flags=re.I):
            return "Legazpi"
        if re_or(name, "matadero", "cineteca", "Casa del Reloj", "Nave Terneras", "La Lonja", flags=re.I):
            return "Legazpi"
        if re_and(addr, "conde duque", "28015"):
            return "Conde Duque y alrededores"
        if re_or(name, "clara del rey"):
            return "Conde Duque y alrededores"
        if self.latlon:
            lat, lon = map(float, self.latlon.split(","))
            for zn in (
                Zones.CARABANCHEL,
                Zones.VILLAVERDE_BAJO,
                Zones.PACIFICO,
                Zones.TRIBUNAL,
                Zones.MONCLOA,
                Zones.SOL,
                Zones.PUERTA_TOLEDO,
                Zones.LAVAPIES,
                Zones.LEGAZPI,
                Zones.MARQUES_DE_VADILLO,
                Zones.USERA,
                Zones.VALLECAS,
            ):
                z = zn.value
                if z.is_in(lat, lon):
                    return z.name
        return None

    def _fix_latlon(self):
        if self.latlon:
            return self.latlon
        if re_or(self.address, "Sierra (de )?Alquife,? 12", flags=re.I):
            return "40.38888553445172,-3.66665737114293"

    def normalize(self):
        name = self.name or ''
        address = self.address or ''
        if re.match(r"^Faro de (la )?Moncloa$", name, flags=re.I):
            return Places.FARO_MONCLOA.value
        if re.match(r"^Conde Duque$", name, flags=re.I):
            return Places.CONDE_DUQUE.value
        if re.match(r"^Sala Berlanga$", name, flags=re.I) and re.search(r"Andr[ée]s Mellado.*53", address, flags=re.I):
            return Places.SALA_BERLANGA.value
        if re.match(r"^Teatro Español$", name, flags=re.I):
            return Places.TEATRO_ESPANOL.value
        if re.match(r"^Teatro Circo Price$", name, flags=re.I):
            return Places.TEATRO_PRICE.value
        if re.match(r"(^Centro\s*Centro$|.*\bPalacio de Cibeles\b.*)", name, flags=re.I):
            return Places.CENTRO_CENTRO.value
        if re.search("cineteca", name, flags=re.I) and (self.latlon == Places.CINETECA.value.latlon or re_or(self.address, "Legazpi", flags=re.I)):
            return Places.CINETECA.value
        if re.search(r"\bESLA EKO\b", name, flags=re.I):
            return Places.EKO.value
        if re.search(r"Fundaci[óo]n Anselmo Lorenzo", name, flags=re.I):
            return Places.FUNDACION_ALSELMO_LORENZO.value
        if re.search(r"auditorio francisca (martinez|Mtnez\.?) garrido", name, flags=re.I):
            return Places.AUDITORIO_FRANCISCA_MARTINEZ_GARRIDO.value
        if re.search(r"\b(CS la cheli|local de xr madrid)\b", name, flags=re.I):
            return Places.CS_LA_CHELI.value
        if re.search(r"CS[ROA]* [dD]is[ck]ordia", name) and re.search(r"Antoñita Jiménez", self.address, flags=re.I):
            return Places.CSO_DISKORDIA.value
        for plc in Places:
            p = plc.value
            if (p.name, p.address) == (self.name, self.address):
                return p
            if (p.name, p.latlon) == (self.name, self.latlon):
                return p
        return self


class Places(Enum):
    ACADEMIA_CINE = Place(
        name="Academia de cine",
        address="C/ de Zurbano, 3, Chamberí, 28010 Madrid",
        latlon="40.427566448169316,-3.6939387798888634",
        zone='Moncloa'
    )
    CAIXA_FORUM = Place(
        name="Caixa Forum",
        address="Paseo del Prado, 36, Centro, 28014 Madrid",
        latlon="40.41134208472603,-3.6935713500263523",
        zone='Paseo del Pardo'
    )
    CASA_AMERICA = Place(
        name="La casa America",
        address="Plaza Cibeles, s/n, Salamanca, 28014 Madrid",
        latlon="40.419580635299525,-3.693332407512017",
        zone='Paseo del Pardo'
    )
    CASA_ENCENDIDA = Place(
        name="La casa encendida",
        address="Rda. de Valencia, 2, Centro, 28012 Madrid",
        latlon="40.4062337055155,-3.6999346068731525",
        zone='Lavapies'
    )
    CIRCULO_BELLAS_ARTES = Place(
        name="Circulo de Bellas Artes",
        address="C/ Alcalá, 42, Centro, 28014 Madrid, España",
        latlon="40.4183042,-3.6991136",
        zone='Sol'
    )
    DORE = Place(
        name="Cine Doré",
        address="C/ de Santa Isabel, 3, Centro, 28012 Madrid",
        latlon="40.411950735826316,-3.699066276358703",
        zone='Sol'
    )
    SALA_BERLANGA = Place(
        name="Sala Berlanga",
        address="C/ de Andrés Mellado, 53, Chamberí, 28015 Madrid",
        latlon="40.436106653741795,-3.714403054648641",
        zone='Moncloa'
    )
    SALA_EQUIS = Place(
        name="Sala Equis",
        address="C/ del Duque de Alba, 4, Centro, 28012 Madrid, España",
        latlon="40.412126715926796,-3.7059047815506396",
        zone='Sol'
    )
    FUNDACION_TELEFONICA = Place(
        name="Fundación Telefónica",
        address="C/ Fuencarral, 3, Centro, 28004 Madrid",
        latlon="40.42058956643586,-3.7017498812379235",
        zone='Sol'
    )
    TEATRO_ESPANOL = Place(
        name="Teatro Español",
        address="C/ del Príncipe, 25, Centro, 28012 Madrid",
        latlon="40.414828532240946,-3.700164949543688",
        zone='Sol'
    )
    TEATRO_PRICE = Place(
        name="Teatro Circo Price",
        address="Ronda de Atocha, 35. 28012 Madrid",
        latlon="40.40596936645757,-3.698589986849812",
        zone='Lavapies'
    )
    CENTRO_CENTRO = Place(
        name="Centro Centro",
        address="Pl. Cibeles, 1, Retiro, 28014 Madrid",
        latlon="40.41902261618159,-3.692188193693138",
        zone='Paseo del Pardo'
    )
    CINETECA = Place(
        name="Cineteca",
        address="Pl. de Legazpi, 8, Arganzuela, 28045 Madrid",
        latlon="40.39130985242181,-3.6958028442054074",
        zone='Legazpi'
    )
    CONDE_DUQUE = Place(
        name="Conde Duque",
        address="C/ del Conde Duque, 11, 28015 Madrid",
        latlon="40.42739911262292,-3.710589286287491"
    )
    FARO_MONCLOA = Place(
        name="Faro de Moncloa",
        address="Av. de la Memoria, 2, 28040 Madrid",
        latlon="40.43727075977316,-3.721682694006853",
        zone='Moncloa'
    )
    TEATRO_MONUMENTAL = Place(
        name="Teatro Monumental",
        address="C. de Atocha, 65, Centro, 28012 Madrid",
        zone='Sol'
    )
    EKO = Place(
        name="CSO EKO",
        address="C. del Ánade, 10, Carabanchel, 28019 Madrid",
        latlon="40.391899629090574,-3.7310781522792906",
    )
    FUNDACION_ALSELMO_LORENZO = Place(
        name="Fundación Anselmo Lorenzo",
        address="Calle de las Peñuelas, 41, Arganzuela, 28005 Madrid",
        latlon="40.4008721991779, -3.7021363154852938",
        zone='Legazpi'
    )
    AUDITORIO_FRANCISCA_MARTINEZ_GARRIDO = Place(
        name="Auditorio Francisca Martínez Garrido",
        address="P.º de la Chopera, 6, Arganzuela, 28045 Madrid",
        latlon="40.3948050403511,-3.7003903328011405",
        zone="Legazpi"
    )
    CS_LA_CHELI = Place(
        name="CS La Cheli",
        address="C. de la Iglesia, 12, Carabanchel, 28019 Madrid",
        latlon="40.39584448961841,-3.7177346134909293",
        zone="Marques de Vadillo"
    )
    CSO_DISKORDIA = Place(
        name="CSO Diskordia",
        address="C. de Antoñita Jiménez, 60, Carabanchel, 28019 Madrid",
        latlon="40.39131044903329,-3.7197457145163964",
        zone="Marques de Vadillo"
    )


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
    if re.search(r"Visitas? dialogadas? Matadero", name):
        return "Visita dialogada Matadero"
    place = plain_text((place or "").lower())
    bak = ['']
    while bak[-1] != name:
        bak.append(str(name))
        if "'" not in name:
            name = re.sub(r'["`´”“‘’]', "'", name)
        for k, v in {
            "A.I At War": "A.I. At War",
            "AI At War": "A.I. At War",
            "El sorprendente Dr.Clitterhouse": "El sorprendente Dr. Clitterhouse",
            "El sorprendente Dr.Clitterhousem": "El sorprendente Dr. Clitterhouse",
            "LOS EXILIDOS ROMÁNTICOS": "Los exiliados románticos"
        }.items():
            name = re.sub(r"^\s*"+(r"\s+".join(map(re.escape, re.split(r"\s+", k))))+r"\s*$", v, name, flags=re.I)
        name = re.sub(r"^Taller para adultos:\s*", "", name, flags=re.I)
        name = re.sub(r"^POM Condeduque [\d\-]+\s*", "", name, flags=re.I)
        name = re.sub(r"\s*en el Espacio de Igualdad Lourdes Hernández$", "", name, flags=re.I)
        name = re.sub(r"^Música:\s*", "", name, flags=re.I)
        name = re.sub(r"^Semana de la Ciencia 2025:\s*", "", name, flags=re.I)
        name = re.sub(r"^[a-zA-ZáéÁÉ]+ con Historia[\.\s]+[vV]isitas guiadas tem[aá]ticas a la colecci[oó]n[\.\s]+[a-zA-Z]+", "Visitas guiadas temáticas a la colección", name)
        name = re.sub(r"^Charlas con altura:\s+", "", name)
        name = re.sub(r"[\s\-]+Encuentro con el público$", "", name)
        name = re.sub(r"^[Pp]el[íi]cula[:\.]\s+", "", name)
        name = re.sub(r"Matadero (Madrid )?Centro de Creación Contemporánea", "Matadero", name, flags=re.I)
        name = re.sub(r"\s*\(Ídem\)\s*$", "", name, flags=re.I)
        name = re.sub(r"\.\s*(conferencia)\s*$", "", name, flags=re.I)
        name = re.sub(r"Visita a la exposición '([^']+)'\. .*", r"\1", name, flags=re.I)
        name = re.sub(r"^(lectura dramatizada|presentación del libro|Cinefórum[^:]*|^Madrid, plató de cine)\s*[\.:]\s+", "", name, flags=re.I)
        name = re.sub(r"^conferencia\s+y\s+audiovisual:\s+", "", name, flags=re.I)
        name = re.sub(r"^(conferencia|visita[^'\"]*)[\s:]+(['\"])", r"\2", name, flags=re.I)
        name = re.sub(r"^(conferencia|concierto|espect[aá]culo|proyección( película)?)\s*[\-:\.]\s*", "", name, flags=re.I)
        name = re.sub(r"^(conferencia)\s*", "", name, flags=re.I)
        name = re.sub(r"^visita (comentada|guiada)(:| -)\s+", "", name, flags=re.I)
        name = re.sub(r"^Proyección del documental:\s+", "", name, flags=re.I)
        name = re.sub(r"^(Cine .*)?Proyección de (['\"])", r"\2", name, flags=re.I)
        name = re.sub(r"^Cineclub con .* '([^']+)'.*", r"\1", name, flags=re.I)
        name = re.sub(r"\s*-\s*(moncloa|arganzuela|retiro|chamberi)\s*$", "", name, flags=re.I)
        name = re.sub(r"^(Exposición|Danza|Música):? ([\"'`])(.+)\2$", r"\3", name, flags=re.I)
        name = re.sub(r"Red de Escuelas Municipales del Ayuntamiento de Madrid", "red de Escuelas", name, flags=re.I)
        name = re.sub(r".*Ciclo de conferencias de la Sociedad Española de Retórica': (['\"])", r"\1", name, flags=re.I)
        name = re.sub(r"\s*-\s*$", "", name)
        name = re.sub(r"Asociación (de )?Jubilados( (del )?Ayuntamiento( de Madrid)?)?", "asociación de jubilados", name, flags=re.I)
        name = re.sub(r"^Proyección de la película '([^']+)'", r"\1", name, flags=re.I)
        name = re.sub(r"^(Obra de teatro|Noches? de Clásicos?|21 Distritos)\s*[:\-]\s*", r"", name, flags=re.I)
        name = re.sub(r"Piano City (Madrid *'?\d+|Madrid|'?\d+)", r"Piano City", name, flags=re.I)
        name = re.sub(r"CinePlaza:.*?> (Proyección|Cine)[^:]*:\s+", "", name, flags=re.I)
        name = re.sub(r"^Teatro:?\s+'([^']+)'$", r"\1", name, flags=re.I)
        name = re.sub(r"^Representaci[óo]n teatral:?\s+'([^']+)'$", r"\1", name, flags=re.I)
        name = re.sub(r"^Obra de teatro\.\s+", "", name, flags=re.I)
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
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Actividades_Adultos/Cine_ActividadesAudiovisuales/ficheros/Cine_260x260.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/DistritoRetiro/FICHEROS/FICHEROS%20ACTIVIDADES%20JUNIO/CineVeranoRetiro25-001.jpg',
    'https://entradasfilmoteca.gob.es//Contenido/ImagenesEspectaculos/00_5077/Jazz%20On%20A%20Summer',
    'https://www.madrid.es/UnidadesDescentralizadas/MuseosMunicipales/DepartamentoExposiciones/Actividades/Ciclo%20Cine%20Una%20tarde%20con%20%20Marilyn/Cartel%20Marilyn%20jpg.jpg',
    'https://www.madrid.es/UnidadesDescentralizadas/DistritoRetiro/FICHEROS/FICHEROS%20ACTIVIDADES%20ENERO/18%20enero%20%20CONCIERTO%20Ra%C3%ADzes-001.jpg',
    'https://cdn.tenemosplan.com/tenemosplan/default_image.jpg',
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
    also_in: Tuple[str] = tuple()
    sessions: Tuple[Session] = tuple()
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
        new_name = _clean_name(self.name, self.place.name)
        if new_name != self.name:
            logger.debug(f"[{self.id}].__post_init__ name={new_name} <- {self.name}")
            object.__setattr__(self, 'name', new_name)
        fix_event = FIX_EVENT.get(self.id, {})
        for f in fields(self):
            old_val = getattr(self, f.name, None)
            v = fix_event.get(f.name) or old_val
            if f.name == "sessions":
                v = Session.parse_list(v)
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
            src = self._get_img_from_url(url)
            if src not in ko:
                return src

    def _fix_category(self):
        if self.category == Category.CHILDISH:
            return self.category
        if self.category == Category.CONFERENCE and get_domain(self.more) == "goodreads.com":
            return Category.LITERATURE
        if get_domain(self.url) == "madrid.es":
            soup = WEB.get_cached_soup(self.url)
            for txt in map(plain_text, soup.select("div.tramites-content div.tiny-text")):
                if re_or(
                    txt,
                    "actividad dirigida a familias",
                    "para que menores y mayores aprendan",
                    "teatro infantil",
                    "concierto familiar",
                    "relatos en familia",
                    r"musical? infantil",
                    r"actividad (diseñada )?para familias",
                    flags=re.I
                ):
                    return Category.CHILDISH
                #if re_or(
                #    txt,
                #    "Presentación de la novela",
                #    "presentación del libro",
                #    "la autora firmar[áa]",
                #    "el autor firmar[áa]",
                #    "Publica la editorial Edelvives",
                #    ("encuentro literario", "el autor conversar[áa] sobre su novela")
                #    flags=re.I
                #):
                #    return Category.LITERATURE
        return self.category

    def _get_img_from_url(self, url: str):
        if url is None:
            return None
        dom = get_domain(url)
        if dom == "madrid.es":
            soup = WEB.get_cached_soup(url)
            nodes = soup.select("div.image-content img, div.tramites-content div.tiny-text img, div.detalle img")
            for src in map(get_img_src, nodes):
                if src:
                    return src

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

    def _fix_more(self):
        if self.more:
            return self.more
        urls = self.__get_urls()
        if get_domain(self.url) == "madrid.es":
            if self.category in (Category.CONFERENCE, Category.LITERATURE):
                books = GR.find(self.name)
                if books:
                    return books[0].url
            href = find_more_url_madrides(self.url)
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
    def fusionIfSimilar(all_events: tuple["Event", ...], keys: tuple[str, ...],firstEventUrl: bool = False) -> tuple["Event", ...]
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
            mrg_events.add(Event.fusion(*ok, firstEventUrl=True))
        return tuple(sorted(mrg_events))

    @staticmethod
    def fusion(*evs: "Event", firstEventUrl: bool = False):
        if len(evs) == 0:
            raise ValueError("len(events)==0")
        if len(evs) == 1:
            return evs[0]
        url_title: dict[str, str] = dict()
        for e in evs:
            if e.title and e.url and e.url not in url_title:
                url_title[e.url] = e.title
            for s in e.sessions:
                if s.title and s.url and s.url not in url_title:
                    url_title[s.url] = s.title
        logger.debug("Fusión: " + " + ".join(map(lambda e: e.id, evs)))
        logger.debug("Fusión: " + " + ".join(map(str, evs)))
        dates_with_url: Set[str] = set()
        full_session: Set[str] = set()
        for e in evs:
            for s in e.sessions:
                if s.url is not None:
                    dates_with_url.add(s.date)
                if s.full is True:
                    full_session.add(s.date)
        events = list(evs)
        for i, e in enumerate(events):
            sessions = tuple((s for s in e.sessions if s.url or s.date not in dates_with_url))
            events[i] = e.merge(sessions=sessions)

        sessions: Set[Session] = set()
        sessions_with_url: Set[Session] = set()
        categories: List[Category] = []
        durations: List[float] = []
        imgs: List[str] = []
        set_seen_in: Set[str] = set()
        more_url: List[str] = list()
        for e in events:
            if e.category not in (None, Category.UNKNOWN):
                categories.append(e.category)
            more_url.append(e.more)
            durations.append(e.duration)
            imgs.append(e.img)
            for s in e.sessions:
                s = s._replace(title=None)
                sessions.add(s)
                if firstEventUrl:
                    s = s._replace(url=e.url or s.url)
                else:
                    s = s._replace(url=s.url or e.url)
                sessions_with_url.add(s)
            set_seen_in.add(e.url)
            for u in e.also_in:
                set_seen_in.add(u)
        for st in (categories, set_seen_in, imgs, durations, more_url):
            if None in st:
                st.remove(None)
        seen_in = tuple(sorted(set_seen_in))
        url = seen_in[0]
        also_in = seen_in[1:]
        sessions_url = set(s.url for s in sessions_with_url if s.url is not None)
        if len(sessions) > 1:
            sessions = sessions_with_url
            also_in = tuple((u for u in also_in if u not in sessions_url))
            url = also_in[0] if also_in else None
            also_in = also_in[1:]
        e = events[0].merge(
            url=url,
            also_in=also_in,
            duration=get_main_value(durations),
            img=get_main_value(imgs),
            category=get_main_value(categories, default=Category.UNKNOWN),
            sessions=tuple(sorted(sessions, key=lambda s: (s.date, s.url))),
            price=max(x.price for x in events)
        )
        e = e.fix()
        if e.category != Category.CINEMA and e.more is None and len(e.also_in) == 1:
            e = e.merge(
                more=e.also_in[0],
                also_in=tuple()
            )
        sessions = list(e.sessions)
        for i, s in enumerate(sessions):
            sessions[i] = s._replace(
                title=url_title.get(s.url) or s.title,
                full=True if s.date in full_session else None
            )
        e = e.merge(sessions=sessions)
        if e.more is None:
            not_in = set(e.iter_urls()).union(e.also_in)
            more_url = [u for u in more_url if u not in not_in]
            if more_url:
                e = e.merge(more=more_url[0])
        logger.debug(f"=== {e}")
        return e

    def _fix_cycle(self):
        if self.cycle:
            return self.cycle
        name = self.name or ''
        if re.search(r"^Derechos [dD]igitales: ", name):
            return "Derechos digitales"
        if re.search(r"^Nuevos [Ii]maginarios: ", name):
            return "Nuevos imaginarios"
        if re.search(r"^Los artesanos de la tumba", name):
            return "Los artesanos de la tumba"
        if self.category == Category.THEATER and self.place.name == "Sala Berlanga":
            return "Teatro en la Berlanga"
        #if re.search(r"\s*\-\s*Teatro en la [Bb]erlanga$", name):
        #    return "Teatro en la Berlanga"
        m = re.match(r"^(Interautor 20\d+)\b.*", name)
        if m:
            #if self.category == Category.THEATER and self.place.name == "Sala Berlanga":
            #    return "Teatro en la Berlanga"
            return m.group(1)
        if self.category == Category.VISIT:
            if self.more == "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Itinerarios-guiados-por-El-Retiro/?vgnextfmt=default&vgnextoid=e7b01130a93b1810VgnVCM1000001d4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD":
                return "Itinerarios guiados por El Retiro"
        if self.category == Category.CONFERENCE:
            if self.more == "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/-Los-Clasicos-en-el-Museo-V-Ciclo-de-Conferencias-/?vgnextfmt=default&vgnextoid=3a7136c30d489910VgnVCM100000891ecb1aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD":
                return "Los Clásicos en el Museo"
            if self.more == "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/-Codigo-eterno-codigo-secreto-Las-lenguas-clasicas-y-sus-misterios-XXXIII-Ciclo-de-Conferencias-de-Otono-/?vgnextfmt=default&vgnextoid=abf8a70a5ac39910VgnVCM100000891ecb1aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD":
                return "Las lenguas clásicas y sus misterios"
        if self.category == Category.CINEMA and self.place.name == "Cineteca":
            if re.search(r"^(Esc[áa]ner|Mrgente|Sesi[oó]n) \d+$", name, flags=re.I) or re.search("Stop Motion exquisito|Alzo mi voz.*realidades animadas", name, flags=re.I):
                return "Cortometrajes"
        if self.category == Category.CINEMA:
            if re.search(r"SGAE en corto", name, flags=re.I):
                return "Cortometrajes"
        if re.search(r"cat[áa]logo.*Madrid entre libros", self.name, flags=re.I):
            return "Madrid entre libros"
        if self.category == Category.CONFERENCE and self.img in ("https://www.madrid.es/UnidadWeb/UGBBDD/Actividades/Distritos/Arganzuela/Eventos/ficheros/Roma.png", ):
            return "Tardes romanas"
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
                logger.debug(f"[{self.id}].__fix_name_director: director={d} name={new_name} <- {self.name}")
                object.__setattr__(self, "director", (d, ))
                object.__setattr__(self, "name", new_name)
                return

    def _fix_year(self):
        if self.year is not None:
            return self.year
        yrs: set[int] = set()
        for url in self.iter_urls():
            dom = get_domain(url)
            if dom == "madrid.es":
                soup = WEB.get_cached_soup(url)
                desc = get_text(soup.select_one("div.tramites-content div.tiny-text"))
                for y in map(int, re.findall(r"Año:?\s*((?:19|20)\d+)", desc or "")):
                    if y >= 1900 and y <= (TODAY.year+1):
                        yrs.add(y)
        if len(yrs) == 1:
            return yrs.pop()
        if len(yrs) > 1:
            return None
        for url in self.iter_urls():
            dom = get_domain(url)
            if dom == "madrid.es":
                soup = WEB.get_cached_soup(url)
                desc = get_text(soup.select_one("div.tramites-content div.tiny-text"))
                for y in map(int, re.findall(r"\([^\(\)\d]*((?:19|20)\d+)\)", desc or "")):
                    if y >= 1900 and y <= (TODAY.year+1):
                        yrs.add(y)
        if len(yrs) == 1:
            return yrs.pop()
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

import re
from typing import List, Dict, Union, Set, Tuple, Optional, Callable, TypeVar, Iterable
from bs4 import Tag, BeautifulSoup
import logging
from unidecode import unidecode
import pytz
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2
from collections import Counter, defaultdict
from os import environ
from url_normalize import url_normalize
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse, ParseResult
from functools import cache
import requests
from datetime import date
import holidays


import uuid

UUID_NAMESPACE = uuid.UUID('00000000-0000-0000-0000-000000000000')

T = TypeVar('T')
KeyTuple = TypeVar("KeyTuple", bound=tuple)
ValObject = TypeVar("ValObject", bound=object)

logger = logging.getLogger(__name__)

MONTH = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')

re_sp = re.compile(r"\s+")

tag_concat = ('u', 'ul', 'ol', 'i', 'em', 'strong')
tag_round = ('u', 'i', 'em', 'span', 'strong', 'a')
tag_trim = ('li', 'th', 'td', 'div', 'caption', 'h[1-6]')
tag_right = ('p',)
heads = ("h1", "h2", "h3", "h4", "h5", "h6")
block = heads + ("p", "div", "table", "article")
inline = ("span", "strong", "i", "em", "u", "b", "del")


def get_domain(url: str):
    if url is None or len(url) == 0:
        return None
    parsed_url: ParseResult = urlparse(url)
    domain: str = parsed_url.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def get_img_src(n: Tag):
    if n is None:
        return None
    src = n.attrs.get('src')
    if not isinstance(src, str):
        return None
    src = src.rstrip(" /").strip()
    if len(src) == 0:
        return None
    sch = src.split("://")[0].lower()
    if sch not in ("https", "http"):
        return None
    name = src.rsplit("/", 1)[-1]
    if "." not in name:
        return None
    return src


def get_a_href(n: Tag):
    if n is None:
        return None
    href = n.attrs.get('href')
    if not isinstance(href, str):
        return None
    href = href.strip()
    if len(href) == 0:
        return None
    sch = href.split("://")[0].lower()
    if sch not in ("https", "http"):
        return None
    return href


def get_obj(*args, **kwargs) -> dict:
    if len(args) != 0 and len(kwargs) != 0:
        raise ValueError()
    if len(args) > 1:
        raise ValueError()
    if len(args) == 0:
        return kwargs
    obj = args[0]
    if obj is not None and not isinstance(obj, (dict, list)):
        raise ValueError()
    return obj


def trim(s: str):
    if s is None:
        return None
    s = s.strip()
    if len(s) == 0:
        return None
    return s


def get_text(n: Tag):
    if n is None:
        return None
    txt = n.get_text()
    txt = re_sp.sub(r" ", txt)
    txt = txt.strip()
    if len(txt) == 0:
        return None
    return txt



def dict_add(obj: Dict[str, Set], a: str, b: Union[str, int, List[str], Set[str], Tuple[str]]):
    if a not in obj:
        obj[a] = set()
    if isinstance(b, (str, int)):
        obj[a].add(b)
    else:
        obj[a] = obj[a].union(b)


def plain_text(s: Union[str, Tag], is_html=False):
    if isinstance(s, str) and is_html:
        s = BeautifulSoup(s, "html.parser")
    if isinstance(s, Tag):
        for n in s.findAll(["p", "br"]):
            n.insert_after(" ")
        s = get_text(s)
    if s is None:
        return None
    faken = "&%%%#%%%#%%#%%%%%%&"
    s = re.sub(r"[,\.:\(\)\[\]¡!¿\?\"']", " ", s).lower()
    s = s.replace("ñ", faken)
    s = unidecode(s)
    s = s.replace(faken, "ñ")
    s = re_sp.sub(" ", s).strip()
    if len(s) == 0:
        return None
    return s


def re_or(s: str, *args: Union[str, Tuple[str]], to_log: str = None, flags=0):
    if s is None or len(s) == 0 or len(args) == 0:
        return None
    for r in args:
        if isinstance(r, tuple):
            b = re_and(s, *r, flags=flags)
            if b is not None:
                if to_log:
                    logger.debug(f"{to_log} cumple {b}")
                return b
        else:
            reg = str(r)
            if reg[0] not in ("^", " "):
                reg = r"\b" + reg
            if reg[-1] not in ("$", " ", ":"):
                reg = reg + r"\b"
            if re.search(reg, s, flags=flags):
                if to_log:
                    logger.debug(f"{to_log} cumple {r}")
                return r
    return None


def re_and(s: str, *args: Union[str, Tuple[str]], to_log: str = None, flags=0):
    if s is None or len(s) == 0 or len(args) == 0:
        return None
    arr = []
    for r in args:
        if isinstance(r, tuple):
            b = re_or(s, *r, flags=flags)
            if b is None:
                return None
            arr.append(b)
        elif re.search(r"\b" + r + r"\b", s, flags=flags):
            arr.append(r)
        else:
            return None
    txt = " AND ".join(arr)
    if to_log:
        logger.debug(f"{to_log} cumple {txt}")
    return txt


def to_datetime(s: str):
    if s is None:
        return None
    tz = pytz.timezone('Europe/Madrid')
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
    return tz.localize(dt)


def getKm(lat1: float, lon1: float, lat2: float, lon2: float):
    # Radio de la Tierra en kilómetros
    R = 6371.0

    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Fórmula de Haversine
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Distancia
    distance = R * c
    return abs(distance)


def my_filter(iterable: Iterable[T], func: Callable[[T], bool]) -> Tuple[List[T], List[T]]:
    ok: List[T] = []
    ko: List[T] = []
    for i in iterable:
        if func(i) is True:
            ok.append(i)
        else:
            ko.append(i)
    return ok, ko


def get_main_value(arr: List[T], default: Optional[T] = None) -> Union[T, None]:
    if arr is None:
        return default
    if len(arr) == 0:
        return default
    contador = Counter(arr)
    max_rep = max(contador.values())
    for e in arr:
        if contador[e] == max_rep:
            return e


def to_uuid(s: str):
    try:
        _ = uuid.UUID(s)
        return s.upper()
    except ValueError:
        return str(uuid.uuid5(UUID_NAMESPACE, s)).upper()


def uniq(*args: Union[str, None]):
    arr: List[str] = []
    for a in args:
        if a not in (None, '') and a not in arr:
            arr.append(a)
    return arr


def iter_chunk(size: int, args: list):
    arr = []
    for a in args:
        arr.append(a)
        if len(arr) == size:
            yield arr
            arr = []
    if arr:
        yield arr


def get_env(*args: str, default: str = None) -> str | None:
    for a in args:
        v = environ.get(a)
        if isinstance(v, str):
            v = v.strip()
            if len(v):
                return v
    return default


def find_duplicates(
    evs: Iterable[ValObject],
    mk_key: Callable[[ValObject], Optional[KeyTuple]],
) -> Tuple[Tuple[ValObject, ...], ...]:
    data_set: Dict[KeyTuple, List[ValObject]] = defaultdict(list)
    for e in evs:
        k = mk_key(e)
        if k is not None and e not in data_set[k]:
            data_set[k].append(e)
    data: list[Tuple[ValObject, ...]] = []
    for k, v in data_set.items():
        if len(v) > 1:
            data.append(tuple(v))
    return tuple(data)


def normalize_url(url: str, *tail: str) -> str:
    norm_url = url_normalize(url)
    parsed = urlparse(norm_url)
    query_params = parse_qsl(parsed.query, keep_blank_values=True)

    new_params = [p for p in query_params if p[0] not in tail]
    for param in tail:
        if param in dict(query_params):
            new_params.append((param, dict(query_params)[param]))

    new_query = urlencode(new_params)
    new_unparse = urlunparse(parsed._replace(query=new_query))
    return new_unparse


def find_euros(*prices: str | None) -> None | float | int:
    for prc in prices:
        if prc is None:
            continue
        if re.match(r"^\s*(gratuito|gratis)\s*$", prc, flags=re.I):
            return 0
        if re.search(
            r"\b(gratuit[ao] (para|con)|(entrada|acceso) (gratuit[oa]|libre)|actividad(es)? gratuitas?)\b",
            prc,
            flags=re.I
        ):
            return 0
        if re.search(
            r"Taller(es)? gratuitos?\b",
            prc,
            flags=re.I
        ):
            return 0
        eur: set[float] = set()
        for s in re.findall(r"(\d[\d\.,]*)\s*(?:€|euros?)", prc, flags=re.I):
            p = float(s.replace(",", "."))
            if p == int(p):
                p = int(p)
            eur.add(p)
        if len(eur):
            return max(eur)


@cache
def get_festivos(year: int):
    dates = _get_festivos_from_calendarioslaborales(year)
    if len(dates) > 0:
        return tuple(sorted(dates))
    hol = holidays.country_holidays(
        country="ES",
        subdiv="MD",
        years=year
    )
    return tuple(sorted(hol.keys()))


def _safe_soup(url: str):
    try:
        r = requests.get(url)
        r.raise_for_status()
        return BeautifulSoup(r.content, 'html.parser')
    except Exception as e:
        logger.critical(f"{url} {e}")


def _get_festivos_from_calendarioslaborales(year: int):
    dates: set[date] = set()
    soup = _safe_soup(f"https://www.calendarioslaborales.com/calendario-laboral-madrid-{year}.htm")
    if soup is None:
        return dates
    for month, div in enumerate(soup.select("div.calendar-row div.month")):
        for td in div.select("td"):
            cls = td.attrs.get('class')
            if isinstance(cls, str):
                cls = cls.strip().split()
            if cls is None or len(cls) == 0:
                continue
            if not isinstance(cls, list):
                raise ValueError(f"class={cls} in {td}")
            for i in cls:
                if re.search(r"holiday", i, flags=re.I):
                    dt = date(year, month+1, int(get_text(td)))
                    dates.add(dt)
    return dates


def isWorkingHours(dt: datetime, min_hour=16):
    if dt is None:
        return False
    hm = dt.hour + (dt.minute/60)
    if hm == 0 or hm >= min_hour:
        return False
    if dt.weekday() in (5, 6):
        return False
    if dt.date() in get_festivos(dt.year):
        return False
    return True


def un_camel(x: str):
    if x is None or " " in x:
        return x
    if x in ("LGTBI",):
        return x
    return re.sub(
        r"(?<!^)(?=[A-ZÁÉÍÓÚÜÑ])",
        " ",
        x
    )


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
    'https://www.madrid.es/UnidadesDescentralizadas/Bibliotecas/BibliotecasPublicas/Actividades/Cine_Audiovisuales/ficheros/esqueria_260x260.png',
    'https://cdn.tenemosplan.com/tenemosplan/default_image.jpg',
    'https://www.madrid.es/UnidadWeb/UGBBDD/EntidadesYOrganismos/CulturaYOcio/InstalacionesCulturales/CentrosCulturalesMunicipales/CCVillaverde/Ficheros/CSCBohemios.jpg',
)

KO_MORE = (
    'https://www.semanacienciamadrid.org/',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Dotacional-Integrado-Arganzuela-Angel-del-Rio/?vgnextfmt=default&vgnextoid=0758c4a248991910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Sociocultural-Oporto/?vgnextfmt=default&vgnextoid=e990f36edd371910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Casa-del-Reloj/?vgnextfmt=default&vgnextoid=b8ce2420dc891910VgnVCM1000001d4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Fernando-Lazaro-Carreter/?vgnextfmt=default&vgnextoid=25bff36edd371910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Programacion-Cultural-Chamberi/?vgnextfmt=default&vgnextoid=5ea6daba65bc4910VgnVCM1000001d4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Espacio-Sociocultural-Marta-Escudero-Diaz-Tejeiro/?vgnextfmt=default&vgnextoid=d7e6b95f6495a910VgnVCM200000f921e388RCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Festividad-de-San-Anton-2026/?vgnextfmt=default&vgnextoid=4ad7b5e5b979b910VgnVCM100000891ecb1aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/44-Semana-de-cine-espanol-de-Carabanchel/?vgnextfmt=default&vgnextoid=626e8c7843cab910VgnVCM200000f921e388RCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Sociocultural-Bohemios/?vgnextfmt=default&vgnextoid=51dcd15c0bfe4910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Aula-Ambiental-La-Cabana-del-Retiro/?vgnextfmt=default&vgnextoid=2f682316c35ba910VgnVCM100000891ecb1aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Casa-de-Vacas/?vgnextfmt=default&vgnextoid=eba1c5a51e2c9910VgnVCM100000891ecb1aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Lavapies/?vgnextfmt=default&vgnextoid=06dec9f1b5902910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Clara-del-Rey-Museo-ABC/?vgnextfmt=default&vgnextoid=2eb1b01c7d402910VgnVCM1000001d4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Programacion-cultural-de-febrero-y-marzo/?vgnextfmt=default&vgnextoid=2275dc8de040c910VgnVCM200000f921e388RCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Carnaval-en-Arganzuela/?vgnextfmt=default&vgnextoid=669c11abd940c910VgnVCM100000891ecb1aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/FormularioElectronico/Inicio/Buscador/Visita-Guiada/?vgnextfmt=default&vgnextoid=bd5fc2c8ca51a910VgnVCM100000891ecb1aRCRD&vgnextchannel=7db8fc12aa936610VgnVCM1000008a4a900aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Puerta-de-Toledo/?vgnextfmt=default&vgnextoid=8d42fd00f7902910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Sociocultural-Santa-Petronila/?vgnextfmt=default&vgnextoid=fe7ed15c0bfe4910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Talleres-presenciales-ciudades-sostenibles-/?vgnextfmt=default&vgnextoid=d2a0924d36704910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'imccwem.munimadrid.es'
)


def capitalize(name: str):
    if name == name.upper():
        name = name.capitalize()
    for x in (
        "María la Rica",
        "Cervantes",
        "Alcalá",
        "Henares",
        "Antezana",
        "Santiago",
        "Complutense",
        "Mononoke",
        "IV",
        "BSMM",
        "Paco de Lucía",
        "AWWZ",
        "CSO",
        "EKO",
        "IA",
        "AI",
        "centro cultural",
        "XXX",
        "VHZ",
        "XXV",
        "Quijote",
    ):
        name = re.sub(r"\b"+re.escape(x)+r"\b", x, name, flags=re.I)
    w1 = name[0]
    if w1.isalpha():
        name = w1.upper()+name[1:]
    return name
